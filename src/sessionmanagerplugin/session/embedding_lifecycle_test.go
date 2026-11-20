// Modified by the ssmc project: regression tests for embedded session lifetime.
package session

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	wsMocks "github.com/esoel/session-manager-plugin/src/communicator/mocks"
	"github.com/esoel/session-manager-plugin/src/datachannel"
	dcMocks "github.com/esoel/session-manager-plugin/src/datachannel/mocks"
	"github.com/esoel/session-manager-plugin/src/sdkutil"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/mock"
)

func TestEmbeddedResumeFailureStopsSession(t *testing.T) {
	file := filepath.Join(t.TempDir(), "empty")
	if err := os.WriteFile(file, nil, 0600); err != nil {
		t.Fatal(err)
	}
	for key, val := range map[string]string{"AWS_ACCESS_KEY_ID": "fake", "AWS_SECRET_ACCESS_KEY": "fake", "AWS_SESSION_TOKEN": "", "AWS_PROFILE": "", "AWS_DEFAULT_PROFILE": "", "AWS_CONFIG_FILE": file, "AWS_SHARED_CREDENTIALS_FILE": file, "AWS_EC2_METADATA_DISABLED": "true"} {
		t.Setenv(key, val)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"__type":"InvalidSessionId","message":"session ended"}`))
	}))
	defer server.Close()
	sdkutil.SetRegionAndProfile("us-east-1", "")
	mockDataChannel, mockWsChannel = &dcMocks.IDataChannel{}, &wsMocks.IWebSocketChannel{}
	SetupMockActions()
	mockDataChannel.On("Open", mock.Anything).Return(nil)
	mockDataChannel.On("GetStreamDataSequenceNumber").Return(int64(0))
	var onError func(error)
	for _, call := range mockWsChannel.ExpectedCalls {
		if call.Method == "SetOnError" {
			call.Run(func(args mock.Arguments) { onError = args.Get(0).(func(error)) })
		}
	}
	s := &Session{SessionId: "test", Endpoint: server.URL, DataChannel: mockDataChannel}
	if err := s.OpenDataChannel(logger); err != nil {
		t.Fatal(err)
	}
	s.retryParams.MaxAttempts = 0
	old := StopHook
	defer func() { StopHook = old }()
	stopped := false
	StopHook = func(id string) { stopped = id == "test" }
	onError(errors.New("network connection lost"))
	if !stopped {
		t.Fatal("terminal ResumeSession error did not stop the owning session")
	}
}

func TestEmbeddedCancellationBeforeOpen(t *testing.T) {
	mockDataChannel, mockWsChannel = &dcMocks.IDataChannel{}, &wsMocks.IWebSocketChannel{}
	SetupMockActions()
	opened := false
	mockDataChannel.On("Open", mock.Anything).Run(func(mock.Arguments) { opened = true }).Return(nil)
	mockDataChannel.On("Close", mock.Anything).Return(nil)
	mockDataChannel.On("IsSessionTypeSet").Return(make(chan bool))
	mockDataChannel.On("IsStreamMessageResendTimeout").Return(make(chan bool))
	oldHook, oldContext := StopHook, SessionContext
	defer func() { StopHook, SessionContext = oldHook, oldContext }()
	StopHook = func(string) {}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	SessionContext = func(string) context.Context { return ctx }
	s := &Session{DataChannel: mockDataChannel}
	err := s.Execute(logger)
	if opened || !errors.Is(err, context.Canceled) {
		t.Fatalf("opened=%v error=%v", opened, err)
	}
}

func TestEmbeddedInitialOpenFailureReturned(t *testing.T) {
	mockDataChannel, mockWsChannel = &dcMocks.IDataChannel{}, &wsMocks.IWebSocketChannel{}
	SetupMockActions()
	mockDataChannel.On("Open", mock.Anything).Return(errors.New("connection refused"))
	mockDataChannel.On("Reconnect", mock.Anything).Return(errors.New("still refused"))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	oldContext := SessionContext
	defer func() { SessionContext = oldContext }()
	SessionContext = func(string) context.Context { return ctx }
	s := &Session{DataChannel: mockDataChannel}
	if err := s.OpenDataChannel(logger); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("opening failure was swallowed: %v", err)
	}
	mockDataChannel.AssertNotCalled(t, "ResendStreamDataMessageScheduler", mock.Anything)
}

func TestEmbeddedCancellationDuringWebsocketHandshake(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	oldHook, oldContext := StopHook, SessionContext
	defer func() { StopHook, SessionContext = oldHook, oldContext }()
	StopHook = func(string) {}
	SessionContext = func(string) context.Context { return ctx }
	s := &Session{DataChannel: &datachannel.DataChannel{}, StreamUrl: "ws" + strings.TrimPrefix(server.URL, "http")}
	done := make(chan error, 1)
	go func() { done <- s.Execute(logger) }()
	// Release the fake server even when the assertion fails, then join Execute.
	defer func() { close(release); <-done }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("dial did not reach the fake endpoint")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Error("cancelled handshake succeeded")
		}
		done <- err
	case <-time.After(time.Second):
		t.Error("cancellation did not interrupt the websocket handshake")
	}
}

func TestEmbeddedRepeatedSessionsJoinWorkers(t *testing.T) {
	connected := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
		connected <- struct{}{}
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	oldHook, oldContext := StopHook, SessionContext
	defer func() { StopHook, SessionContext = oldHook, oldContext }()
	StopHook = func(string) {}
	for n := 0; n < 5; n++ {
		ctx, cancel := context.WithCancel(context.Background())
		SessionContext = func(string) context.Context { return ctx }
		s := &Session{SessionId: "test", DataChannel: &datachannel.DataChannel{}, StreamUrl: "ws" + strings.TrimPrefix(server.URL, "http")}
		done := make(chan error, 1)
		go func() { done <- s.Execute(logger) }()
		select {
		case <-connected:
		case <-time.After(time.Second):
			cancel()
			t.Fatal("session did not open its channel")
		}
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("session workers did not stop")
		}
		select {
		case <-s.resendDone:
		default:
			t.Fatal("Execute returned with a live resend-timeout waiter")
		}
	}
}
