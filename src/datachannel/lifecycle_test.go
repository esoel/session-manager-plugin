// Modified by the ssmc project: regression tests for datachannel worker cleanup.
package datachannel

import (
	"runtime"
	"strings"
	"testing"
	"time"

	wsMocks "github.com/esoel/session-manager-plugin/src/communicator/mocks"
	"github.com/esoel/session-manager-plugin/src/log"
	"github.com/stretchr/testify/mock"
)

func resendWorkers() int {
	buffer := make([]byte, 1<<20)
	n := runtime.Stack(buffer, true)
	return strings.Count(string(buffer[:n]), "ResendStreamDataMessageScheduler.func1(")
}

func TestCloseStopsResendWorkers(t *testing.T) {
	before := resendWorkers()
	logger := log.NewMockLog()
	for i := 0; i < 3; i++ {
		ws := &wsMocks.IWebSocketChannel{}
		ws.On("Close", mock.Anything).Return(nil)
		ws.On("GetStreamUrl").Return("local-test")
		dc := &DataChannel{}
		dc.Initialize(logger, "client", "session", "target", false)
		dc.SetWsChannel(ws)
		if err := dc.ResendStreamDataMessageScheduler(logger); err != nil {
			t.Fatal(err)
		}
		if err := dc.Close(logger); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(time.Second)
	for resendWorkers() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if after := resendWorkers(); after > before {
		t.Fatalf("closed sessions leaked %d resend workers", after-before)
	}
}
