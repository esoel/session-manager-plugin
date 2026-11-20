// Modified by the ssmc project: tests for embedded session hooks.
package portsession

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/esoel/session-manager-plugin/src/log"
	"github.com/esoel/session-manager-plugin/src/sessionmanagerplugin/session"
)

type embeddingListener struct{ closed bool }

func (l *embeddingListener) Accept() (net.Conn, error) { return nil, net.ErrClosed }
func (l *embeddingListener) Close() error              { l.closed = true; return nil }
func (l *embeddingListener) Addr() net.Addr            { return &net.TCPAddr{Port: 2222} }

func TestEmbeddedListenerHook(t *testing.T) {
	previous := Listen
	defer func() { Listen = previous }()
	l := &embeddingListener{}
	Listen = func(id, network, address string) (net.Listener, error) {
		if id != "s-1" || network != "tcp" || address != "localhost:2222" {
			t.Fatalf("listen %s %s", network, address)
		}
		return l, nil
	}
	p := &MuxPortForwarding{sessionId: "s-1", portParameters: PortParameters{LocalPortNumber: "2222"}}
	err := p.handleClientConnections(log.NewMockLog(), context.Background())
	if !errors.Is(err, net.ErrClosed) || !l.closed {
		t.Fatalf("error=%v closed=%v", err, l.closed)
	}
}

func TestEmbeddedStopReturnsOnce(t *testing.T) {
	previous := session.StopHook
	defer func() { session.StopHook = previous }()
	calls := 0
	session.StopHook = func(id string) {
		calls++
		if id != "s-1" {
			t.Fatal(id)
		}
	}
	p := &MuxPortForwarding{sessionId: "s-1"}
	p.Stop()
	p.Stop()
	if calls != 1 {
		t.Fatalf("stop hook calls=%d", calls)
	}
}
