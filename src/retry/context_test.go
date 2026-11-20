// Modified by the ssmc project: cancellable retry tests.
package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCallContextCancelsBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	called := make(chan struct{}, 1)
	r := RepeatableExponentialRetryer{MaxAttempts: 10, InitialDelayInMilli: 60000, MaxDelayInMilli: 60000, GeometricRatio: 1,
		CallableFunc: func() error { called <- struct{}{}; return errors.New("try again") },
	}
	done := make(chan error, 1)
	go func() { done <- r.CallContext(ctx) }()
	<-called
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled retry remained asleep")
	}
	select {
	case <-called:
		t.Fatal("operation retried after cancellation")
	default:
	}
}

func TestCallContextAlreadyCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := RepeatableExponentialRetryer{CallableFunc: func() error { t.Fatal("operation ran after cancellation"); return nil }}
	if err := r.CallContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
