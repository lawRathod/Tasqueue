package tasqueue

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	rb "github.com/lawRathod/tasqueue/brokers/in-memory"
	rr "github.com/lawRathod/tasqueue/results/in-memory"
)

// TestCallbacksReceiveLiveContext is the minimal reproducer for the lifecycle
// hooks being handed a cancelled context.
//
// execJob cancels the attempt's context as soon as the attempt resolves — in the
// same select that receives the handler's result — and only then calls
// SuccessCB / RetryingCB / FailedCB with that same JobCtx. ProcessingCB runs
// before the handler, so it is the only hook with a live context. A callback
// that uses its context (the documented JobCtx.Save, an outbound call, a log
// with a request id) therefore fails: on the redis results backend Save passes
// the context to go-redis and returns context.Canceled.
//
// The test asserts the ordinary contract of a callback that receives a
// context.Context: it is usable for the work the callback is being asked to do.
func TestCallbacksReceiveLiveContext(t *testing.T) {
	ctx := context.Background()
	srv, err := NewServer(ServerOpts{Broker: rb.New(), Results: rr.New()})
	if err != nil {
		t.Fatal(err)
	}

	var (
		mu       sync.Mutex
		observed = map[string]error{}
		record   = func(hook string) func(JobCtx) {
			return func(j JobCtx) {
				mu.Lock()
				observed[hook] = j.Err()
				mu.Unlock()
			}
		}
	)
	if err := srv.RegisterTask("ok", func(_ []byte, _ JobCtx) error { return nil }, TaskOpts{
		Concurrency:  1,
		ProcessingCB: record("processing"),
		SuccessCB:    record("success"),
	}); err != nil {
		t.Fatal(err)
	}
	if err := srv.RegisterTask("bad", func(_ []byte, _ JobCtx) error { return errors.New("nope") }, TaskOpts{
		Concurrency: 1,
		RetryingCB: func(j JobCtx, _ error) {
			mu.Lock()
			observed["retrying"] = j.Err()
			mu.Unlock()
		},
		FailedCB: func(j JobCtx, _ error) {
			mu.Lock()
			observed["failed"] = j.Err()
			mu.Unlock()
		},
	}); err != nil {
		t.Fatal(err)
	}

	go srv.Start(ctx)

	ok, _ := NewJob("ok", nil, JobOpts{MaxRetries: 0})
	bad, _ := NewJob("bad", nil, JobOpts{MaxRetries: 1})
	if _, err := srv.Enqueue(ctx, ok); err != nil {
		t.Fatal(err)
	}
	if _, err := srv.Enqueue(ctx, bad); err != nil {
		t.Fatal(err)
	}
	time.Sleep(700 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(observed) != 4 {
		t.Fatalf("expected all four hooks to fire, got %v", observed)
	}
	for hook, err := range observed {
		if err != nil {
			t.Errorf("%s callback received a dead context: %v", hook, err)
		}
	}
}
