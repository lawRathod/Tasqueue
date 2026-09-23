package tasqueue

import (
	"context"
	"testing"
	"time"
)

// TestChainWithoutSavedResult is the minimal reproducer for a chain whose
// handlers do not persist a result.
//
// Meta.PrevJobResult is documented as "nil if the previous job doesn't set the
// results on JobCtx" (jobs.go, and the README's Meta section), so a handler that
// never calls JobCtx.Save is a supported chain step. What actually happens:
// execJob reads the previous result with GetResult and returns on the
// ErrNotFound, before it enqueues the next job AND before it calls statusDone.
// So the step that succeeded is never recorded terminal and the chain stops.
//
// This test is written against the documented behaviour, so it fails on the
// current code and passes once the missing result is treated as nil.
func TestChainWithoutSavedResult(t *testing.T) {
	ctx := context.Background()
	srv := newServer(t, "noop", func(_ []byte, _ JobCtx) error { return nil })

	go srv.Start(ctx)

	first, err := NewJob("noop", []byte("one"), JobOpts{MaxRetries: 0})
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewJob("noop", []byte("two"), JobOpts{MaxRetries: 0})
	if err != nil {
		t.Fatal(err)
	}
	chain, err := NewChain([]Job{first, second}, ChainOpts{ID: "chain-probe"})
	if err != nil {
		t.Fatal(err)
	}
	chainID, err := srv.EnqueueChain(ctx, chain)
	if err != nil {
		t.Fatal(err)
	}

	cm, err := srv.GetChain(ctx, chainID)
	if err != nil {
		t.Fatal(err)
	}
	rootID := cm.JobID

	// Bounded wait for the (real-time) server to process the root.
	deadline := time.Now().Add(2 * time.Second)
	var root JobMessage
	for time.Now().Before(deadline) {
		jm, err := srv.GetJob(ctx, rootID)
		if err == nil {
			root = jm
			if jm.Status == StatusDone || jm.Status == StatusFailed {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	// The root's handler returned nil, so the step succeeded. It must be
	// terminal, and its successor must have been enqueued.
	if root.Status != StatusDone {
		t.Errorf("root job status = %q, want %q: a successful step was never recorded terminal",
			root.Status, StatusDone)
	}
	if len(root.OnSuccessIDs) == 0 {
		t.Errorf("root job enqueued no successor: the chain stopped at step 1")
	}

	success, _ := srv.GetSuccess(ctx)
	if len(success) == 0 {
		t.Errorf("success partition is empty: the successful step was not recorded")
	}

	final, err := srv.GetChain(ctx, chainID)
	if err != nil {
		t.Fatal(err)
	}
	if final.Status != StatusDone {
		t.Errorf("chain status = %q, want %q", final.Status, StatusDone)
	}
}
