package tasqueue

import (
	"context"
	"testing"
	"time"
)

// TestUnknownTaskJobIsNotDiscarded is the minimal reproducer for a consumed job
// whose task has no registered handler.
//
// Every task defaults to the same queue (TaskOpts.Queue -> DefaultQueue), and a
// queue is consumed by every worker on it. So a worker that does not have the
// handler still POPS the message: process() calls getHandler, logs "handler not
// found" and `continue`s — before statusProcessing, before any retry, and
// without writing a terminal row. The message is gone from the broker while the
// job's stored status stays `queued`.
//
// A task queue's client can only learn an outcome through the results backend,
// so the test asserts that an accepted job ends in a terminal state.
func TestUnknownTaskJobIsNotDiscarded(t *testing.T) {
	ctx := context.Background()
	srv := newServer(t, "known", func(_ []byte, _ JobCtx) error { return nil })
	go srv.Start(ctx)

	job, err := NewJob("unregistered", nil, JobOpts{MaxRetries: 3})
	if err != nil {
		t.Fatal(err)
	}
	id, err := srv.Enqueue(ctx, job)
	if err != nil {
		t.Fatal(err)
	}

	// Bounded wait for the job to be consumed.
	time.Sleep(500 * time.Millisecond)

	jm, err := srv.GetJob(ctx, id)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	success, _ := srv.GetSuccess(ctx)
	failed, _ := srv.GetFailed(ctx)
	pending, _ := srv.GetPending(ctx, DefaultQueue)

	terminal := jm.Status == StatusDone || jm.Status == StatusFailed
	if !terminal {
		t.Errorf("job %s is not terminal (status=%q) and the broker holds %d message(s): "+
			"success=%v failed=%v", id, jm.Status, len(pending), success, failed)
	}
}
