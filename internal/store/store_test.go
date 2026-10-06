package store_test

import (
	"context"
	"testing"
	"github.com/convin/webhook-ingest/internal/store"
	"github.com/convin/webhook-ingest/internal/testutil"
	"sync"
)

func TestIngestEventThenExists(t *testing.T) {
	s := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, s)
	ctx := context.Background()

	evt := store.Event{
		EventID: eventID, CallID: callID, AccountID: accountID,
		Status: "completed", DurationSec: 10, Payload: []byte(`{}`),
	}

	var exists bool
	err := s.Pool().QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM events WHERE event_id = $1)`, eventID).Scan(&exists)
	if err != nil {
		t.Fatalf("check event existence: %v", err)
	}
	if exists {
		t.Fatal("expected event to be absent before insert")
	}

	inserted, err := s.IngestEvent(ctx, evt)
	if err != nil {
		t.Fatalf("IngestEvent: %v", err)
	}
	if !inserted {
		t.Fatal("expected event to be inserted")
	}

	err = s.Pool().QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM events WHERE event_id = $1)`, eventID).Scan(&exists)
	if err != nil {
		t.Fatalf("check event existence: %v", err)
	}
	if !exists {
		t.Fatal("expected event to exist after insert")
	}
}

func TestAccountStatsAccumulatesViaIngest(t *testing.T) {
	s := testutil.NewStore(t)
	eventID1, callID1, accountID := testutil.IDs(t, s)
	eventID2 := eventID1 + "_2"
	callID2 := callID1 + "_2"
	ctx := context.Background()

	evt1 := store.Event{
		EventID: eventID1, CallID: callID1, AccountID: accountID,
		Status: "completed", DurationSec: 30, Payload: []byte(`{}`),
	}
	evt2 := store.Event{
		EventID: eventID2, CallID: callID2, AccountID: accountID,
		Status: "completed", DurationSec: 12, Payload: []byte(`{}`),
	}

	if _, err := s.IngestEvent(ctx, evt1); err != nil {
		t.Fatalf("IngestEvent 1: %v", err)
	}
	if _, err := s.IngestEvent(ctx, evt2); err != nil {
		t.Fatalf("IngestEvent 2: %v", err)
	}

	got, err := s.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if got.CallCount != 2 || got.TotalDurationSec != 42 {
		t.Fatalf("got %+v, want CallCount=2 TotalDurationSec=42", got)
	}
}

func TestIngestEventThenMarkRecordingProcessed(t *testing.T) {
	s := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, s)
	ctx := context.Background()

	evt := store.Event{
		EventID: eventID, CallID: callID, AccountID: accountID,
		Status: "completed", DurationSec: 10,
		RecordingURL: "https://example.com/a.wav", Payload: []byte(`{}`),
	}
	if _, err := s.IngestEvent(ctx, evt); err != nil {
		t.Fatalf("IngestEvent: %v", err)
	}
	if err := s.MarkRecordingProcessed(ctx, callID); err != nil {
		t.Fatalf("MarkRecordingProcessed: %v", err)
	}

	var processed bool
	row := s.Pool().QueryRow(ctx, `SELECT recording_processed FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&processed); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !processed {
		t.Fatal("expected recording_processed to be true")
	}
}

func TestConcurrentPendingRecordingClaims(t *testing.T) {
	srv, st := testutil.NewServer(t)
	defer srv.Close()
	_, callID, accountID := testutil.IDs(t, st)
	evt := store.Event{
		EventID: "event-claim-test",
		CallID: callID,
		AccountID: accountID,
		Status: "completed",
		DurationSec: 10, 
		RecordingURL: "https://example.com/a.wav",
		Payload: []byte(`{}`),
	}
	if _, err := st.IngestEvent(context.Background(), evt); err != nil {
		t.Fatal(err)
	}
	// now i will create go routines to fetch this recording in db
	var wg sync.WaitGroup
	results := make(chan []store.Event, 2)

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			recordings, err := st.PendingRecordings(context.Background())
			if err != nil {
				t.Fatal(err)
				return
			}
			results <- recordings

		}()
	}
	wg.Wait()
	close(results)
	total := 0
	for recordings := range results {
		total += len(recordings)

	}
	if total != 1 { 
		t.Fatalf("recording was claimed %d times, want 1", total)
	}

	
}