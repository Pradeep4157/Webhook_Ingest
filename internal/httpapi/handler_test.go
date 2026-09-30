package httpapi_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/convin/webhook-ingest/internal/testutil"
)

func TestConcurrentDuplicateWebhook(t *testing.T) {
	srv, store := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, store)

	// Single payload duplicated across all concurrent workers
	payload := []byte(fmt.Sprintf(`{
		"event_id": "%s",
		"call_id": "%s",
		"account_id": "%s",
		"status": "completed",
		"duration_sec": 120,
		"recording_url": "https://example.com/rec.mp3"
	}`, eventID, callID, accountID))

	const n = 20
	var wg sync.WaitGroup
	wg.Add(n)

	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()

			resp, err := http.Post(srv.URL+"/webhooks/calls", "application/json", bytes.NewReader(payload))
			if err != nil {
				t.Errorf("HTTP POST failed: %v", err)
				return
			}
			resp.Body.Close()

			// Expecting 200 OK (or 202) regardless of duplicate status
			if resp.StatusCode >= 400 {
				t.Errorf("expected successful status code, got %d", resp.StatusCode)
			}
		}()
	}

	wg.Wait()

	ctx := context.Background()

	// 1. Assert exactly 1 event record was inserted in Postgres
	var eventCount int
	err := store.Pool().QueryRow(ctx, `SELECT COUNT(*) FROM events WHERE event_id = $1`, eventID).Scan(&eventCount)
	if err != nil {
		t.Fatalf("failed to query events table: %v", err)
	}
	if eventCount != 1 {
		t.Errorf("expected 1 event record, got %d", eventCount)
	}

	// 2. Assert account stats were incremented exactly once
	stats, err := store.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("failed to query account stats: %v", err)
	}
	if stats.CallCount != 1 {
		t.Errorf("expected CallCount = 1, got %d", stats.CallCount)
	}
	if stats.TotalDurationSec != 120 {
		t.Errorf("expected TotalDurationSec = 120, got %d", stats.TotalDurationSec)
	}
}

func post(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestHealthz(t *testing.T) {
	srv, _ := testutil.NewServer(t)

	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}
}

func TestWebhookRejectsMalformedJSON(t *testing.T) {
	srv, _ := testutil.NewServer(t)

	resp := post(t, srv.URL+"/webhooks/calls", `{not json`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("got %d, want 400", resp.StatusCode)
	}
}

func TestWebhookRejectsMissingEventID(t *testing.T) {
	srv, st := testutil.NewServer(t)
	_, callID, accountID := testutil.IDs(t, st)

	body := fmt.Sprintf(
		`{"call_id":%q,"account_id":%q,"status":"completed","duration_sec":10}`,
		callID, accountID)
	resp := post(t, srv.URL+"/webhooks/calls", body)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("got %d, want 400", resp.StatusCode)
	}
}

func TestWebhookRejectsUnknownStatus(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)

	body := fmt.Sprintf(
		`{"event_id":%q,"call_id":%q,"account_id":%q,"status":"exploded","duration_sec":10}`,
		eventID, callID, accountID)
	resp := post(t, srv.URL+"/webhooks/calls", body)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("got %d, want 400", resp.StatusCode)
	}
}

func TestAccountStatsEndpointRespondsJSON(t *testing.T) {
	srv, st := testutil.NewServer(t)
	_, _, accountID := testutil.IDs(t, st)

	resp, err := http.Get(srv.URL + "/accounts/" + accountID + "/stats")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("Content-Type is %q, want application/json", ct)
	}
}