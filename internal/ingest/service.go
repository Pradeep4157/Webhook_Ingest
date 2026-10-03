// Package ingest accepts call-completion webhooks and processes them.
package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"
	"github.com/redis/go-redis/v9"
	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
)

// recordingWork stands in for downloading and transcoding a recording.
const recordingWork = 50 * time.Millisecond

// Service ingests webhook deliveries.
type Service struct {
	store *store.Store
	cache *stats.Cache
	rdb   *redis.Client
	log   *slog.Logger
	wg sync.WaitGroup
}

// New builds a Service.
func New(s *store.Store, c *stats.Cache, rdb *redis.Client, log *slog.Logger) *Service {
	return &Service{store: s, cache: c, rdb: rdb, log: log}
}

// Stats returns durable totals for an account
func (s *Service) Stats(accountID string) stats.AccountStats {
	st, err := s.store.AccountStats(context.Background(), accountID)
	if err != nil { 
		s.log.Error("failed to read account stats", "account_id", accountID,"err",err)
		return stats.AccountStats{}
	}
	return stats.AccountStats{
		CallCount: st.CallCount,
		TotalDurationSec: st.TotalDurationSec,
	}
}

// Ingest stores a delivery and kicks off processing. Processing runs
// asynchronously so the provider gets a fast acknowledgement.
func (s *Service) Ingest(ctx context.Context, evt Event) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	rec := store.Event{
		EventID:      evt.EventID,
		CallID:       evt.CallID,
		AccountID:    evt.AccountID,
		Status:       evt.Status,
		DurationSec:  evt.DurationSec,
		RecordingURL: evt.RecordingURL,
		OccurredAt:   evt.OccurredAt,
		Payload:      payload,
	}

	isNew, err := s.store.IngestEvent(ctx, rec)
	if err != nil {
		return err
	}
	if !isNew {
		s.log.Info("duplicate delivery ignored", "event_id", evt.EventID)
		return nil
	}

	s.cache.Record(rec.AccountID, rec.DurationSec)

	if rec.RecordingURL != "" {
		// Detach from the request context so HTTP cancellation doesn't kill the background job
		bgCtx := context.WithoutCancel(ctx)
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			if err := s.processRecording(bgCtx, rec); err != nil {
				s.log.Error("failed to process recording", "event_id", rec.EventID, "error", err)
			}
		}()
		
	}
	return nil
}

// processRecording downloads and transcodes the call recording, then marks
// the call as done.
func (s *Service) processRecording(ctx context.Context, rec store.Event) error {
	time.Sleep(recordingWork)
	return s.store.MarkRecordingProcessed(ctx, rec.CallID)
}
func (s *Service) Wait() {
    s.wg.Wait()
}

func (s *Service) RecoverPendingRecordings(ctx context.Context) error {
	recordings, err := s.store.PendingRecordings(ctx)
	if err != nil {
		return err
	}
	s.log.Info("pending recordings found", "count", len(recordings))
	for _, rec := range recordings { 
		s.log.Info("recovering recording", "call_id", rec.CallID)
		if err := s.processRecording(ctx, rec); err != nil { 
			s.log.Error(
				"failed to recover recording",
				"call_id", rec.CallID,
				"error", err,
			)
		}
	}
	return nil
}