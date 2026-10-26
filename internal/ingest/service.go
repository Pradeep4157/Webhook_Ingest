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

	// if rec.RecordingURL != "" {
	// 	// Detach from the request context so HTTP cancellation doesn't kill the background job
	// 	bgCtx := context.WithoutCancel(ctx)
	// 	s.wg.Add(1)
	// 	go func() {
	// 		defer s.wg.Done()
	// 		if err := s.processRecording(bgCtx, rec); err != nil {
	// 			s.log.Error("failed to process recording", "event_id", rec.EventID, "error", err)
	// 		}
	// 	}()
		
	// }
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
			// relCtx, cancel := context.WithTimeout(context.Background(), 5 * time.Second())
			// defer cancel()
			// if relError := s.store.ReleaseRecordingClaim(relCtx, rec.CallID); relErrr != nil {
			// 	s.log.Error("release claim failed", "call_id", rec.CallID,  "error", relError)
			// }
			s.log.Error(
				"failed to recover recording",
				"call_id", rec.CallID,
				"error", err,
			)
		}
	}
	return nil
}

func (s *Service) ProcessNextRecording(ctx context.Context) (*store.Event, error) {
	rec, err := s.store.ClaimPendingRecording(ctx)
	if err != nil { 
		return nil, err
	}
	if rec == nil { 
		return nil, nil
	}
	// now we have claimed some recording atomically from db, now we just need to process it. 
	if err := s.processRecording(ctx, *rec); err != nil { 
		// there was some error while processing the recording, 
		// so instead of waiting for worker to fix it, we can free it now only. 
		releaseCtx, cancel := context.WithTimeout(
			context.Background(), 5*time.Second,
		)
		defer cancel()
		if releaseError := s.store.ReleaseRecordingClaim(releaseCtx, rec.CallID); releaseError != nil { 
			s.log.Error("failed to release recording Claim", "call_id", rec.CallID, "error", releaseError)
		}
		return rec, err
	}
	return rec, nil
}

func (s *Service) Worker(ctx context.Context) {
	for ctx.Err() == nil { 
		rec, err := s.ProcessNextRecording(ctx) 
		if err != nil { 
			s.log.Error("recording processing failed", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
			continue
		}
		if rec == nil { 
			select {
			case <- ctx.Done():
				return
			case <-time.After(500 * time.Millisecond):
			}
		}
	}
}

func (s *Service) WatchDog(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	const staleTimeOut = 5 * time.Minute

	for {
		select {
		case <- ctx.Done():
				return
		case <- ticker.C:
			reclaimed, err := s.store.ReleaseStaleRecordingClaim(ctx, staleTimeOut)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				s.log.Error("WatchDog failed to reset stale recording claims", "error", err)
				continue
			}
			if reclaimed > 0 {
				s.log.Info("WatchDog reclaimed stale recording claims", "count", reclaimed)
				
			}
		}
	}
}