// Package store persists webhook events, calls, and per-account aggregates.
package store

import (
	"context"
	"errors"
	"time"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type dbtx interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Event is one call-completion webhook delivery.
type Event struct {
	EventID      string
	CallID       string
	AccountID    string
	Status       string
	DurationSec  int
	RecordingURL string
	OccurredAt   time.Time
	Payload      []byte
}

// Stats is the durable per-account aggregate.
type Stats struct {
	CallCount        int64
	TotalDurationSec int64
}

// Store is a Postgres-backed repository.
type Store struct {
	pool *pgxpool.Pool
}

// New opens a connection pool bounded to maxConns.
func New(ctx context.Context, dsn string, maxConns int32) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = maxConns

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool}, nil
}

// Pool exposes the underlying pool for tests and ad-hoc queries.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Close releases all pooled connections.
func (s *Store) Close() { s.pool.Close() }

// IngestEvent coordinates the transaction to store an event and update relevant entities.
func (s *Store) IngestEvent(ctx context.Context, e Event) (isNew bool, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	// If Commit succeeds below, this Rollback is a no-op (tx is already closed).
	defer tx.Rollback(ctx)

	isNew, err = insertEventIfNew(ctx, tx, e)
	if err != nil {
		return false, err
	}
	if !isNew {
		// Nothing to roll back beyond the no-op insert; just end the tx.
		return false, tx.Commit(ctx)
	}

	if err := upsertCall(ctx, tx, e); err != nil {
		return false, err
	}
	if err := incrementAccountStats(ctx, tx, e.AccountID, e.DurationSec); err != nil {
		return false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

func insertEventIfNew(ctx context.Context, db dbtx, e Event) (bool, error) {
	var id int64
	err := db.QueryRow(ctx,
		`INSERT INTO events (event_id, call_id, account_id, payload)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (event_id) DO NOTHING
		 RETURNING id`,
		e.EventID, e.CallID, e.AccountID, e.Payload).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func upsertCall(ctx context.Context, db dbtx, e Event) error {
	_, err := db.Exec(ctx,
		`INSERT INTO calls (call_id, account_id, status, duration_sec, recording_url, updated_at)
		 VALUES ($1, $2, $3, $4, $5, now())
		 ON CONFLICT (call_id) DO UPDATE SET
		     status        = EXCLUDED.status,
		     duration_sec  = EXCLUDED.duration_sec,
		     recording_url = EXCLUDED.recording_url,
		     updated_at    = now()`,
		e.CallID, e.AccountID, e.Status, e.DurationSec, e.RecordingURL)
	return err
}

// MarkRecordingProcessed flags the call's recording as handled.
func (s *Store) MarkRecordingProcessed(ctx context.Context, callID string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE calls SET recording_processed = TRUE,
				recording_processing = FALSE, 
				recording_processing_started_at = NULL, 
				updated_at = now()
		 WHERE call_id = $1`, callID)
	return err
}

func incrementAccountStats(ctx context.Context, db dbtx, accountID string, durationSec int) error {
	_, err := db.Exec(ctx,
		`INSERT INTO account_stats (account_id, call_count, total_duration_sec)
		 VALUES ($1, 1, $2)
		 ON CONFLICT (account_id) DO UPDATE SET
		     call_count         = account_stats.call_count + 1,
		     total_duration_sec = account_stats.total_duration_sec + EXCLUDED.total_duration_sec`,
		accountID, durationSec)
	return err
}

// AccountStats reads the durable aggregate. A missing account reads as zero.
func (s *Store) AccountStats(ctx context.Context, accountID string) (Stats, error) {
	var st Stats
	err := s.pool.QueryRow(ctx,
		`SELECT call_count, total_duration_sec FROM account_stats WHERE account_id = $1`,
		accountID).Scan(&st.CallCount, &st.TotalDurationSec)
	if errors.Is(err, pgx.ErrNoRows) {
		return Stats{}, nil
	}
	if err != nil {
		return Stats{}, err
	}
	return st, nil
}

func (s *Store) PendingRecordings(ctx context.Context) ([]Event, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT call_id, account_id, recording_url
		FROM calls 
		WHERE recording_url IS NOT NULL 
			AND recording_processed = FALSE`)
		
	if err != nil { 
		return nil, err
	}
	defer rows.Close() 
	var recordings []Event 
	for rows.Next() {
		var rec Event 
		if err := rows.Scan(
			&rec.CallID, 
			&rec.AccountID, 
			&rec.RecordingURL, 
		); err != nil { 
			return nil, err
		}
		recordings = append(recordings, rec)
	}
	if err := rows.Err(); err != nil { 
		return nil, err
	}
	return recordings, nil
}

func (s *Store) ClaimPendingRecording(ctx context.Context) (*Event, error){
	// defining the query that we will use to lock an unprocessed event in db and 
	// return the data of that.
	query:= `WITH candidate AS (
		SELECT call_id
		FROM calls 
		WHERE recording_url IS NOT NULL
			AND recording_processed IS FALSE
			AND recording_processing IS FALSE
			AND recording_attempts < 5
		ORDER BY updated_at
		FOR UPDATE SKIP LOCKED 
		LIMIT 1  
	)
	UPDATE calls 
	SET recording_processing = TRUE,
		recording_processing_started_at = NOW(),
		recording_attempts = recording_attempts + 1,
		updated_at = NOW() 
	WHERE call_id = (SELECT call_id FROM Candidate)
	RETURNING call_id, account_id, recording_url `

	var rec Event
	// Here we get the row data of the Event that is not processed yet.
	err := s.pool.QueryRow(ctx, query).Scan(
		&rec.CallID,
		&rec.AccountID,
		&rec.RecordingURL,
	)
	// If we dont get any rows, then we return nothing
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	// If we got some error while fetching the Event from db, we return that.
	if err != nil {
		return nil, err
	}
	// else we return the Pointer to the Event.
	return &rec, nil
}

// WatchDog function that is going to keep checking for unprocessed records claimed by 
// some worker which has exceeded time limit 

func (s *Store) ResetStaleRecordingClaims(ctx context.Context, timeOut time.Duration) (int64, error){
	result, err := s.pool.Exec(ctx, `
        UPDATE calls
        SET recording_processing = FALSE,
            recording_processing_started_at = NULL
        WHERE recording_processing = TRUE
          AND recording_processed = FALSE
          AND recording_processing_started_at
              < NOW() - ($1 * INTERVAL '1 second')
    `, timeOut.Seconds())
	if err != nil { 
		return 0, err
	}
	return result.RowsAffected(), nil
}

func (s *Store) ReleaseRecordingClaim(ctx context.Context, callID string) (error){
	_, err := s.pool.Exec(ctx, `
		UPDATE calls
		SET recording_processing = FALSE,
			recording_processing_started_at = NULL,
			updated_at = NOW()
		WHERE call_id = $1 	
			AND recording_processing = TRUE
			AND recording_processed = FALSE
	`, callID)
	if err != nil {
		return err
	}
	return nil
}