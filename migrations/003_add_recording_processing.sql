ALTER TABLE calls 
ADD COLUMN recording_processing BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE calls
ADD COLUMN recording_processing_started_at TIMESTAMPTZ;

ALTER TABLE calls
ADD COLUMN recording_attempts INT NOT NULL DEFAULT 0;