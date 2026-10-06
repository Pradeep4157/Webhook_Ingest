ALTER TABLE calls 
ADD COLUMN recording_processed BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE calls
ADD COLUMN recording_processing_started_at TIMESTAMPTZ 

