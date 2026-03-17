-- add a queueing_lock column to the procrastinate_jobs table
ALTER TABLE procrastinate_jobs ADD COLUMN queueing_lock text;
CREATE UNIQUE INDEX procrastinate_jobs_queueing_lock_idx ON procrastinate_jobs (queueing_lock) WHERE status = 'todo';
