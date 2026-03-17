CREATE INDEX IF NOT EXISTS procrastinate_jobs_id_lock_idx ON procrastinate_jobs (id, lock) WHERE status = ANY (ARRAY['todo'::procrastinate_job_status, 'doing'::procrastinate_job_status]);
