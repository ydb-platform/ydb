-- recreate procrastinate_jobs_id_lock_idx index by adding aborting status to the filter so that it can be used by the fetch job function
CREATE INDEX procrastinate_jobs_id_lock_idx_temp ON procrastinate_jobs (id, lock) WHERE status = ANY (ARRAY['todo'::procrastinate_job_status, 'doing'::procrastinate_job_status, 'aborting'::procrastinate_job_status]);
DROP INDEX procrastinate_jobs_id_lock_idx;
ALTER INDEX procrastinate_jobs_id_lock_idx_temp RENAME TO procrastinate_jobs_id_lock_idx;

-- add index to avoid seq scan of outer query in the fetch job function
CREATE INDEX procrastinate_jobs_priority_idx ON procrastinate_jobs(priority desc, id asc) WHERE (status = 'todo'::procrastinate_job_status);
