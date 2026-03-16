-- replace the procrastinate_fetch_job function with a version that doesn't
-- set the started_at field when updating the procrastinate_jobs table
CREATE OR REPLACE FUNCTION procrastinate_fetch_job(target_queue_names character varying[]) RETURNS procrastinate_jobs
    LANGUAGE plpgsql
    AS $$
DECLARE
	found_jobs procrastinate_jobs;
BEGIN
	WITH potential_job AS (
		SELECT procrastinate_jobs.*
			FROM procrastinate_jobs
			LEFT JOIN procrastinate_job_locks ON procrastinate_job_locks.object = procrastinate_jobs.lock
			WHERE (target_queue_names IS NULL OR queue_name = ANY( target_queue_names ))
			  AND procrastinate_job_locks.object IS NULL
			  AND status = 'todo'
			  AND (scheduled_at IS NULL OR scheduled_at <= now())
            ORDER BY id ASC
			FOR UPDATE OF procrastinate_jobs SKIP LOCKED LIMIT 1
	), lock_object AS (
		INSERT INTO procrastinate_job_locks
			SELECT lock FROM potential_job
            ON CONFLICT DO NOTHING
            RETURNING object
	)
	UPDATE procrastinate_jobs
		SET status = 'doing'
		FROM potential_job, lock_object
        WHERE lock_object.object IS NOT NULL
		AND procrastinate_jobs.id = potential_job.id
		RETURNING * INTO found_jobs;

	RETURN found_jobs;
END;
$$;
