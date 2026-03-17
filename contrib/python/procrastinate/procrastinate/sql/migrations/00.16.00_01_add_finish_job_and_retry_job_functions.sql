CREATE FUNCTION procrastinate_finish_job(job_id integer, end_status procrastinate_job_status) RETURNS void
    LANGUAGE plpgsql
    AS $$
BEGIN
    UPDATE procrastinate_jobs
    SET status = end_status,
        attempts = attempts + 1
    WHERE id = job_id;
END;
$$;

CREATE FUNCTION procrastinate_retry_job(job_id integer, retry_at timestamp with time zone) RETURNS void
    LANGUAGE plpgsql
    AS $$
BEGIN
    UPDATE procrastinate_jobs
    SET status = 'todo',
        attempts = attempts + 1,
        scheduled_at = retry_at
    WHERE id = job_id;
END;
$$;
