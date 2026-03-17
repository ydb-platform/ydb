CREATE FUNCTION procrastinate_finish_job(job_id integer, end_status procrastinate_job_status, delete_job boolean) RETURNS void
    LANGUAGE plpgsql
AS $$
BEGIN
    IF delete_job THEN
        DELETE FROM procrastinate_jobs WHERE id = job_id;
    ELSE
        UPDATE procrastinate_jobs
        SET status = end_status,
            attempts = attempts + 1
        WHERE id = job_id;
    END IF;
END;
$$;
