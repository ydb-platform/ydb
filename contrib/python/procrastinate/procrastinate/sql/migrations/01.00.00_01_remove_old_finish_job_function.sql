-- remove old procrastinate_finish_job functions

-- https://github.com/procrastinate-org/procrastinate/pull/336
DROP FUNCTION IF EXISTS procrastinate_finish_job(integer, procrastinate_job_status, timestamp with time zone);

-- https://github.com/procrastinate-org/procrastinate/pull/354
DROP FUNCTION IF EXISTS procrastinate_finish_job(integer, procrastinate_job_status);

-- https://github.com/procrastinate-org/procrastinate/pull/381
CREATE OR REPLACE FUNCTION procrastinate_finish_job(job_id integer, end_status procrastinate_job_status, delete_job boolean) RETURNS void
    LANGUAGE plpgsql
AS $$
DECLARE
    _job_id bigint;
BEGIN
    IF end_status NOT IN ('succeeded', 'failed') THEN
        RAISE 'End status should be either "succeeded" or "failed" (job id: %)', job_id;
    END IF;
    IF delete_job THEN
        DELETE FROM procrastinate_jobs
        WHERE id = job_id AND status IN ('todo', 'doing')
        RETURNING id INTO _job_id;
    ELSE
        UPDATE procrastinate_jobs
        SET status = end_status,
            attempts =
                CASE
                    WHEN status = 'doing' THEN attempts + 1
                    ELSE attempts
                END
        WHERE id = job_id AND status IN ('todo', 'doing')
        RETURNING id INTO _job_id;
    END IF;
    IF _job_id IS NULL THEN
        RAISE 'Job was not found or not in "doing" or "todo" status (job id: %)', job_id;
    END IF;
END;
$$;

-- https://github.com/procrastinate-org/procrastinate/pull/471
DROP FUNCTION IF EXISTS procrastinate_defer_periodic_job(character varying, character varying, character varying, character varying, bigint);

ALTER TABLE procrastinate_periodic_defers DROP COLUMN "queue_name";
