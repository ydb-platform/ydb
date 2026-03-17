CREATE TABLE procrastinate_periodic_defers (
    id bigserial PRIMARY KEY,
    task_name character varying(128) NOT NULL,
    defer_timestamp bigint,
    job_id bigint REFERENCES procrastinate_jobs(id) NULL,
    UNIQUE (task_name, defer_timestamp)
);
-- When parameters change, function must be dropped.
DROP FUNCTION IF EXISTS procrastinate_defer_job;
CREATE FUNCTION procrastinate_defer_job(
    queue_name character varying,
    task_name character varying,
    lock text,
    queueing_lock text,
    args jsonb,
    scheduled_at timestamp with time zone
) RETURNS bigint
    LANGUAGE plpgsql
    AS $$
DECLARE
	job_id bigint;
BEGIN
    INSERT INTO procrastinate_jobs (queue_name, task_name, lock, queueing_lock, args, scheduled_at)
    VALUES (queue_name, task_name, lock, queueing_lock, args, scheduled_at)
    RETURNING id INTO job_id;

    RETURN job_id;
END;
$$;

DROP FUNCTION IF EXISTS procrastinate_defer_periodic_job;
CREATE FUNCTION procrastinate_defer_periodic_job(
    _queue_name character varying,
    _task_name character varying,
    _defer_timestamp bigint
) RETURNS bigint
    LANGUAGE plpgsql
    AS $$
DECLARE
	_job_id bigint;
	_defer_id bigint;
BEGIN

    INSERT
        INTO procrastinate_periodic_defers (task_name, defer_timestamp)
        VALUES (_task_name, _defer_timestamp)
        ON CONFLICT DO NOTHING
        RETURNING id into _defer_id;

    IF _defer_id IS NULL THEN
        RETURN NULL;
    END IF;

    UPDATE procrastinate_periodic_defers
        SET job_id = procrastinate_defer_job(
                _queue_name,
                _task_name,
                NULL,
                NULL,
                ('{"timestamp": ' || _defer_timestamp || '}')::jsonb,
                NULL
            )
        WHERE id = _defer_id
        RETURNING job_id INTO _job_id;

    DELETE
        FROM procrastinate_periodic_defers
        WHERE
            _job_id IS NOT NULL
            AND procrastinate_periodic_defers.task_name = _task_name
            AND procrastinate_periodic_defers.defer_timestamp < _defer_timestamp;

    RETURN _job_id;
END;
$$;
