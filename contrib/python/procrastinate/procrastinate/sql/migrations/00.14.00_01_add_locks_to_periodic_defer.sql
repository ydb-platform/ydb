ALTER TABLE procrastinate_periodic_defers
    ADD COLUMN queue_name character varying(128) NOT NULL DEFAULT 'undefined';

ALTER TABLE procrastinate_periodic_defers
    ALTER COLUMN queue_name DROP DEFAULT;

ALTER TABLE procrastinate_periodic_defers
    DROP CONSTRAINT procrastinate_periodic_defers_task_name_defer_timestamp_key;

ALTER TABLE procrastinate_periodic_defers
    ADD CONSTRAINT procrastinate_periodic_defers_unique UNIQUE (task_name, queue_name, defer_timestamp);

DROP FUNCTION IF EXISTS procrastinate_defer_periodic_job;
CREATE FUNCTION procrastinate_defer_periodic_job(
    _queue_name character varying,
    _lock character varying,
    _queueing_lock character varying,
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
        INTO procrastinate_periodic_defers (task_name, queue_name, defer_timestamp)
        VALUES (_task_name, _queue_name, _defer_timestamp)
        ON CONFLICT DO NOTHING
        RETURNING id into _defer_id;

    IF _defer_id IS NULL THEN
        RETURN NULL;
    END IF;

    UPDATE procrastinate_periodic_defers
        SET job_id = procrastinate_defer_job(
                _queue_name,
                _task_name,
                _lock,
                _queueing_lock,
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
