CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;

CREATE TABLE IF NOT EXISTS procrastinate_version (
    id BIGSERIAL PRIMARY KEY,
    version TEXT,
    name TEXT,
    applied_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TYPE procrastinate_job_status AS ENUM (
    'todo',  -- The job is queued
    'doing',  -- The job has been fetched by a worker
    'succeeded',  -- The job ended successfully
    'failed'  -- The job ended with an error
);

CREATE TYPE procrastinate_job_event_type AS ENUM (
    'deferred',  -- Job created, in todo
    'started',  -- todo -> doing
    'deferred_for_retry',  -- doing -> todo
    'failed',  -- doing -> failed
    'succeeded',  -- doing -> succeeded
    'cancelled', -- todo -> failed or succeeded
    'scheduled' -- not an event transition, but recording when a task is scheduled for
);

CREATE TABLE procrastinate_jobs (
    id bigserial PRIMARY KEY,
    queue_name character varying(128) NOT NULL,
    task_name character varying(128) NOT NULL,
    lock text,
    args jsonb DEFAULT '{}' NOT NULL,
    status procrastinate_job_status DEFAULT 'todo'::procrastinate_job_status NOT NULL,
    scheduled_at timestamp with time zone NULL,
    started_at timestamp with time zone NULL,
    attempts integer DEFAULT 0 NOT NULL
);

CREATE TABLE procrastinate_events (
    id BIGSERIAL PRIMARY KEY,
    job_id integer NOT NULL REFERENCES procrastinate_jobs ON DELETE CASCADE,
    type procrastinate_job_event_type,
    at timestamp with time zone DEFAULT NOW() NULL
);

CREATE TABLE procrastinate_job_locks (
    object text PRIMARY KEY
);

CREATE FUNCTION procrastinate_fetch_job(target_queue_names character varying[]) RETURNS procrastinate_jobs
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
		SET status = 'doing',
            started_at = NOW()
		FROM potential_job, lock_object
        WHERE lock_object.object IS NOT NULL
		AND procrastinate_jobs.id = potential_job.id
		RETURNING * INTO found_jobs;

	RETURN found_jobs;
END;
$$;

CREATE FUNCTION procrastinate_finish_job(job_id integer, end_status procrastinate_job_status, next_scheduled_at timestamp with time zone) RETURNS void
    LANGUAGE plpgsql
    AS $$
BEGIN
	WITH finished_job AS (
		UPDATE procrastinate_jobs
        SET status = end_status,
            attempts = attempts + 1,
            scheduled_at = COALESCE(next_scheduled_at, scheduled_at)
        WHERE id = job_id RETURNING lock
	)
	DELETE FROM procrastinate_job_locks WHERE object = (SELECT lock FROM finished_job);
END;
$$;

CREATE FUNCTION procrastinate_notify_queue() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
	PERFORM pg_notify('procrastinate_queue#' || NEW.queue_name, NEW.task_name);
	PERFORM pg_notify('procrastinate_any_queue', NEW.task_name);
	RETURN NEW;
END;
$$;

CREATE FUNCTION procrastinate_trigger_status_events_procedure_insert() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    INSERT INTO procrastinate_events(job_id, type)
        VALUES (NEW.id, 'deferred'::procrastinate_job_event_type);
	RETURN NEW;
END;
$$;

CREATE FUNCTION procrastinate_trigger_status_events_procedure_update() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    WITH t AS (
        SELECT CASE
            WHEN OLD.status = 'todo'::procrastinate_job_status
                AND NEW.status = 'doing'::procrastinate_job_status
                THEN 'started'::procrastinate_job_event_type
            WHEN OLD.status = 'doing'::procrastinate_job_status
                AND NEW.status = 'todo'::procrastinate_job_status
                THEN 'deferred_for_retry'::procrastinate_job_event_type
            WHEN OLD.status = 'doing'::procrastinate_job_status
                AND NEW.status = 'failed'::procrastinate_job_status
                THEN 'failed'::procrastinate_job_event_type
            WHEN OLD.status = 'doing'::procrastinate_job_status
                AND NEW.status = 'succeeded'::procrastinate_job_status
                THEN 'succeeded'::procrastinate_job_event_type
            WHEN OLD.status = 'todo'::procrastinate_job_status
                AND (
                    NEW.status = 'failed'::procrastinate_job_status
                    OR NEW.status = 'succeeded'::procrastinate_job_status
                )
                THEN 'cancelled'::procrastinate_job_event_type
            ELSE NULL
        END as event_type
    )
    INSERT INTO procrastinate_events(job_id, type)
        SELECT NEW.id, t.event_type
        FROM t
        WHERE t.event_type IS NOT NULL;
	RETURN NEW;
END;
$$;

CREATE FUNCTION procrastinate_trigger_scheduled_events_procedure() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    INSERT INTO procrastinate_events(job_id, type, at)
        VALUES (NEW.id, 'scheduled'::procrastinate_job_event_type, NEW.scheduled_at);

	RETURN NEW;
END;
$$;

CREATE INDEX ON procrastinate_jobs(queue_name);

CREATE TRIGGER procrastinate_jobs_notify_queue
    AFTER INSERT ON procrastinate_jobs
    FOR EACH ROW WHEN ((new.status = 'todo'::procrastinate_job_status))
    EXECUTE PROCEDURE procrastinate_notify_queue();

CREATE TRIGGER procrastinate_trigger_status_events_update
    AFTER UPDATE OF status ON procrastinate_jobs
    FOR EACH ROW
    EXECUTE PROCEDURE procrastinate_trigger_status_events_procedure_update();

CREATE TRIGGER procrastinate_trigger_status_events_insert
    AFTER INSERT ON procrastinate_jobs
    FOR EACH ROW WHEN ((new.status = 'doing'::procrastinate_job_status))
    EXECUTE PROCEDURE procrastinate_trigger_status_events_procedure_insert();

CREATE TRIGGER procrastinate_trigger_scheduled_events
    AFTER UPDATE OR INSERT ON procrastinate_jobs
    FOR EACH ROW WHEN ((new.scheduled_at IS NOT NULL AND new.status = 'todo'::procrastinate_job_status))
    EXECUTE PROCEDURE procrastinate_trigger_scheduled_events_procedure();

CREATE INDEX procrastinate_events_job_id_fkey ON procrastinate_events(job_id);
