-- These are old versions of functions, that we needed to keep around for
-- backwards compatibility. We can now safely drop them.
DROP FUNCTION IF EXISTS procrastinate_finish_job(
    integer,
    procrastinate_job_status,
    timestamp with time zone,
    boolean
);
DROP FUNCTION IF EXISTS procrastinate_defer_job(
    character varying,
    character varying,
    text,
    text,
    jsonb,
    timestamp with time zone
);
DROP FUNCTION IF EXISTS procrastinate_defer_periodic_job(
    character varying,
    character varying,
    character varying,
    character varying,
    character varying,
    bigint,
    jsonb
);
DROP FUNCTION IF EXISTS procrastinate_retry_job(
    bigint,
    timestamp with time zone
);
DROP FUNCTION IF EXISTS procrastinate_retry_job(
    bigint,
    timestamp with time zone,
    integer,
    character varying,
    character varying
);

-- Remove all traces of the "aborting" status
-- Last sanity update in case the trigger didn't work 100% of the time
UPDATE procrastinate_jobs SET abort_requested = true WHERE status = 'aborting';

-- Delete the indexes that depend on the old status and enum type
DROP INDEX IF EXISTS procrastinate_jobs_queueing_lock_idx;
DROP INDEX IF EXISTS procrastinate_jobs_lock_idx;
DROP INDEX IF EXISTS procrastinate_jobs_id_lock_idx;

-- Delete the unversioned triggers
DROP TRIGGER IF EXISTS procrastinate_trigger_status_events_update ON procrastinate_jobs;
DROP TRIGGER IF EXISTS procrastinate_trigger_status_events_insert ON procrastinate_jobs;
DROP TRIGGER IF EXISTS procrastinate_trigger_scheduled_events ON procrastinate_jobs;
DROP TRIGGER IF EXISTS procrastinate_trigger_status_events_update ON procrastinate_jobs;
DROP TRIGGER IF EXISTS procrastinate_jobs_notify_queue ON procrastinate_jobs;

-- Delete the unversioned functions
DROP FUNCTION IF EXISTS procrastinate_defer_job;
DROP FUNCTION IF EXISTS procrastinate_defer_periodic_job;
DROP FUNCTION IF EXISTS procrastinate_fetch_job;
DROP FUNCTION IF EXISTS procrastinate_finish_job(bigint, procrastinate_job_status, boolean);
DROP FUNCTION IF EXISTS procrastinate_cancel_job;
DROP FUNCTION IF EXISTS procrastinate_trigger_status_events_procedure_update;
DROP FUNCTION IF EXISTS procrastinate_finish_job(integer, procrastinate_job_status, timestamp with time zone, boolean);
DROP FUNCTION IF EXISTS procrastinate_notify_queue;

-- Delete the functions that depend on the old event type
DROP FUNCTION IF EXISTS procrastinate_trigger_status_events_procedure_insert;
DROP FUNCTION IF EXISTS procrastinate_trigger_scheduled_events_procedure;

-- Delete temporary triggers and functions
DROP TRIGGER IF EXISTS procrastinate_jobs_notify_queue_job_inserted_temp ON procrastinate_jobs;
DROP TRIGGER IF EXISTS procrastinate_jobs_notify_queue_job_aborted_temp ON procrastinate_jobs;
DROP TRIGGER IF EXISTS procrastinate_trigger_sync_abort_requested_with_status_temp ON procrastinate_jobs;
DROP FUNCTION IF EXISTS procrastinate_sync_abort_requested_with_status_temp;

-- Alter the table to not use the 'aborting' status anymore
ALTER TABLE procrastinate_jobs
ALTER COLUMN status TYPE procrastinate_job_status
USING (
    CASE status::text
        WHEN 'aborting' THEN 'doing'::procrastinate_job_status
        ELSE status::procrastinate_job_status
    END
);

-- Recreate the dropped temporary triggers
CREATE TRIGGER procrastinate_jobs_notify_queue_job_inserted_v1
    AFTER INSERT ON procrastinate_jobs
    FOR EACH ROW WHEN ((new.status = 'todo'::procrastinate_job_status))
    EXECUTE PROCEDURE procrastinate_notify_queue_job_inserted_v1();
CREATE TRIGGER procrastinate_jobs_notify_queue_job_aborted_v1
    AFTER UPDATE OF abort_requested ON procrastinate_jobs
    FOR EACH ROW WHEN ((old.abort_requested = false AND new.abort_requested = true AND new.status = 'doing'::procrastinate_job_status))
    EXECUTE PROCEDURE procrastinate_notify_queue_abort_job_v1();

-- Recreate the dropped indexes (with version suffix)
CREATE UNIQUE INDEX procrastinate_jobs_queueing_lock_idx_v1 ON procrastinate_jobs (queueing_lock) WHERE status = 'todo';
CREATE UNIQUE INDEX procrastinate_jobs_lock_idx_v1 ON procrastinate_jobs (lock) WHERE status = 'doing';
CREATE INDEX procrastinate_jobs_id_lock_idx_v1 ON procrastinate_jobs (id, lock) WHERE status = ANY (ARRAY['todo'::procrastinate_job_status, 'doing'::procrastinate_job_status]);

-- Rename existing indexes
ALTER INDEX procrastinate_jobs_queue_name_idx RENAME TO procrastinate_jobs_queue_name_idx_v1;
ALTER INDEX procrastinate_events_job_id_fkey  RENAME TO procrastinate_events_job_id_fkey_v1;
ALTER INDEX procrastinate_periodic_defers_job_id_fkey RENAME TO procrastinate_periodic_defers_job_id_fkey_v1;
ALTER INDEX procrastinate_jobs_priority_idx RENAME TO procrastinate_jobs_priority_idx_v1;

-- Recreate or rename the other triggers & their associated functions

CREATE FUNCTION procrastinate_trigger_function_status_events_insert_v1()
    RETURNS trigger
    LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO procrastinate_events(job_id, type)
        VALUES (NEW.id, 'deferred'::procrastinate_job_event_type);
	RETURN NEW;
END;
$$;

CREATE TRIGGER procrastinate_trigger_status_events_insert_v1
    AFTER INSERT ON procrastinate_jobs
    FOR EACH ROW WHEN ((new.status = 'todo'::procrastinate_job_status))
    EXECUTE PROCEDURE procrastinate_trigger_function_status_events_insert_v1();

CREATE FUNCTION procrastinate_trigger_function_status_events_update_v1()
    RETURNS trigger
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
                    NEW.status = 'cancelled'::procrastinate_job_status
                    OR NEW.status = 'failed'::procrastinate_job_status
                    OR NEW.status = 'succeeded'::procrastinate_job_status
                )
                THEN 'cancelled'::procrastinate_job_event_type
            WHEN OLD.status = 'doing'::procrastinate_job_status
                AND NEW.status = 'aborted'::procrastinate_job_status
                THEN 'aborted'::procrastinate_job_event_type
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

CREATE TRIGGER procrastinate_trigger_status_events_update_v1
    AFTER UPDATE OF status ON procrastinate_jobs
    FOR EACH ROW
    EXECUTE PROCEDURE procrastinate_trigger_function_status_events_update_v1();

CREATE FUNCTION procrastinate_trigger_function_scheduled_events_v1()
    RETURNS trigger
    LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO procrastinate_events(job_id, type, at)
        VALUES (NEW.id, 'scheduled'::procrastinate_job_event_type, NEW.scheduled_at);

	RETURN NEW;
END;
$$;

CREATE TRIGGER procrastinate_trigger_scheduled_events_v1
    AFTER UPDATE OR INSERT ON procrastinate_jobs
    FOR EACH ROW WHEN ((new.scheduled_at IS NOT NULL AND new.status = 'todo'::procrastinate_job_status))
    EXECUTE PROCEDURE procrastinate_trigger_function_scheduled_events_v1();

CREATE FUNCTION procrastinate_trigger_abort_requested_events_procedure_v1()
    RETURNS trigger
    LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO procrastinate_events(job_id, type)
        VALUES (NEW.id, 'abort_requested'::procrastinate_job_event_type);
    RETURN NEW;
END;
$$;

CREATE TRIGGER procrastinate_trigger_abort_requested_events_v1
    AFTER UPDATE OF abort_requested ON procrastinate_jobs
    FOR EACH ROW WHEN ((new.abort_requested = true))
    EXECUTE PROCEDURE procrastinate_trigger_abort_requested_events_procedure_v1();

-- Rename remaining functions to use version suffix
ALTER FUNCTION procrastinate_unlink_periodic_defers RENAME TO procrastinate_unlink_periodic_defers_v1;
ALTER TRIGGER procrastinate_trigger_delete_jobs ON procrastinate_jobs RENAME TO procrastinate_trigger_delete_jobs_v1;

-- New constraints
ALTER TABLE procrastinate_jobs ADD CONSTRAINT check_not_todo_abort_requested CHECK (NOT (status = 'todo' AND abort_requested = true));
