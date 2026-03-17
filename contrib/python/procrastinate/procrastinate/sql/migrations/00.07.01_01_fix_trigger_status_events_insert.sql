-- re-create the procrastinate_trigger_status_events_insert trigger
DROP TRIGGER procrastinate_trigger_status_events_insert on procrastinate_jobs;
CREATE TRIGGER procrastinate_trigger_status_events_insert
    AFTER INSERT ON procrastinate_jobs
    FOR EACH ROW WHEN ((new.status = 'todo'::procrastinate_job_status))
    EXECUTE PROCEDURE procrastinate_trigger_status_events_procedure_insert();
