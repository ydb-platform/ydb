from __future__ import annotations

from procrastinate import blueprints, job_context

builtin = blueprints.Blueprint()


@builtin.task(pass_context=True, queue="builtin")
async def remove_old_jobs(
    context: job_context.JobContext,
    *,
    max_hours: int,
    queue: str | None = None,
    remove_failed: bool | None = False,
    remove_cancelled: bool | None = False,
    remove_aborted: bool | None = False,
) -> None:
    """
    This task cleans your database by removing old jobs. Note that jobs and linked
    events will be irreversibly removed from the database when running this task.

    Parameters
    ----------
    max_hours :
        Only jobs which were finished more than ``max_hours`` ago will be deleted.
    queue :
        The name of the queue in which jobs will be deleted. If not specified, the
        task will delete jobs from all queues.
    remove_failed:
        By default only successful jobs will be removed. When this parameter is True
        failed jobs will also be deleted.
    remove_cancelled:
        By default only successful jobs will be removed. When this parameter is True
        cancelled jobs will also be deleted.
    remove_aborted:
        By default only successful jobs will be removed. When this parameter is True
        aborted jobs will also be deleted.
    """
    await context.app.job_manager.delete_old_jobs(
        nb_hours=max_hours,
        queue=queue,
        include_failed=remove_failed,
        include_cancelled=remove_cancelled,
        include_aborted=remove_aborted,
    )
