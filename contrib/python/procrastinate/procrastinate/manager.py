from __future__ import annotations

import datetime
import json
import logging
import warnings
from collections.abc import Awaitable, Iterable
from typing import Any, NoReturn, Protocol

from procrastinate import connector, exceptions, sql, types, utils
from procrastinate import jobs as jobs_module

logger = logging.getLogger(__name__)

QUEUEING_LOCK_CONSTRAINT = "procrastinate_jobs_queueing_lock_idx_v1"


class NotificationCallback(Protocol):
    def __call__(
        self, *, channel: str, notification: jobs_module.Notification
    ) -> Awaitable[None]: ...


def get_channel_for_queues(queues: Iterable[str] | None = None) -> Iterable[str]:
    if queues is None:
        return ["procrastinate_any_queue_v1"]
    else:
        return ["procrastinate_queue_v1#" + queue for queue in queues]


class JobManager:
    def __init__(self, connector: connector.BaseConnector):
        self.connector = connector

    async def defer_job_async(self, job: jobs_module.Job) -> jobs_module.Job:
        """
        Add a job in its queue for later processing by a worker.

        Parameters
        ----------
        job:
            The job to defer

        Returns
        -------
        :
            A copy of the job instance with the id set.
        """
        return (await self.batch_defer_jobs_async(jobs=[job]))[0]

    async def batch_defer_jobs_async(
        self, jobs: list[jobs_module.Job]
    ) -> list[jobs_module.Job]:
        """
        Add multiple jobs in their queue for later processing by a worker.

        Parameters
        ----------
        jobs:
            The jobs to defer

        Returns
        -------
        :
            A list of jobs with their id set.
        """
        # Make sure this code stays synchronized with .batch_defer_jobs()
        try:
            results = await self.connector.execute_query_all_async(
                **self._defer_jobs_query_kwargs(jobs=jobs)
            )
        except exceptions.UniqueViolation as exc:
            self._raise_already_enqueued(exc=exc, queueing_lock=exc.queueing_lock)

        return [
            job.evolve(id=results[index]["id"], status=jobs_module.Status.TODO.value)
            for index, job in enumerate(jobs)
        ]

    def defer_job(self, job: jobs_module.Job) -> jobs_module.Job:
        """
        Sync version of `defer_job_async`.
        """
        return self.batch_defer_jobs(jobs=[job])[0]

    def batch_defer_jobs(self, jobs: list[jobs_module.Job]) -> list[jobs_module.Job]:
        """
        Sync version of `batch_defer_jobs_async`.
        """
        try:
            results = self.connector.get_sync_connector().execute_query_all(
                **self._defer_jobs_query_kwargs(jobs=jobs)
            )
        except exceptions.UniqueViolation as exc:
            self._raise_already_enqueued(exc=exc, queueing_lock=exc.queueing_lock)

        return [
            job.evolve(id=results[index]["id"], status=jobs_module.Status.TODO.value)
            for index, job in enumerate(jobs)
        ]

    def _defer_jobs_query_kwargs(self, jobs: list[jobs_module.Job]) -> dict[str, Any]:
        return {
            "query": sql.queries["defer_jobs"],
            "jobs": [
                types.JobToDefer(
                    queue_name=job.queue,
                    task_name=job.task_name,
                    priority=job.priority,
                    lock=job.lock,
                    queueing_lock=job.queueing_lock,
                    args=job.task_kwargs,
                    scheduled_at=job.scheduled_at,
                )
                for job in jobs
            ],
        }

    def _raise_already_enqueued(
        self, exc: exceptions.UniqueViolation, queueing_lock: str | None
    ) -> NoReturn:
        if exc.constraint_name == QUEUEING_LOCK_CONSTRAINT:
            raise exceptions.AlreadyEnqueued(
                "Job cannot be enqueued: there is already a job in the queue "
                f"with the queueing lock {queueing_lock}"
            ) from exc
        raise exc

    async def defer_periodic_job(
        self,
        job: jobs_module.Job,
        periodic_id: str,
        defer_timestamp: int,
    ) -> int | None:
        """
        Defer a periodic job, ensuring that no other worker will defer a job for the
        same timestamp.

        If the job was deferred, return its id.
        If the job was not deferred, return None.
        """
        # If we mutate the existing task_kwargs dict, we could have unintended side
        # effects
        if job.task_kwargs.get("timestamp") != defer_timestamp:
            raise exceptions.InvalidTimestamp

        # schedule_at and schedule_in are meaningless in this context, we ignore them
        try:
            result = await self.connector.execute_query_one_async(
                query=sql.queries["defer_periodic_job"],
                task_name=job.task_name,
                defer_timestamp=defer_timestamp,
                periodic_id=periodic_id,
                queue=job.queue,
                priority=job.priority,
                lock=job.lock,
                queueing_lock=job.queueing_lock,
                args=job.task_kwargs,
            )
        except exceptions.UniqueViolation as exc:
            self._raise_already_enqueued(exc=exc, queueing_lock=job.queueing_lock)

        return result["id"]

    async def fetch_job(
        self, queues: Iterable[str] | None, worker_id: int
    ) -> jobs_module.Job | None:
        """
        Select a job in the queue, and mark it as doing.
        The worker selecting a job is then responsible for running it, and then
        to update the DB with the new status once it's done.

        Parameters
        ----------
        queues:
            Filter by job queue names

        Returns
        -------
        :
            None if no suitable job was found. The job otherwise.
        """

        row = await self.connector.execute_query_one_async(
            query=sql.queries["fetch_job"], queues=queues, worker_id=worker_id
        )

        # fetch_tasks will always return a row, but is there's no relevant
        # value, it will all be None
        if row["id"] is None:
            return None

        return jobs_module.Job.from_row(row)

    async def get_stalled_jobs(
        self,
        nb_seconds: int | None = None,
        queue: str | None = None,
        task_name: str | None = None,
        seconds_since_heartbeat: float = 30,
    ) -> Iterable[jobs_module.Job]:
        """
        Return all jobs that have been in ``doing`` state for more than a given time.

        Parameters
        ----------
        nb_seconds:
            If set then jobs that have been in ``doing`` state for longer than that time
            in seconds will be returned without considering stalled workers and heartbeats.
            This parameter is DEPRECATED and will be removed in a next major version.
            Use this method without this parameter instead to get stalled jobs based on
            stalled workers and heartbeats.
        queue:
            Filter by job queue name
        task_name:
            Filter by job task name
        seconds_since_heartbeat:
            Get stalled jobs based on workers that have not sent a heartbeat for longer
            than this time in seconds. Only used if ``nb_seconds`` is not set. Defaults
            to 30 seconds. When changing it then check also the ``update_heartbeat_interval``
            and ``stalled_worker_timeout`` parameters of the worker.
        """
        if nb_seconds is not None:
            warnings.warn(
                "The `nb_seconds` parameter is deprecated and will be removed in a next "
                "major version. Use the method without this parameter instead to get "
                "stalled jobs based on stalled workers and heartbeats.",
                DeprecationWarning,
                stacklevel=2,
            )
            rows = await self.connector.execute_query_all_async(
                query=sql.queries["select_stalled_jobs_by_started"],
                nb_seconds=nb_seconds,
                queue=queue,
                task_name=task_name,
            )
        else:
            rows = await self.connector.execute_query_all_async(
                query=sql.queries["select_stalled_jobs_by_heartbeat"],
                queue=queue,
                task_name=task_name,
                seconds_since_heartbeat=seconds_since_heartbeat,
            )

        return [jobs_module.Job.from_row(row) for row in rows]

    async def delete_old_jobs(
        self,
        nb_hours: int,
        queue: str | None = None,
        include_failed: bool | None = False,
        include_cancelled: bool | None = False,
        include_aborted: bool | None = False,
    ) -> None:
        """
        Delete jobs that have reached a final state (``succeeded``, ``failed``,
        ``cancelled``, or ``aborted``). By default, only considers jobs that have
        succeeded.

        Parameters
        ----------
        nb_hours:
            Consider jobs that been in a final state for more than ``nb_hours``
        queue:
            Filter by job queue name
        include_failed:
            If ``True``, also consider errored jobs. ``False`` by default
        include_cancelled:
            If ``True``, also consider cancelled jobs. ``False`` by default.
        include_aborted:
            If ``True``, also consider aborted jobs. ``False`` by default.
        """
        # We only consider finished jobs by default
        statuses = [jobs_module.Status.SUCCEEDED.value]
        if include_failed:
            statuses.append(jobs_module.Status.FAILED.value)
        if include_cancelled:
            statuses.append(jobs_module.Status.CANCELLED.value)
        if include_aborted:
            statuses.append(jobs_module.Status.ABORTED.value)

        await self.connector.execute_query_async(
            query=sql.queries["delete_old_jobs"],
            nb_hours=nb_hours,
            queue=queue,
            statuses=statuses,
        )

    async def finish_job(
        self,
        job: jobs_module.Job,
        status: jobs_module.Status,
        delete_job: bool,
    ) -> None:
        """
        Set a job to its final state (``succeeded``, ``failed`` or ``aborted``).

        Parameters
        ----------
        job:
        status:
            ``succeeded``, ``failed`` or ``aborted``
        """
        assert job.id  # TODO remove this
        await self.finish_job_by_id_async(
            job_id=job.id, status=status, delete_job=delete_job
        )

    async def finish_job_by_id_async(
        self,
        job_id: int,
        status: jobs_module.Status,
        delete_job: bool,
    ) -> None:
        await self.connector.execute_query_async(
            query=sql.queries["finish_job"],
            job_id=job_id,
            status=status.value,
            delete_job=delete_job,
        )

    def cancel_job_by_id(
        self, job_id: int, abort: bool = False, delete_job: bool = False
    ) -> bool:
        """
        Cancel a job by id.

        Parameters
        ----------
        job_id:
            The id of the job to cancel
        abort:
            If True, a job will be marked for abortion, but the task itself has to
            respect the abortion request. If False, only jobs in ``todo`` state will
            be set to ``cancelled`` and won't be processed by a worker anymore.
        delete_job:
            If True, the job will be deleted from the database after being cancelled. Does
            not affect the jobs that should be aborted.

        Returns
        -------
        :
            If True, the job was cancelled (or its abortion was requested). If False,
            nothing was done: either there is no job with this id or it's not in a state
            where it may be cancelled (i.e. `todo` or `doing`)
        """
        result = self.connector.get_sync_connector().execute_query_one(
            query=sql.queries["cancel_job"],
            job_id=job_id,
            abort=abort,
            delete_job=delete_job,
        )

        if result["id"] is None:
            return False

        assert result["id"] == job_id
        return True

    async def cancel_job_by_id_async(
        self, job_id: int, abort: bool = False, delete_job: bool = False
    ) -> bool:
        """
        Cancel a job by id.

        Parameters
        ----------
        job_id:
            The id of the job to cancel
        abort:
            If True, a job will be marked for abortion, but the task itself has to
            respect the abortion request. If False, only jobs in ``todo`` state will
            be set to ``cancelled`` and won't be processed by a worker anymore.
        delete_job:
            If True, the job will be deleted from the database after being cancelled. Does
            not affect the jobs that should be aborted.

        Returns
        -------
        :
            If True, the job was cancelled (or its abortion was requested). If False,
            nothing was done: either there is no job with this id or it's not in a state
            where it may be cancelled (i.e. `todo` or `doing`)
        """
        result = await self.connector.execute_query_one_async(
            query=sql.queries["cancel_job"],
            job_id=job_id,
            abort=abort,
            delete_job=delete_job,
        )

        if result["id"] is None:
            return False

        assert result["id"] == job_id
        return True

    def get_job_status(self, job_id: int) -> jobs_module.Status:
        """
        Get the status of a job by id.

        Parameters
        ----------
        job_id:
            The id of the job to get the status of

        Returns
        -------
        :
        """
        result = self.connector.get_sync_connector().execute_query_one(
            query=sql.queries["get_job_status"], job_id=job_id
        )
        return jobs_module.Status(result["status"])

    async def get_job_status_async(self, job_id: int) -> jobs_module.Status:
        """
        Get the status of a job by id.

        Parameters
        ----------
        job_id:
            The id of the job to get the status of

        Returns
        -------
        :
        """
        result = await self.connector.execute_query_one_async(
            query=sql.queries["get_job_status"], job_id=job_id
        )
        return jobs_module.Status(result["status"])

    async def retry_job(
        self,
        job: jobs_module.Job,
        retry_at: datetime.datetime | None = None,
        priority: int | None = None,
        queue: str | None = None,
        lock: str | None = None,
    ) -> None:
        """
        Indicates that a job should be retried later.

        Parameters
        ----------
        job:
        retry_at:
            If set at present time or in the past, the job may be retried immediately.
            Otherwise, the job will be retried no sooner than this date & time.
            Should be timezone-aware (even if UTC). Defaults to present time.
        priority:
            If set, the job will be retried with this priority. If not set, the priority
            remains unchanged.
        queue:
            If set, the job will be retried on this queue. If not set, the queue remains
            unchanged.
        lock:
            If set, the job will be retried with this lock. If not set, the lock remains
            unchanged.
        """
        assert job.id  # TODO remove this
        await self.retry_job_by_id_async(
            job_id=job.id,
            retry_at=retry_at or utils.utcnow(),
            priority=priority,
            queue=queue,
            lock=lock,
        )

    async def retry_job_by_id_async(
        self,
        job_id: int,
        retry_at: datetime.datetime,
        priority: int | None = None,
        queue: str | None = None,
        lock: str | None = None,
    ) -> None:
        """
        Indicates that a job should be retried later.

        Parameters
        ----------
        job_id:
        retry_at:
            If set at present time or in the past, the job may be retried immediately.
            Otherwise, the job will be retried no sooner than this date & time.
            Should be timezone-aware (even if UTC).
        priority:
            If set, the job will be retried with this priority. If not set, the priority
            remains unchanged.
        queue:
            If set, the job will be retried on this queue. If not set, the queue remains
            unchanged.
        lock:
            If set, the job will be retried with this lock. If not set, the lock remains
            unchanged.
        """
        await self.connector.execute_query_async(
            query=sql.queries["retry_job"],
            job_id=job_id,
            retry_at=retry_at,
            new_priority=priority,
            new_queue_name=queue,
            new_lock=lock,
        )

    def retry_job_by_id(
        self,
        job_id: int,
        retry_at: datetime.datetime,
        priority: int | None = None,
        queue: str | None = None,
        lock: str | None = None,
    ) -> None:
        """
        Sync version of `retry_job_by_id_async`.
        """
        self.connector.get_sync_connector().execute_query(
            query=sql.queries["retry_job"],
            job_id=job_id,
            retry_at=retry_at,
            new_priority=priority,
            new_queue_name=queue,
            new_lock=lock,
        )

    async def listen_for_jobs(
        self,
        *,
        on_notification: NotificationCallback,
        queues: Iterable[str] | None = None,
    ) -> None:
        """
        Listens to job notifications from the database, and invokes the callback each time an
        notification is received.

        This coroutine either returns ``None`` upon calling if it cannot start
        listening or does not return and needs to be cancelled to end.

        Parameters
        ----------
        on_notification : ``connector.Notify``
            A coroutine that will be called and awaited every time a notification is received
        queues : ``Optional[Iterable[str]]``
            If ``None``, all notification will be considered. If an iterable of
            queue names is passed, only defer operations on those queues will be
            considered. Defaults to ``None``
        """

        async def handle_notification(channel: str, payload: str):
            notification: jobs_module.Notification = json.loads(payload)
            logger.debug(
                f"Received {notification['type']} notification from channel",
                extra={channel: channel, payload: payload},
            )
            await on_notification(channel=channel, notification=notification)

        await self.connector.listen_notify(
            on_notification=handle_notification,
            channels=get_channel_for_queues(queues=queues),
        )

    async def check_connection_async(self) -> bool:
        """
        Dummy query, check that the main Procrastinate SQL table exists.
        Raises if there's a connection problem.

        Returns
        -------
        :
            ``True`` if the table exists, ``False`` otherwise.
        """
        result = await self.connector.execute_query_one_async(
            query=sql.queries["check_connection"],
        )
        return result["check"] is not None

    def check_connection(self) -> bool:
        """
        Sync version of `check_connection_async`.
        """
        result = self.connector.get_sync_connector().execute_query_one(
            query=sql.queries["check_connection"],
        )
        return result["check"] is not None

    async def list_jobs_async(
        self,
        id: int | None = None,
        queue: str | None = None,
        task: str | None = None,
        status: str | None = None,
        lock: str | None = None,
        queueing_lock: str | None = None,
        worker_id: int | None = None,
    ) -> Iterable[jobs_module.Job]:
        """
        List all procrastinate jobs given query filters.

        Parameters
        ----------
        id:
            Filter by job ID
        queue:
            Filter by job queue name
        task:
            Filter by job task name
        status:
            Filter by job status (``todo``/``doing``/``succeeded``/``failed``)
        lock:
            Filter by job lock
        queueing_lock:
            Filter by job queueing_lock
        worker_id:
            Filter by worker ID

        Returns
        -------
        :
        """
        rows = await self.connector.execute_query_all_async(
            query=sql.queries["list_jobs"],
            id=id,
            queue_name=queue,
            task_name=task,
            status=status,
            lock=lock,
            queueing_lock=queueing_lock,
            worker_id=worker_id,
        )
        return [jobs_module.Job.from_row(row) for row in rows]

    def list_jobs(
        self,
        id: int | None = None,
        queue: str | None = None,
        task: str | None = None,
        status: str | None = None,
        lock: str | None = None,
        queueing_lock: str | None = None,
        worker_id: int | None = None,
    ) -> list[jobs_module.Job]:
        """
        Sync version of `list_jobs_async`
        """
        rows = self.connector.get_sync_connector().execute_query_all(
            query=sql.queries["list_jobs"],
            id=id,
            queue_name=queue,
            task_name=task,
            status=status,
            lock=lock,
            queueing_lock=queueing_lock,
            worker_id=worker_id,
        )
        return [jobs_module.Job.from_row(row) for row in rows]

    async def list_queues_async(
        self,
        queue: str | None = None,
        task: str | None = None,
        status: str | None = None,
        lock: str | None = None,
    ) -> Iterable[dict[str, Any]]:
        """
        List all queues and number of jobs per status for each queue.

        Parameters
        ----------
        queue:
            Filter by job queue name
        task:
            Filter by job task name
        status:
            Filter by job status (``todo``/``doing``/``succeeded``/``failed``)
        lock:
            Filter by job lock

        Returns
        -------
        :
            A list of dictionaries representing queues stats (``name``, ``jobs_count``,
            ``todo``, ``doing``, ``succeeded``, ``failed``, ``cancelled``, ``aborted``).
        """
        return [
            {
                "name": row["name"],
                "jobs_count": row["jobs_count"],
                "todo": row["stats"].get("todo", 0),
                "doing": row["stats"].get("doing", 0),
                "succeeded": row["stats"].get("succeeded", 0),
                "failed": row["stats"].get("failed", 0),
                "cancelled": row["stats"].get("cancelled", 0),
                "aborted": row["stats"].get("aborted", 0),
            }
            for row in await self.connector.execute_query_all_async(
                query=sql.queries["list_queues"],
                queue_name=queue,
                task_name=task,
                status=status,
                lock=lock,
            )
        ]

    def list_queues(
        self,
        queue: str | None = None,
        task: str | None = None,
        status: str | None = None,
        lock: str | None = None,
    ) -> Iterable[dict[str, Any]]:
        """
        Sync version of `list_queues_async`
        """
        return [
            {
                "name": row["name"],
                "jobs_count": row["jobs_count"],
                "todo": row["stats"].get("todo", 0),
                "doing": row["stats"].get("doing", 0),
                "succeeded": row["stats"].get("succeeded", 0),
                "failed": row["stats"].get("failed", 0),
                "cancelled": row["stats"].get("cancelled", 0),
                "aborted": row["stats"].get("aborted", 0),
            }
            for row in self.connector.get_sync_connector().execute_query_all(
                query=sql.queries["list_queues"],
                queue_name=queue,
                task_name=task,
                status=status,
                lock=lock,
            )
        ]

    async def list_tasks_async(
        self,
        queue: str | None = None,
        task: str | None = None,
        status: str | None = None,
        lock: str | None = None,
    ) -> Iterable[dict[str, Any]]:
        """
        List all tasks and number of jobs per status for each task.

        Parameters
        ----------
        queue:
            Filter by job queue name
        task:
            Filter by job task name
        status:
            Filter by job status (``todo``/``doing``/``succeeded``/``failed``)
        lock:
            Filter by job lock

        Returns
        -------
        :
            A list of dictionaries representing tasks stats (``name``, ``jobs_count``,
            ``todo``, ``doing``, ``succeeded``, ``failed``, ``cancelled``, ``aborted``).
        """
        return [
            {
                "name": row["name"],
                "jobs_count": row["jobs_count"],
                "todo": row["stats"].get("todo", 0),
                "doing": row["stats"].get("doing", 0),
                "succeeded": row["stats"].get("succeeded", 0),
                "failed": row["stats"].get("failed", 0),
                "cancelled": row["stats"].get("cancelled", 0),
                "aborted": row["stats"].get("aborted", 0),
            }
            for row in await self.connector.execute_query_all_async(
                query=sql.queries["list_tasks"],
                queue_name=queue,
                task_name=task,
                status=status,
                lock=lock,
            )
        ]

    def list_tasks(
        self,
        queue: str | None = None,
        task: str | None = None,
        status: str | None = None,
        lock: str | None = None,
    ) -> Iterable[dict[str, Any]]:
        """
        Sync version of `list_queues`
        """
        return [
            {
                "name": row["name"],
                "jobs_count": row["jobs_count"],
                "todo": row["stats"].get("todo", 0),
                "doing": row["stats"].get("doing", 0),
                "succeeded": row["stats"].get("succeeded", 0),
                "failed": row["stats"].get("failed", 0),
                "cancelled": row["stats"].get("cancelled", 0),
                "aborted": row["stats"].get("aborted", 0),
            }
            for row in self.connector.get_sync_connector().execute_query_all(
                query=sql.queries["list_tasks"],
                queue_name=queue,
                task_name=task,
                status=status,
                lock=lock,
            )
        ]

    async def list_locks_async(
        self,
        queue: str | None = None,
        task: str | None = None,
        status: str | None = None,
        lock: str | None = None,
    ) -> Iterable[dict[str, Any]]:
        """
        List all locks and number of jobs per lock for each lock value.

        Parameters
        ----------
        queue:
            Filter by job queue name
        task:
            Filter by job task name
        status:
            Filter by job status (``todo``/``doing``/``succeeded``/``failed``)
        lock:
            Filter by job lock

        Returns
        -------
        :
            A list of dictionaries representing locks stats (``name``, ``jobs_count``,
            ``todo``, ``doing``, ``succeeded``, ``failed``, ``cancelled``, ``aborted``).
        """
        result = []
        for row in await self.connector.execute_query_all_async(
            query=sql.queries["list_locks"],
            queue_name=queue,
            task_name=task,
            status=status,
            lock=lock,
        ):
            result.append(
                {
                    "name": row["name"],
                    "jobs_count": row["jobs_count"],
                    "todo": row["stats"].get("todo", 0),
                    "doing": row["stats"].get("doing", 0),
                    "succeeded": row["stats"].get("succeeded", 0),
                    "failed": row["stats"].get("failed", 0),
                    "cancelled": row["stats"].get("cancelled", 0),
                    "aborted": row["stats"].get("aborted", 0),
                }
            )
        return result

    def list_locks(
        self,
        queue: str | None = None,
        task: str | None = None,
        status: str | None = None,
        lock: str | None = None,
    ) -> Iterable[dict[str, Any]]:
        """
        Sync version of `list_queues`
        """
        result = []
        for row in self.connector.get_sync_connector().execute_query_all(
            query=sql.queries["list_locks"],
            queue_name=queue,
            task_name=task,
            status=status,
            lock=lock,
        ):
            result.append(
                {
                    "name": row["name"],
                    "jobs_count": row["jobs_count"],
                    "todo": row["stats"].get("todo", 0),
                    "doing": row["stats"].get("doing", 0),
                    "succeeded": row["stats"].get("succeeded", 0),
                    "failed": row["stats"].get("failed", 0),
                    "cancelled": row["stats"].get("cancelled", 0),
                    "aborted": row["stats"].get("aborted", 0),
                }
            )
        return result

    async def list_jobs_to_abort_async(self, queue: str | None = None) -> Iterable[int]:
        """
        List ids of running jobs to abort
        """
        rows = await self.connector.execute_query_all_async(
            query=sql.queries["list_jobs_to_abort"], queue_name=queue
        )
        return [row["id"] for row in rows]

    async def register_worker(self) -> int:
        """
        Register a newly started worker (with a initial heartbeat) in the database.

        Returns
        -------
        :
            The ID of the registered worker
        """
        result = await self.connector.execute_query_one_async(
            query=sql.queries["register_worker"],
        )
        return result["worker_id"]

    async def unregister_worker(self, worker_id: int) -> None:
        """
        Unregister a shut down worker and also delete its heartbeat from the database.

        Parameters
        ----------
        worker_id:
            The ID of the worker to delete
        """
        await self.connector.execute_query_async(
            query=sql.queries["unregister_worker"],
            worker_id=worker_id,
        )

    async def update_heartbeat(self, worker_id: int) -> None:
        """
        Update the heartbeat of a worker.

        Parameters
        ----------
        worker_id:
            The ID of the worker to update the heartbeat
        """
        await self.connector.execute_query_async(
            query=sql.queries["update_heartbeat"],
            worker_id=worker_id,
        )

    async def prune_stalled_workers(self, seconds_since_heartbeat: float) -> list[int]:
        """
        Delete the workers that have not sent a heartbeat for more than a given time.

        Parameters
        ----------
        seconds_since_heartbeat:
            Only workers that have not sent a heartbeat for longer than this will be
            deleted

        Returns
        -------
        :
            A list of worker IDs that have been deleted
        """
        rows = await self.connector.execute_query_all_async(
            query=sql.queries["prune_stalled_workers"],
            seconds_since_heartbeat=seconds_since_heartbeat,
        )
        return [row["worker_id"] for row in rows]
