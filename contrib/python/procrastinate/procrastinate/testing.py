from __future__ import annotations

import asyncio
import datetime
import json
import threading
from collections import Counter
from collections.abc import Coroutine, Iterable, Iterator
from itertools import count
from typing import Any, Literal

from procrastinate import (
    connector,
    exceptions,
    jobs,
    manager,
    schema,
    sql,
    types,
    utils,
)

JobRow = dict[str, Any]
EventRow = dict[str, Any]


class InMemoryConnector(connector.BaseAsyncConnector):
    """
    An InMemoryConnector may be used for testing only. Tasks are not
    persisted and will be lost when the process ends.

    While implementing the Connector interface, it also adds a few
    methods and attributes to ease testing.
    """

    def __init__(self):
        self.reset()
        self.reverse_queries = {value: key for key, value in sql.queries.items()}
        self.reverse_queries[schema.SchemaManager.get_schema()] = "apply_schema"
        #: Mapping of ``{<job id>: <Job database row as a dictionary>}``
        self.jobs: dict[int, JobRow] = {}
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_thread_id: int | None = None

    def reset(self) -> None:
        """
        Removes anything the in-memory pseudo-database contains, to ensure test
        independence.
        """
        self.jobs = {}
        self.events: dict[int, list[EventRow]] = {}
        self.workers: dict[int, datetime.datetime] = {}
        self.job_counter = count(1)
        self.queries: list[tuple[str, dict[str, Any]]] = []
        self.on_notification: connector.Notify | None = None
        self.notify_channels: list[str] = []
        self.periodic_defers: dict[tuple[str, str], int] = {}
        self.table_exists = True
        self.states: list[str] = []

    def get_sync_connector(self) -> connector.BaseConnector:
        return self

    async def generic_execute(
        self, query: str, suffix: Literal["run", "one", "all"], **arguments: Any
    ) -> Any:
        """
        Calling a query will call the <query_name>_<suffix> method
        on this class. Suffix is "run" if no result is expected,
        "one" if a single result, and "all" if multiple results.
        """
        query_name = self.reverse_queries[query]
        self.queries.append((query_name, arguments))
        return await getattr(self, f"{query_name}_{suffix}")(**arguments)

    def make_dynamic_query(self, query: str, **identifiers: str) -> str:
        return query.format(**identifiers)

    async def execute_query_async(self, query: str, **arguments: Any) -> None:
        await self.generic_execute(query, "run", **arguments)

    async def execute_query_one_async(
        self, query: str, **arguments: Any
    ) -> dict[str, Any]:
        return await self.generic_execute(query, "one", **arguments)

    async def execute_query_all_async(
        self, query: str, **arguments: Any
    ) -> list[dict[str, Any]]:
        return await self.generic_execute(query, "all", **arguments)

    async def listen_notify(
        self, on_notification: connector.Notify, channels: Iterable[str]
    ) -> None:
        self.on_notification = on_notification
        self.notify_channels = list(channels)

    def open(self, pool: connector.Pool | None = None) -> None:
        self.states.append("open")

    async def open_async(self, pool: connector.Pool | None = None) -> None:
        """
        Save the current event loop and its thread id so that later notifications
        can be scheduled on this loop.
        """
        self._loop = asyncio.get_running_loop()
        self._loop_thread_id = threading.get_ident()
        self.states.append("open_async")

    def close(self) -> None:
        self.states.append("closed")

    async def close_async(self) -> None:
        self.states.append("closed_async")

    # End of BaseConnector methods

    async def defer_jobs_all(self, jobs: list[types.JobToDefer]) -> list[JobRow]:
        # We check the queueing locks upfront so that no job is inserted into
        # the queue if the constraint is violated (simulating a database
        # rollback).
        new_queueing_locks = [
            job.queueing_lock for job in jobs if job.queueing_lock is not None
        ]

        counts = Counter(new_queueing_locks)
        duplicate = next(
            (lock for lock in new_queueing_locks if counts[lock] > 1), None
        )

        if duplicate is None:
            current_queueing_locks = {
                job["queueing_lock"]
                for job in self.jobs.values()
                if job["status"] == "todo"
            } - {None}

            duplicate = next(
                (lock for lock in new_queueing_locks if lock in current_queueing_locks),
                None,
            )

        if duplicate is not None:
            raise exceptions.UniqueViolation(
                constraint_name=manager.QUEUEING_LOCK_CONSTRAINT,
                queueing_lock=duplicate,
            )

        job_rows = []
        for job in jobs:
            id = next(self.job_counter)

            self.jobs[id] = job_row = {
                "id": id,
                "queue_name": job.queue_name,
                "task_name": job.task_name,
                "priority": job.priority,
                "lock": job.lock,
                "queueing_lock": job.queueing_lock,
                "args": job.args,
                "status": "todo",
                "scheduled_at": job.scheduled_at,
                "attempts": 0,
                "abort_requested": False,
                "worker_id": None,
            }
            self.events[id] = []
            if job.scheduled_at:
                self.events[id].append({"type": "scheduled", "at": job.scheduled_at})
            self.events[id].append({"type": "deferred", "at": utils.utcnow()})

            await self._notify(
                job.queue_name,
                {
                    "type": "job_inserted",
                    "job_id": id,
                },
            )
            job_rows.append(job_row)

        return job_rows

    async def defer_periodic_job_one(
        self,
        queue: str,
        task_name: str,
        priority: int,
        args: types.JSONDict,
        defer_timestamp: int,
        lock: str | None,
        queueing_lock: str | None,
        periodic_id: str,
    ) -> JobRow:
        # If the periodic task has already been deferred for this timestamp
        if self.periodic_defers.get((task_name, periodic_id)) == defer_timestamp:
            return {"id": None}

        self.periodic_defers[(task_name, periodic_id)] = defer_timestamp
        job_rows = await self.defer_jobs_all(
            [
                types.JobToDefer(
                    queue_name=queue,
                    task_name=task_name,
                    priority=priority,
                    lock=lock,
                    queueing_lock=queueing_lock,
                    args=args,
                    scheduled_at=None,
                )
            ]
        )
        return job_rows[0]

    @property
    def current_locks(self) -> Iterable[str]:
        return {
            job["lock"] for job in self.jobs.values() if job["status"] == "doing"
        } - {None}

    @property
    def finished_jobs(self) -> list[JobRow]:
        return [
            job
            for job in self.jobs.values()
            if job["status"] in {"failed", "succeeded"}
        ]

    async def _notify(self, queue_name: str, notification: jobs.Notification) -> None:
        """
        Instead of directly awaiting on_notification, we check the current thread.
        If we are not on the same thread as the one where the loop was saved,
        we schedule the notification on the correct loop.
        """
        if not self.on_notification:
            return

        destination_channels = {
            "procrastinate_any_queue_v1",
            f"procrastinate_queue_v1#{queue_name}",
        }
        for channel in set(self.notify_channels).intersection(destination_channels):
            coro = self.on_notification(
                channel=channel, payload=json.dumps(notification)
            )
            if threading.get_ident() == self._loop_thread_id:
                # Already on the right thread: just await.
                await coro
            else:
                # Not on the correct thread: schedule the coroutine on the saved loop.

                # run_coroutine_threadsafe needs a coroutine, but coro may be any
                # awaitable (coroutines are awaitable but the opposite is not true).
                # Consequently, we ensure that coro is a coroutine.
                if not isinstance(coro, Coroutine):
                    original_coro = coro

                    async def _coro() -> None:
                        return await original_coro

                    coro: Any = _coro()
                assert self._loop
                future = asyncio.run_coroutine_threadsafe(coro, self._loop)
                # Wrap the concurrent.futures.Future so we can await it.
                await asyncio.wrap_future(future)

    async def fetch_job_one(
        self, queues: Iterable[str] | None, worker_id: int
    ) -> dict[str, Any]:
        assert worker_id in self.workers, f"Worker {worker_id} not found"

        filtered_jobs = [
            job
            for job in self.jobs.values()
            if (
                job["status"] == "todo"
                and (queues is None or job["queue_name"] in queues)
                and (not job["scheduled_at"] or job["scheduled_at"] <= utils.utcnow())
                and job["lock"] not in self.current_locks
            )
        ]

        filtered_jobs.sort(key=lambda job: (-job["priority"], job["id"]))

        if not filtered_jobs:
            return {"id": None}

        job = filtered_jobs[0]
        job["status"] = "doing"
        job["worker_id"] = worker_id
        self.events[job["id"]].append({"type": "started", "at": utils.utcnow()})
        return job

    async def finish_job_run(self, job_id: int, status: str, delete_job: bool) -> None:
        if delete_job:
            self.jobs.pop(job_id)
            return

        job_row = self.jobs[job_id]
        job_row["status"] = status
        job_row["attempts"] += 1
        job_row["abort_requested"] = False
        self.events[job_id].append({"type": status, "at": utils.utcnow()})

    async def cancel_job_one(
        self, job_id: int, abort: bool, delete_job: bool
    ) -> dict[str, Any]:
        job_row = self.jobs[job_id]

        if job_row["status"] == "todo":
            if delete_job:
                self.jobs.pop(job_id)
                return {"id": job_id}

            job_row["status"] = "cancelled"
            return {"id": job_id}

        if abort:
            job_row["abort_requested"] = True
            await self._notify(
                job_row["queue_name"],
                {
                    "type": "abort_job_requested",
                    "job_id": job_id,
                },
            )

            return {"id": job_id}

        return {"id": None}

    async def get_job_status_one(self, job_id: int) -> dict[str, Any]:
        return {"status": self.jobs[job_id]["status"]}

    async def retry_job_run(
        self,
        job_id: int,
        retry_at: datetime.datetime,
        new_priority: int | None = None,
        new_queue_name: str | None = None,
        new_lock: str | None = None,
    ) -> None:
        job_row = self.jobs[job_id]
        job_row["status"] = "todo"
        job_row["attempts"] += 1
        job_row["scheduled_at"] = retry_at
        if new_priority is not None:
            job_row["priority"] = new_priority
        if new_queue_name is not None:
            job_row["queue_name"] = new_queue_name
        if new_lock is not None:
            job_row["lock"] = new_lock
        self.events[job_id].append({"type": "scheduled", "at": retry_at})
        self.events[job_id].append({"type": "deferred_for_retry", "at": utils.utcnow()})

    async def select_stalled_jobs_by_started_all(
        self, nb_seconds: int, queue: str, task_name: str
    ) -> Iterator[JobRow]:
        return (
            job
            for job in self.jobs.values()
            if job["status"] == "doing"
            and self.events[job["id"]][-1]["at"]
            < utils.utcnow() - datetime.timedelta(seconds=nb_seconds)
            and queue in (job["queue_name"], None)
            and task_name in (job["task_name"], None)
        )

    async def select_stalled_jobs_by_heartbeat_all(
        self, queue: str, task_name: str, seconds_since_heartbeat: int
    ):
        return (
            job
            for job in self.jobs.values()
            if job["status"] == "doing"
            and queue in (job["queue_name"], None)
            and task_name in (job["task_name"], None)
            and (
                self.workers.get(
                    job["worker_id"],
                    datetime.datetime.min.replace(tzinfo=datetime.timezone.utc),
                )
                < utils.utcnow() - datetime.timedelta(seconds=seconds_since_heartbeat)
            )
        )

    async def delete_old_jobs_run(
        self, nb_hours: int, queue: str, statuses: Iterable[str]
    ):
        for id, job in list(self.jobs.items()):
            if (
                job["status"] in statuses
                and (
                    max(e["at"] for e in self.events[id])
                    < utils.utcnow() - datetime.timedelta(hours=nb_hours)
                )
                and queue in (job["queue_name"], None)
            ):
                self.jobs.pop(id)

    async def listen_for_jobs_run(self) -> None:
        pass

    async def apply_schema_run(self) -> None:
        pass

    async def list_jobs_all(self, **kwargs: Any):
        jobs: list[JobRow] = []
        for job in self.jobs.values():
            if all(
                expected is None or str(job[key]) == str(expected)
                for key, expected in kwargs.items()
            ):
                jobs.append(job)
        return iter(jobs)

    async def list_queues_all(self, **kwargs: Any):
        result: list[dict[str, Any]] = []
        jobs = list(await self.list_jobs_all(**kwargs))
        queues = sorted({job["queue_name"] for job in jobs})
        for queue in queues:
            queue_jobs = [job for job in jobs if job["queue_name"] == queue]
            stats = Counter(job["status"] for job in queue_jobs)
            result.append(
                {"name": queue, "jobs_count": len(queue_jobs), "stats": stats}
            )
        return iter(result)

    async def list_tasks_all(self, **kwargs: Any):
        result: list[dict[str, Any]] = []
        jobs = list(await self.list_jobs_all(**kwargs))
        tasks = sorted({job["task_name"] for job in jobs})
        for task in tasks:
            task_jobs = [job for job in jobs if job["task_name"] == task]
            stats = Counter(job["status"] for job in task_jobs)
            result.append({"name": task, "jobs_count": len(task_jobs), "stats": stats})
        return result

    async def list_locks_all(self, **kwargs: Any):
        result: list[dict[str, Any]] = []
        jobs = list(await self.list_jobs_all(**kwargs))
        locks = sorted({job["lock"] for job in jobs})
        for lock in locks:
            lock_jobs = [job for job in jobs if job["lock"] == lock]
            stats = Counter(job["status"] for job in lock_jobs)
            result.append({"name": lock, "jobs_count": len(lock_jobs), "stats": stats})
        return result

    async def list_jobs_to_abort_all(self, queue_name: str | None):
        return list(
            await self.list_jobs_all(
                status="doing", abort_requested=True, queue_name=queue_name
            )
        )

    async def set_job_status_run(self, id: int, status: str):
        id = int(id)
        self.jobs[id]["status"] = status

    async def check_connection_one(self):
        return {"check": self.table_exists or None}

    async def register_worker_one(self):
        worker_id = max(self.workers, default=0) + 1
        self.workers[worker_id] = utils.utcnow()
        return {"worker_id": worker_id}

    async def unregister_worker_run(self, worker_id: int):
        self.workers.pop(worker_id)
        for job in self.jobs.values():
            if job["worker_id"] == worker_id:
                job["worker_id"] = None

    async def update_heartbeat_run(self, worker_id: int):
        self.workers[worker_id] = utils.utcnow()

    async def prune_stalled_workers_all(self, seconds_since_heartbeat: int):
        pruned_workers = []
        for worker_id, heartbeat in list(self.workers.items()):
            if heartbeat < utils.utcnow() - datetime.timedelta(
                seconds=seconds_since_heartbeat
            ):
                self.workers.pop(worker_id)
                pruned_workers.append({"worker_id": worker_id})

        for job in self.jobs.values():
            if job["worker_id"] not in self.workers:
                job["worker_id"] = None

        return pruned_workers
