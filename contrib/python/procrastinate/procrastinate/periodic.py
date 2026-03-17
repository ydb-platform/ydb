from __future__ import annotations

import asyncio
import functools
import logging
import time
from collections.abc import Callable, Iterable
from typing import Concatenate, Generic, cast

import attr
import croniter
from typing_extensions import ParamSpec, TypeVar, Unpack

from procrastinate import exceptions, tasks

P = ParamSpec("P")
R = TypeVar("R")
Args = ParamSpec("Args")

# The maximum delay after which tasks will be considered as
# outdated, and ignored.
MAX_DELAY = 60 * 10  # 10 minutes
# We'll always be oversleeping by this amount to avoid waking up too early for our
# tasks. This is, of course, the most important part of procrastinate ;)
MARGIN = 0.5  # seconds

logger = logging.getLogger(__name__)

cached_property = functools.cached_property


@attr.dataclass(frozen=True)
class PeriodicTask(Generic[P, R, Args]):
    task: tasks.Task[P, R, Args]
    cron: str
    periodic_id: str
    configure_kwargs: tasks.ConfigureTaskOptions

    @cached_property
    def croniter(self) -> croniter.croniter:
        return croniter.croniter(self.cron)


TaskAtTime = tuple[PeriodicTask, int]


class PeriodicRegistry:
    def __init__(self):
        self.periodic_tasks: dict[tuple[str, str], PeriodicTask] = {}

    def periodic_decorator(
        self,
        cron: str,
        periodic_id: str,
        **configure_kwargs: Unpack[tasks.ConfigureTaskOptions],
    ) -> Callable[[tasks.Task[P, R, Concatenate[int, Args]]], tasks.Task[P, R, Args]]:
        """
        Decorator over a task definition that registers that task for periodic
        launch. This decorator should not be used directly, ``@app.periodic()`` is meant
        to be used instead.
        """

        def wrapper(
            task: tasks.Task[P, R, Concatenate[int, Args]],
        ) -> tasks.Task[P, R, Args]:
            self.register_task(
                task=task,
                cron=cron,
                periodic_id=periodic_id,
                configure_kwargs=configure_kwargs,
            )
            return cast(tasks.Task[P, R, Args], task)

        return wrapper

    def register_task(
        self,
        task: tasks.Task[P, R, Concatenate[int, Args]],
        cron: str,
        periodic_id: str,
        configure_kwargs: tasks.ConfigureTaskOptions,
    ) -> PeriodicTask[P, R, Concatenate[int, Args]]:
        key = (task.name, periodic_id)
        if key in self.periodic_tasks:
            raise exceptions.TaskAlreadyRegistered(
                "A periodic task was already registed with the same periodic_id "
                f"({periodic_id!r}). Please use a different periodic_id for multiple "
                "schedules of the same task."
            )

        logger.info(
            f"Registering task {task.name} with periodic id {periodic_id!r} to run "
            f"periodically with cron {cron}",
            extra={
                "action": "registering_periodic_task",
                "task": task.name,
                "cron": cron,
                "periodic_id": periodic_id,
                "kwargs": str(configure_kwargs),
            },
        )

        self.periodic_tasks[key] = periodic_task = PeriodicTask(
            task=task,
            cron=cron,
            periodic_id=periodic_id,
            configure_kwargs=configure_kwargs,
        )
        return periodic_task


class PeriodicDeferrer:
    def __init__(
        self,
        registry: PeriodicRegistry,
        max_delay: float = MAX_DELAY,
    ):
        # {(task_name, periodic_id): defer_timestamp}
        self.last_defers: dict[tuple[str, str], int] = {}
        self.max_delay = max_delay
        self.registry = registry

    async def worker(self) -> None:
        """
        High-level command for the periodic deferrer. Launches the loop.
        """
        if not self.registry.periodic_tasks:
            logger.info(
                "No periodic task found, periodic deferrer will not run.",
                extra={"action": "periodic_deferrer_no_task"},
            )
            return

        while True:
            now = time.time()
            await self.defer_jobs(jobs_to_defer=self.get_previous_tasks(at=now))
            await self.wait(next_tick=self.get_next_tick(at=now))

    def get_next_tick(self, at: float):
        """
        Return the number of seconds to wait before the next periodic task needs to be
        deferred.
        If now is not passed, the current timestamp is used.
        """
        next_timestamp = min(
            pt.croniter.get_next(ret_type=float, start_time=at)  # type: ignore
            for pt in self.registry.periodic_tasks.values()
        )
        return next_timestamp - at

    def get_previous_tasks(self, at: float) -> Iterable[TaskAtTime]:
        """
        Return each periodic task that may not have been deferred yet, alongside with
        its scheduled time.
        Tasks that should have been deferred more than self.max_delay seconds ago are
        ignored.
        """
        for key, periodic_task in self.registry.periodic_tasks.items():
            for timestamp in self.get_timestamps(
                periodic_task=periodic_task,
                since=self.last_defers.get(key),
                until=at,
            ):
                self.last_defers[key] = timestamp
                yield periodic_task, timestamp

    def get_timestamps(
        self, periodic_task: PeriodicTask, since: int | None, until: float
    ) -> Iterable[int]:
        cron_iterator = periodic_task.croniter
        if since:
            timestamp = cron_iterator.set_current(start_time=since)
            while True:
                timestamp = round(cron_iterator.get_next(ret_type=float))
                if timestamp > until:
                    return
                yield int(timestamp)

        else:
            cron_iterator.set_current(start_time=until)
            timestamp = round(cron_iterator.get_prev(ret_type=float))
            delay = until - timestamp

            if delay > self.max_delay:
                logger.debug(
                    f"Ignoring periodic task {periodic_task.task.name} scheduled "
                    f"more than {self.max_delay} s ago ({delay:.0f} s)",
                    extra={
                        "task": periodic_task.task.name,
                        "periodic_id": periodic_task.periodic_id,
                        "action": "ignore_periodic_task",
                        "max_delay": self.max_delay,
                        "delay": delay,
                    },
                )
                return

            yield int(timestamp)

    async def defer_jobs(self, jobs_to_defer: Iterable[TaskAtTime]) -> None:
        """
        Try deferring all tasks that might need deferring. The database will keep us
        from deferring the same task for the same scheduled time multiple times.
        """
        for periodic_task, timestamp in jobs_to_defer:
            task = periodic_task.task
            periodic_id = periodic_task.periodic_id
            configure_kwargs = periodic_task.configure_kwargs
            task_kwargs = configure_kwargs.get("task_kwargs")
            if task_kwargs is None:
                task_kwargs = {}
                configure_kwargs["task_kwargs"] = task_kwargs
            task_kwargs["timestamp"] = timestamp

            description = {
                "task_name": task.name,
                "periodic_id": periodic_id,
                "defer_timestamp": timestamp,
                "kwargs": configure_kwargs,
            }
            job_deferrer = task.configure(**configure_kwargs)
            job = job_deferrer.job
            try:
                job_id = await job_deferrer.job_manager.defer_periodic_job(
                    job=job,
                    periodic_id=periodic_id,
                    defer_timestamp=timestamp,
                )
            except exceptions.AlreadyEnqueued:
                logger.debug(
                    f"Periodic job {job.call_string} cannot be enqueued: there is already "
                    f"a job in the queue with the queueing lock {task.queueing_lock}",
                    extra={
                        "action": "skip_periodic_task_queueing_lock",
                        "queueing_lock": task.queueing_lock,
                        "job": job.log_context(),
                        **description,
                    },
                )
                continue
            else:
                job = job.evolve(id=job_id)

            if job.id:
                logger.info(
                    f"Periodic job {job.call_string} deferred for timestamp "
                    f"{timestamp} with id {job.id}",
                    extra={
                        "action": "periodic_task_deferred",
                        "job": job.log_context(),
                        **description,
                    },
                )
            else:
                logger.debug(
                    f"Periodic job {job.call_string} skipped for timestamp "
                    f"{timestamp}: already deferred",
                    extra={
                        "action": "periodic_task_already_deferred",
                        "job": job.log_context(),
                        **description,
                    },
                )

    async def wait(self, next_tick: float) -> None:
        """
        Wait until it's time to defer new tasks.
        """
        logger.debug(
            f"Periodic deferrer waiting for next tasks to defer ({next_tick:.0f} s)",
            extra={"action": "wait_next_tick", "next_tick": next_tick},
        )

        await asyncio.sleep(next_tick + MARGIN)
