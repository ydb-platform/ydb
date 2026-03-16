from __future__ import annotations

import datetime
import functools
import logging
from enum import Enum
from typing import TYPE_CHECKING, Any, Literal, TypeAlias, TypedDict

import attr

from procrastinate import types

if TYPE_CHECKING:
    from procrastinate import manager

logger = logging.getLogger(__name__)


DEFAULT_QUEUE = "default"
DEFAULT_PRIORITY = 0

cached_property = getattr(functools, "cached_property", property)


class JobInserted(TypedDict):
    type: Literal["job_inserted"]
    job_id: int


class AbortJobRequested(TypedDict):
    type: Literal["abort_job_requested"]
    job_id: int


Notification: TypeAlias = JobInserted | AbortJobRequested


def check_aware(
    instance: Job, attribute: attr.Attribute, value: datetime.datetime
) -> None:
    if value and value.utcoffset() is None:
        raise ValueError("Timezone aware datetime is required")


class Status(Enum):
    """
    An enumeration with all the possible job statuses.
    """

    TODO = "todo"  #: The job is waiting in a queue
    DOING = "doing"  #: A worker is running the job
    SUCCEEDED = "succeeded"  #: The job ended successfully
    FAILED = "failed"  #: The job ended with an error
    CANCELLED = "cancelled"  #: The job was cancelled
    ABORTING = "aborting"  #: legacy, not used anymore
    ABORTED = "aborted"  #: The job was aborted


class DeleteJobCondition(Enum):
    """
    An enumeration with all the possible conditions to delete a job
    """

    NEVER = "never"  #: Keep jobs in database after completion
    SUCCESSFUL = "successful"  #: Delete only successful jobs
    ALWAYS = "always"  #: Always delete jobs at completion


@attr.dataclass(frozen=True, kw_only=True)
class Job:
    """
    A job is the launching of a specific task with specific values for the
    keyword arguments.
    """

    #: Internal id uniquely identifying the job.
    id: int | None = None
    #: Status of the job.
    status: str | None = None
    #: Queue name the job will be run in.
    queue: str
    #: Priority of the job.
    priority: int = DEFAULT_PRIORITY
    #: No two jobs with the same lock string can run simultaneously
    lock: str | None
    #: No two jobs with the same queueing lock can be waiting in the queue.
    queueing_lock: str | None
    #: Name of the associated task.
    task_name: str
    #: Arguments used to call the task.
    task_kwargs: types.JSONDict = attr.ib(factory=dict)
    #: Date and time after which the job is expected to run.
    scheduled_at: datetime.datetime | None = attr.ib(
        default=None, validator=check_aware
    )
    #: Number of times the job has been tried.
    attempts: int = 0
    #: True if the job is requested to abort
    abort_requested: bool = False
    #: ID of the worker that is processing the job
    worker_id: int | None = None

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> Job:
        return cls(
            id=row["id"],
            status=row["status"],
            priority=row["priority"],
            lock=row["lock"],
            queueing_lock=row["queueing_lock"],
            task_name=row["task_name"],
            task_kwargs=row["args"],
            scheduled_at=row["scheduled_at"],
            queue=row["queue_name"],
            attempts=row["attempts"],
            abort_requested=row.get("abort_requested", False),
            worker_id=row.get("worker_id"),
        )

    def asdict(self) -> dict[str, Any]:
        return attr.asdict(self)

    def log_context(self) -> types.JSONDict:
        context = self.asdict()

        if context["scheduled_at"]:
            context["scheduled_at"] = context["scheduled_at"].isoformat()

        context["call_string"] = self.call_string
        return context

    def evolve(self, **kwargs: Any) -> Job:
        return attr.evolve(self, **kwargs)

    @cached_property
    def call_string(self):
        kwargs_string = ", ".join(
            f"{key}={value!r}" for key, value in self.task_kwargs.items()
        )
        return f"{self.task_name}[{self.id}]({kwargs_string})"


class JobDeferrer:
    """
    The main purpose of ``JobDeferrer`` is to get a hold of the job_manager and the job,
    so that we can call ``defer`` without having to specify the job_manager, and the job
    doesn't need a job_manager property.
    """

    def __init__(self, job_manager: manager.JobManager, job: Job):
        self.job = job
        self.job_manager = job_manager

    def make_new_job(self, **task_kwargs: types.JSONValue) -> Job:
        final_kwargs = self.job.task_kwargs.copy()
        final_kwargs.update(task_kwargs)

        return self.job.evolve(task_kwargs=final_kwargs)

    def _log_before_defer_jobs(self, jobs: list[Job]) -> None:
        job_count = len(jobs)
        logger.debug(
            f"About to defer {job_count} {'job' if job_count == 1 else 'jobs'}",
            extra={
                "action": "about_to_defer_jobs",
                "jobs": [job.log_context() for job in jobs],
            },
        )

    def _log_after_defer_jobs(self, jobs: list[Job]) -> None:
        job_count = len(jobs)
        logger.info(
            f"Deferred {job_count} {'job' if job_count == 1 else 'jobs'}",
            extra={
                "action": "jobs_deferred",
                "jobs": [job.log_context() for job in jobs],
            },
        )

    async def defer_async(self, **task_kwargs: types.JSONValue) -> int:
        """
        See `Task.defer_async` for details.
        """
        # Make sure this code stays synchronized with .defer()
        job = self.make_new_job(**task_kwargs)
        self._log_before_defer_jobs(jobs=[job])
        job = await self.job_manager.defer_job_async(job=job)
        self._log_after_defer_jobs(jobs=[job])
        assert job.id  # for mypy
        return job.id

    async def batch_defer_async(self, *task_kwargs: types.JSONDict) -> list[int]:
        """
        See `Task.batch_defer_async` for details.
        """
        jobs = [self.make_new_job(**kwargs) for kwargs in task_kwargs]
        self._log_before_defer_jobs(jobs=jobs)
        jobs = await self.job_manager.batch_defer_jobs_async(jobs=jobs)
        self._log_after_defer_jobs(jobs=jobs)

        job_ids: list[int] = []
        for job in jobs:
            assert job.id  # for mypy
            job_ids.append(job.id)
        return job_ids

    def defer(self, **task_kwargs: types.JSONValue) -> int:
        """
        See `Task.defer` for details.
        """
        # Make sure this code stays synchronized with .defer_async()
        job = self.make_new_job(**task_kwargs)
        self._log_before_defer_jobs(jobs=[job])
        job = self.job_manager.defer_job(job=job)
        self._log_after_defer_jobs(jobs=[job])
        assert job.id  # for mypy
        return job.id

    def batch_defer(self, *task_kwargs: types.JSONDict) -> list[int]:
        """
        See `Task.batch_defer` for details.
        """
        jobs = [self.make_new_job(**kwargs) for kwargs in task_kwargs]
        self._log_before_defer_jobs(jobs=jobs)
        jobs = self.job_manager.batch_defer_jobs(jobs=jobs)
        self._log_after_defer_jobs(jobs=jobs)

        job_ids: list[int] = []
        for job in jobs:
            assert job.id
            job_ids.append(job.id)
        return job_ids
