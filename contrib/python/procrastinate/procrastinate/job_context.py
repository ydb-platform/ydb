from __future__ import annotations

import time
from collections.abc import Callable, Iterable
from enum import Enum
from typing import Any

import attr

from procrastinate import app as app_module
from procrastinate import jobs, tasks, utils


@attr.dataclass(kw_only=True)
class JobResult:
    start_timestamp: float
    end_timestamp: float | None = None
    result: Any = None

    def duration(self, current_timestamp: float) -> float | None:
        return (self.end_timestamp or current_timestamp) - self.start_timestamp

    def as_dict(self):
        result = {}
        result.update(
            {
                "start_timestamp": self.start_timestamp,
                "duration": self.duration(current_timestamp=time.time()),
            }
        )

        if self.end_timestamp:
            result.update({"end_timestamp": self.end_timestamp, "result": self.result})
        return result


class AbortReason(Enum):
    """
    An enumeration of reasons a job is being aborted
    """

    USER_REQUEST = "user_request"  #: The user requested to abort the job
    SHUTDOWN = (
        "shutdown"  #: The job is being aborted as part of shutting down the worker
    )


@attr.dataclass(frozen=True, kw_only=True)
class JobContext:
    """
    Execution context of a running job.
    """

    #: Procrastinate `App` running this job
    app: app_module.App
    #: Name of the worker (may be useful for logging)
    worker_name: str | None = None
    #: Queues listened by this worker
    worker_queues: Iterable[str] | None = None
    #: Corresponding :py:class:`~jobs.Job`
    job: jobs.Job
    #: Time the job started to be processed
    start_timestamp: float

    additional_context: dict = attr.ib(factory=dict)

    #: Callable returning the reason the job should be aborted (or None if it
    #: should not be aborted)
    abort_reason: Callable[[], AbortReason | None]

    def should_abort(self) -> bool:
        """
        Returns True if the job should be aborted: in that case, the job should
        stop processing as soon as possible and raise raise
        :py:class:`~exceptions.JobAborted`
        """
        return bool(self.abort_reason())

    def evolve(self, **update: Any) -> JobContext:
        return attr.evolve(self, **update)

    @property
    def queues_display(self) -> str:
        return utils.queues_display(self.worker_queues)

    @property
    def task(self) -> tasks.Task:
        """
        The :py:class:`~tasks.Task` associated to the job
        """
        return self.app.tasks[self.job.task_name]
