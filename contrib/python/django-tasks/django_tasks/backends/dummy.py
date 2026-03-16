from copy import deepcopy
from typing import Any, TypeVar

from django.utils import timezone
from typing_extensions import ParamSpec

from django_tasks.base import Task, TaskResult, TaskResultStatus
from django_tasks.exceptions import TaskResultDoesNotExist
from django_tasks.signals import task_enqueued
from django_tasks.utils import get_random_id

from .base import BaseTaskBackend

T = TypeVar("T")
P = ParamSpec("P")


class DummyBackend(BaseTaskBackend):
    supports_defer = True
    supports_async_task = True
    supports_priority = True
    results: list[TaskResult]

    def __init__(self, alias: str, params: dict) -> None:
        super().__init__(alias, params)

        self.results = []

    def enqueue(
        self,
        task: Task[P, T],
        args: P.args,  # type:ignore[valid-type]
        kwargs: P.kwargs,  # type:ignore[valid-type]
    ) -> TaskResult[T]:
        self.validate_task(task)

        result: TaskResult[T] = TaskResult(
            task=task,
            id=get_random_id(),
            status=TaskResultStatus.READY,
            enqueued_at=timezone.now(),
            started_at=None,
            last_attempted_at=None,
            finished_at=None,
            args=args,
            kwargs=kwargs,
            backend=self.alias,
            errors=[],
            worker_ids=[],
            metadata={},
        )

        self.results.append(result)

        task_enqueued.send(type(self), task_result=result)

        # Copy the task to prevent mutation issues
        return deepcopy(result)

    # We don't set `supports_get_result` as the results are scoped to the current thread
    def get_result(self, result_id: str) -> TaskResult:
        try:
            return next(result for result in self.results if result.id == result_id)
        except StopIteration:
            raise TaskResultDoesNotExist(result_id) from None

    async def aget_result(self, result_id: str) -> TaskResult:
        try:
            return next(result for result in self.results if result.id == result_id)
        except StopIteration:
            raise TaskResultDoesNotExist(result_id) from None

    def save_metadata(self, result_id: str, metadata: dict[str, Any]) -> None:
        pass

    async def asave_metadata(self, result_id: str, metadata: dict[str, Any]) -> None:
        pass

    def clear(self) -> None:
        self.results.clear()
