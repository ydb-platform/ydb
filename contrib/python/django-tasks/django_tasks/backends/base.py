from abc import ABCMeta, abstractmethod
from collections.abc import Iterable
from inspect import iscoroutinefunction
from typing import Any, TypeVar

from asgiref.sync import sync_to_async
from django.conf import settings
from django.core import checks
from django.utils import timezone
from django.utils.inspect import get_func_args
from typing_extensions import ParamSpec

from django_tasks.base import (
    TASK_DEFAULT_PRIORITY,
    TASK_MAX_PRIORITY,
    TASK_MIN_PRIORITY,
    Task,
    TaskResult,
)
from django_tasks.exceptions import InvalidTaskError
from django_tasks.utils import is_module_level_function

T = TypeVar("T")
P = ParamSpec("P")


class BaseTaskBackend(metaclass=ABCMeta):
    alias: str

    task_class = Task

    supports_defer = False
    """Does the backend support Tasks to be enqueued with the run_after attribute?"""

    supports_async_task = False
    """Does the backend support coroutines to be enqueued?"""

    supports_get_result = False
    """Does the backend support results being retrieved (from any thread / process)?"""

    supports_priority = False
    """Does the backend support tasks being executed in a given priority order?"""

    def __init__(self, alias: str, params: dict) -> None:
        from django_tasks import DEFAULT_TASK_QUEUE_NAME

        self.alias = alias
        self.queues = set(params.get("QUEUES", [DEFAULT_TASK_QUEUE_NAME]))
        self.options = params.get("OPTIONS", {})

    def validate_task(self, task: Task) -> None:
        """
        Determine whether the provided Task can be executed by the backend.
        """
        if not is_module_level_function(task.func):
            raise InvalidTaskError("Task function must be defined at a module level.")

        if not self.supports_async_task and iscoroutinefunction(task.func):
            raise InvalidTaskError("Backend does not support async tasks.")

        task_func_args = get_func_args(task.func)

        if task.takes_context and (
            not task_func_args or task_func_args[0] != "context"
        ):
            raise InvalidTaskError(
                "Task takes context but does not have a first argument of 'context'."
            )

        if not self.supports_priority and task.priority != TASK_DEFAULT_PRIORITY:
            raise InvalidTaskError(
                "Backend does not support setting priority of tasks."
            )

        if (
            task.priority < TASK_MIN_PRIORITY
            or task.priority > TASK_MAX_PRIORITY
            or int(task.priority) != task.priority
        ):
            raise InvalidTaskError(
                f"priority must be a whole number between {TASK_MIN_PRIORITY} and {TASK_MAX_PRIORITY}."
            )

        if not self.supports_defer and task.run_after is not None:
            raise InvalidTaskError("Backend does not support run_after.")

        if (
            settings.USE_TZ
            and task.run_after is not None
            and not timezone.is_aware(task.run_after)
        ):
            raise InvalidTaskError("run_after must be an aware datetime.")

        if self.queues and task.queue_name not in self.queues:
            raise InvalidTaskError(
                f"Queue '{task.queue_name}' is not valid for backend."
            )

    @abstractmethod
    def enqueue(
        self,
        task: Task[P, T],
        args: P.args,  # type:ignore[valid-type]
        kwargs: P.kwargs,  # type:ignore[valid-type]
    ) -> TaskResult[T]:
        """
        Queue up a task to be executed
        """

    async def aenqueue(
        self,
        task: Task[P, T],
        args: P.args,  # type:ignore[valid-type]
        kwargs: P.kwargs,  #  type:ignore[valid-type]
    ) -> TaskResult[T]:
        """
        Queue up a task function (or coroutine) to be executed
        """
        return await sync_to_async(self.enqueue, thread_sensitive=True)(
            task=task, args=args, kwargs=kwargs
        )

    def get_result(self, result_id: str) -> TaskResult:
        """
        Retrieve a result by id if it exists, otherwise raise
        ResultDoesNotExist.
        """
        raise NotImplementedError(
            "This backend does not support retrieving or refreshing results."
        )

    async def aget_result(self, result_id: str) -> TaskResult:
        """See get_result()."""
        return await sync_to_async(self.get_result, thread_sensitive=True)(
            result_id=result_id
        )

    def check(self, **kwargs: Any) -> Iterable[checks.CheckMessage]:
        return []

    @abstractmethod
    def save_metadata(self, result_id: str, metadata: dict[str, Any]) -> None:
        """Save metadata"""

    async def asave_metadata(self, result_id: str, metadata: dict[str, Any]) -> None:
        """Save metadata"""
        return await sync_to_async(self.save_metadata, thread_sensitive=True)(
            result_id, metadata
        )
