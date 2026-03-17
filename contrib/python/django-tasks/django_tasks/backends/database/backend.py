from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypeVar

from django import VERSION
from django.apps import apps
from django.core import checks
from django.core.exceptions import ImproperlyConfigured, ValidationError
from django.db.models import Expression
from django.utils.module_loading import import_string
from django.utils.version import PY311
from typing_extensions import ParamSpec

from django_tasks.backends.base import BaseTaskBackend
from django_tasks.base import Task
from django_tasks.base import TaskResult as BaseTaskResult
from django_tasks.exceptions import TaskResultDoesNotExist
from django_tasks.signals import task_enqueued
from django_tasks.utils import normalize_json

if TYPE_CHECKING:
    from .models import DBTaskResult

T = TypeVar("T")
P = ParamSpec("P")


@dataclass(frozen=True, slots=PY311, kw_only=True)  # type: ignore[literal-required]
class TaskResult(BaseTaskResult[T]):
    db_result: "DBTaskResult"


class DatabaseBackend(BaseTaskBackend):
    supports_async_task = True
    supports_get_result = True
    supports_defer = True
    supports_priority = True

    def __init__(self, alias: str, params: dict) -> None:
        from .models import DBTaskResult

        super().__init__(alias, params)

        if id_function := self.options.get("id_function"):
            if callable(id_function):
                self.id_function = id_function
            else:
                self.id_function = import_string(id_function)
        else:
            # Fall back to the default defined on the model
            self.id_function = DBTaskResult._meta.pk.default

    def _task_to_db_task(
        self,
        task: Task[P, T],
        args: P.args,  # type:ignore[valid-type]
        kwargs: P.kwargs,  # type:ignore[valid-type]
    ) -> "DBTaskResult":
        from .models import DBTaskResult

        result_id = self.id_function()

        if VERSION < (6, 0) and isinstance(result_id, Expression):
            raise ImproperlyConfigured(
                "id_function cannot be a database expression until Django 6.0"
            )

        return DBTaskResult.objects.create(
            id=result_id,
            args_kwargs=normalize_json({"args": args, "kwargs": kwargs}),
            priority=task.priority,
            task_path=task.module_path,
            queue_name=task.queue_name,
            run_after=task.run_after,  # type: ignore[misc]
            backend_name=self.alias,
        )

    def enqueue(
        self,
        task: Task[P, T],
        args: P.args,  # type:ignore[valid-type]
        kwargs: P.kwargs,  # type:ignore[valid-type]
    ) -> TaskResult[T]:
        self.validate_task(task)

        db_result = self._task_to_db_task(task, args, kwargs)

        task_enqueued.send(type(self), task_result=db_result.task_result)

        return db_result.task_result

    def get_result(self, result_id: str) -> TaskResult:
        from .models import DBTaskResult

        try:
            return DBTaskResult.objects.get(id=result_id).task_result
        except (DBTaskResult.DoesNotExist, ValidationError) as e:
            raise TaskResultDoesNotExist(result_id) from e

    async def aget_result(self, result_id: str) -> TaskResult:
        from .models import DBTaskResult

        try:
            return (await DBTaskResult.objects.aget(id=result_id)).task_result
        except (DBTaskResult.DoesNotExist, ValidationError) as e:
            raise TaskResultDoesNotExist(result_id) from e

    def check(self, **kwargs: Any) -> Iterable[checks.CheckMessage]:
        yield from super().check(**kwargs)

        backend_name = self.__class__.__name__

        if not apps.is_installed("django_tasks.backends.database"):
            yield checks.Error(
                f"{backend_name} configured as django_tasks backend, but database app not installed",
                "Insert 'django_tasks.backends.database' in INSTALLED_APPS",
            )

    def save_metadata(self, result_id: str, metadata: dict[str, Any]) -> None:
        from .models import DBTaskResult

        DBTaskResult.objects.filter(id=result_id).update(
            metadata=normalize_json(metadata)
        )

    async def asave_metadata(self, result_id: str, metadata: dict[str, Any]) -> None:
        from .models import DBTaskResult

        await DBTaskResult.objects.filter(id=result_id).aupdate(
            metadata=normalize_json(metadata)
        )
