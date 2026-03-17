import datetime
import logging
import uuid
from typing import TYPE_CHECKING, Any, Generic, Optional, TypeVar

import django
from django.conf import settings
from django.core.exceptions import SuspiciousOperation
from django.db import models
from django.db.models import F, Q
from django.db.models.constraints import CheckConstraint
from django.utils import timezone
from django.utils.module_loading import import_string
from django.utils.translation import gettext_lazy as _
from typing_extensions import ParamSpec

from django_tasks.base import (
    DEFAULT_TASK_QUEUE_NAME,
    TASK_DEFAULT_PRIORITY,
    TASK_MAX_PRIORITY,
    TASK_MIN_PRIORITY,
    Task,
    TaskError,
    TaskResultStatus,
)
from django_tasks.compat import TASK_CLASSES
from django_tasks.utils import (
    get_exception_traceback,
    get_module_path,
    normalize_json,
    retry,
)

from .utils import normalize_uuid

logger = logging.getLogger("django_tasks.backends.database")

T = TypeVar("T")
P = ParamSpec("P")

if TYPE_CHECKING:
    from .backend import TaskResult

    class GenericBase(Generic[P, T]):
        pass

else:

    class GenericBase:
        """
        https://code.djangoproject.com/ticket/33174
        """

        def __class_getitem__(cls, _):
            return cls


def get_date_max() -> datetime.datetime:
    return datetime.datetime(
        9999, 1, 1, tzinfo=datetime.timezone.utc if settings.USE_TZ else None
    )


class DBTaskResultQuerySet(models.QuerySet):
    def ready(self) -> "DBTaskResultQuerySet":
        """
        Return tasks which are ready to be processed.
        """
        return self.filter(
            status=TaskResultStatus.READY,
        ).filter(
            models.Q(run_after=get_date_max()) | models.Q(run_after__lte=timezone.now())
        )

    def succeeded(self) -> "DBTaskResultQuerySet":
        return self.filter(status=TaskResultStatus.SUCCEEDED)

    def failed(self) -> "DBTaskResultQuerySet":
        return self.filter(status=TaskResultStatus.FAILED)

    def running(self) -> "DBTaskResultQuerySet":
        return self.filter(status=TaskResultStatus.RUNNING)

    def finished(self) -> "DBTaskResultQuerySet":
        return self.failed() | self.succeeded()

    @retry()
    def get_locked(self) -> Optional["DBTaskResult"]:
        """
        Get a job, locking the row and accounting for deadlocks.
        """
        return self.select_for_update(skip_locked=True).first()


class DBTaskResult(GenericBase[P, T], models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    status = models.CharField(
        _("status"),
        choices=TaskResultStatus.choices,
        default=TaskResultStatus.READY,
        max_length=max(len(value) for value in TaskResultStatus.values),
    )

    enqueued_at = models.DateTimeField(_("enqueued at"), auto_now_add=True)
    started_at = models.DateTimeField(_("started at"), null=True)
    finished_at = models.DateTimeField(_("finished at"), null=True)

    args_kwargs = models.JSONField(_("args kwargs"))

    priority = models.IntegerField(_("priority"), default=TASK_DEFAULT_PRIORITY)

    task_path = models.TextField(_("task path"))
    worker_ids = models.JSONField(_("worker id"), default=list)

    queue_name = models.CharField(
        _("queue name"), default=DEFAULT_TASK_QUEUE_NAME, max_length=32
    )
    backend_name = models.CharField(_("backend name"), max_length=32)

    run_after = models.DateTimeField(_("run after"))

    return_value = models.JSONField(_("return value"), default=None, null=True)

    exception_class_path = models.TextField(_("exception class path"))
    traceback = models.TextField(_("traceback"))

    metadata = models.JSONField(_("metadata"), default=dict)

    objects = DBTaskResultQuerySet.as_manager()

    class Meta:
        ordering = [F("priority").desc(), F("run_after").asc()]
        verbose_name = _("Task Result")
        verbose_name_plural = _("Task Results")
        indexes = [
            models.Index(
                "status",
                *ordering,
                name="django_task_new_ordering_idx",
                condition=Q(status=TaskResultStatus.READY),
            ),
            models.Index(fields=["queue_name"]),
            models.Index(fields=["backend_name"]),
        ]

        if django.VERSION >= (5, 1):
            constraints = [
                CheckConstraint(
                    condition=Q(priority__range=(TASK_MIN_PRIORITY, TASK_MAX_PRIORITY)),
                    name="priority_range",
                )
            ]
        else:
            constraints = [
                CheckConstraint(
                    check=Q(priority__range=(TASK_MIN_PRIORITY, TASK_MAX_PRIORITY)),
                    name="priority_range",
                )
            ]

    @property
    def task(self) -> Task[P, T]:
        task = import_string(self.task_path)

        if not isinstance(task, TASK_CLASSES):
            raise SuspiciousOperation(
                f"Task {self.id} does not point to a Task ({self.task_path})"
            )

        return task.using(  # type: ignore[no-any-return]
            priority=self.priority,
            queue_name=self.queue_name,
            run_after=None if self.run_after == get_date_max() else self.run_after,
            backend=self.backend_name,
        )

    @property
    def task_result(self) -> "TaskResult[T]":
        from .backend import TaskResult

        task_result: TaskResult[T] = TaskResult(
            db_result=self,
            task=self.task,
            id=normalize_uuid(self.id),
            status=TaskResultStatus[self.status],
            enqueued_at=self.enqueued_at,
            started_at=self.started_at,
            last_attempted_at=self.started_at,
            finished_at=self.finished_at,
            args=self.args_kwargs["args"],
            kwargs=self.args_kwargs["kwargs"],
            backend=self.backend_name,
            errors=[],
            worker_ids=self.worker_ids,
            metadata=self.metadata,
        )

        if self.status == TaskResultStatus.FAILED:
            task_result.errors.append(
                TaskError(
                    exception_class_path=self.exception_class_path,
                    traceback=self.traceback,
                )
            )

        object.__setattr__(task_result, "_return_value", self.return_value)

        return task_result

    @property
    def task_name(self) -> str:
        # If the function for an existing task is no longer available, it'll either raise an
        # ImportError or ModuleNotFoundError (a subclass of ImportError).
        try:
            return self.task.name
        except ImportError:
            pass

        try:
            return self.task_path.rsplit(".", 1)[1]
        except IndexError:
            return self.task_path

    @retry(backoff_delay=0)
    def claim(self, worker_id: str) -> None:
        """
        Mark as job as being run
        """
        self.status = TaskResultStatus.RUNNING
        self.started_at = timezone.now()
        self.worker_ids = [*self.worker_ids, worker_id]
        self.save(update_fields=["status", "started_at", "worker_ids"])

    @retry()
    def set_succeeded(self, return_value: Any, metadata: dict) -> None:
        self.status = TaskResultStatus.SUCCEEDED
        self.finished_at = timezone.now()
        self.return_value = return_value
        self.exception_class_path = ""
        self.traceback = ""
        self.metadata = normalize_json(metadata)

        self.save(
            update_fields=[
                "status",
                "return_value",
                "finished_at",
                "exception_class_path",
                "traceback",
                "metadata",
            ]
        )

    @retry()
    def set_failed(self, exc: BaseException, metadata: dict | None) -> None:
        self.status = TaskResultStatus.FAILED
        self.finished_at = timezone.now()
        self.exception_class_path = get_module_path(type(exc))
        self.traceback = get_exception_traceback(exc)
        self.return_value = None

        if metadata is not None:
            self.metadata = normalize_json(metadata)

        self.save(
            update_fields=[
                "status",
                "return_value",
                "finished_at",
                "exception_class_path",
                "traceback",
                "metadata",
            ]
        )
