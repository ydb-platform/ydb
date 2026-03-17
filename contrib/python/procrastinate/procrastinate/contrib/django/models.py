from __future__ import annotations

from typing import Any, NoReturn

from django.db import models

from procrastinate import jobs

from . import exceptions, settings


def _read_only(*args: Any, **kwargs: Any) -> NoReturn:
    raise exceptions.ReadOnlyModel(
        "Procrastinate models exposed in Django, such as ProcrastinateJob "
        "are read-only. Please use the procrastinate CLI to interact with "
        "jobs."
    )


def _is_readonly() -> bool:
    return settings.settings.READONLY_MODELS


class ProcrastinateReadOnlyModelMixin:
    def save(self, *args: Any, **kwargs: Any) -> Any:
        if _is_readonly():
            _read_only()
        return super().save(*args, **kwargs)  # pyright: ignore[reportAttributeAccessIssue]

    def delete(self, *args: Any, **kwargs: Any) -> Any:
        if _is_readonly():
            _read_only()
        return super().delete(*args, **kwargs)  # pyright: ignore[reportAttributeAccessIssue]


_edit_methods = frozenset(
    (
        "create",
        "acreate",
        "get_or_create",
        "aget_or_create",
        "bulk_create",
        "abulk_create",
        "update",
        "aupdate",
        "update_or_create",
        "aupdate_or_create",
        "bulk_update",
        "abulk_update",
        "delete",
        "adelete",
    )
)


class ProcrastinateReadOnlyManager(models.Manager):
    def __getattribute__(self, name: str) -> Any:
        if name in _edit_methods and _is_readonly():
            return _read_only
        return super().__getattribute__(name)


class ProcrastinateWorker(ProcrastinateReadOnlyModelMixin, models.Model):
    id = models.BigAutoField(primary_key=True)
    last_heartbeat = models.DateTimeField()

    objects = ProcrastinateReadOnlyManager()

    class Meta:
        managed = False
        db_table = "procrastinate_workers"

    def __str__(self) -> str:
        return f"Worker {self.id} - Last heartbeat at {self.last_heartbeat}"


class ProcrastinateJob(ProcrastinateReadOnlyModelMixin, models.Model):
    STATUSES = (
        "todo",
        "doing",
        "succeeded",
        "failed",
        "cancelled",
        "aborted",
    )
    id = models.BigAutoField(primary_key=True)
    queue_name = models.CharField(max_length=128)
    task_name = models.CharField(max_length=128)
    priority = models.IntegerField()
    lock = models.TextField(unique=True, blank=True, null=True)
    args = models.JSONField()
    status = models.CharField(max_length=32, choices=[(e, e) for e in STATUSES])
    scheduled_at = models.DateTimeField(blank=True, null=True)
    attempts = models.IntegerField()
    queueing_lock = models.TextField(unique=True, blank=True, null=True)
    abort_requested = models.BooleanField()
    worker = models.ForeignKey(
        ProcrastinateWorker, on_delete=models.SET_NULL, blank=True, null=True
    )

    objects = ProcrastinateReadOnlyManager()

    class Meta:  # type: ignore
        managed = False
        db_table = "procrastinate_jobs"

    @property
    def procrastinate_job(self) -> jobs.Job:
        return jobs.Job(
            id=self.id,
            queue=self.queue_name,
            task_name=self.task_name,
            task_kwargs=self.args,
            priority=self.priority,
            lock=self.lock,
            status=self.status,
            scheduled_at=self.scheduled_at,
            attempts=self.attempts,
            abort_requested=self.abort_requested,
            queueing_lock=self.queueing_lock,
        )

    def __str__(self) -> str:
        return self.procrastinate_job.call_string


class ProcrastinateEvent(ProcrastinateReadOnlyModelMixin, models.Model):
    TYPES = (
        "deferred",
        "started",
        "deferred_for_retry",
        "failed",
        "succeeded",
        "cancelled",
        "abort_requested",
        "aborted",
        "scheduled",
    )
    id = models.BigAutoField(primary_key=True)
    job = models.ForeignKey(ProcrastinateJob, on_delete=models.CASCADE)
    type = models.CharField(max_length=32, choices=[(e, e) for e in TYPES])
    at = models.DateTimeField(blank=True, null=True)

    objects = ProcrastinateReadOnlyManager()

    class Meta:  # type: ignore
        managed = False
        db_table = "procrastinate_events"
        get_latest_by = "at"

    def __str__(self) -> str:
        return f"Event {self.id} - Job {self.job_id}: {self.type} at {self.at}"  # pyright: ignore[reportAttributeAccessIssue]


class ProcrastinatePeriodicDefer(ProcrastinateReadOnlyModelMixin, models.Model):
    id = models.BigAutoField(primary_key=True)
    task_name = models.CharField(max_length=128)
    defer_timestamp = models.BigIntegerField(blank=True, null=True)
    job = models.ForeignKey(
        ProcrastinateJob, on_delete=models.CASCADE, blank=True, null=True
    )
    periodic_id = models.CharField(max_length=128)

    class Meta:  # type: ignore
        managed = False
        db_table = "procrastinate_periodic_defers"
        unique_together = [("task_name", "periodic_id", "defer_timestamp")]
