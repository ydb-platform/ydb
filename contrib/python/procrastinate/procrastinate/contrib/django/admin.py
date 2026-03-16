from __future__ import annotations

import json
from typing import Any

from django.apps import apps
from django.contrib import admin
from django.db.models import Prefetch, QuerySet
from django.http.request import HttpRequest
from django.template.loader import render_to_string
from django.utils import timezone
from django.utils.html import format_html
from django.utils.safestring import mark_safe

from procrastinate import App, utils
from procrastinate.contrib.django.apps import ProcrastinateConfig
from procrastinate.jobs import Status

from . import models

JOB_STATUS_EMOJI_MAPPING = {
    "todo": "🗓️",
    "doing": "🚂",
    "failed": "❌",
    "succeeded": "✅",
    "cancelled": "🤚",
    "aborting": "🔌🕑️",  # legacy, not used anymore
    "aborted": "🔌",
}


class ProcrastinateEventInline(admin.StackedInline):
    model = models.ProcrastinateEvent


@admin.register(models.ProcrastinateJob)
class ProcrastinateJobAdmin(admin.ModelAdmin):
    fields = [
        "pk",
        "short_task_name",
        "pretty_args",
        "pretty_status",
        "queue_name",
        "lock",
        "queueing_lock",
        "priority",
        "scheduled_at",
        "attempts",
    ]
    list_display = [
        "pk",
        "short_task_name",
        "pretty_args",
        "pretty_status",
        "summary",
    ]
    list_filter = [
        "status",
        "queue_name",
        "task_name",
        "lock",
        "queueing_lock",
        "scheduled_at",
        "priority",
    ]
    inlines = [ProcrastinateEventInline]

    def get_readonly_fields(
        self, request: Any, obj: models.ProcrastinateJob | None = None
    ):
        return [field.name for field in self.model._meta.get_fields() if field.concrete]

    def has_change_permission(
        self, request: Any, obj: models.ProcrastinateJob | None = None
    ):
        return False

    def has_add_permission(
        self, request: Any, obj: models.ProcrastinateJob | None = None
    ):
        return False

    def has_delete_permission(
        self, request: Any, obj: models.ProcrastinateJob | None = None
    ):
        return False

    def get_queryset(self, request: Any):
        return (
            super()
            .get_queryset(request)
            .prefetch_related(
                Prefetch(
                    "procrastinateevent_set",
                    queryset=models.ProcrastinateEvent.objects.order_by("-at"),
                )
            )
        )

    @admin.display(description="Status")
    def pretty_status(self, instance: models.ProcrastinateJob) -> str:
        emoji = JOB_STATUS_EMOJI_MAPPING.get(instance.status, "")
        return f"{emoji} {instance.status.title()}"

    @admin.display(description="Task Name")
    def short_task_name(self, instance: models.ProcrastinateJob) -> str:
        *modules, name = instance.task_name.split(".")
        return format_html(
            "<span title='{task_name}'>{name}</span>",
            task_name=instance.task_name,
            name=".".join(m[0] for m in modules) + f".{name}",
        )

    @admin.display(description="Args")
    def pretty_args(self, instance: models.ProcrastinateJob) -> str:
        indent = 2 if len(instance.args) > 1 or len(str(instance.args)) > 30 else None
        pretty_json = json.dumps(instance.args, indent=indent)
        if len(pretty_json) > 2000:
            pretty_json = pretty_json[:2000] + "..."
        return format_html(
            '<pre style="margin: 0">{pretty_json}</pre>', pretty_json=pretty_json
        )

    @admin.display(description="Summary")
    def summary(self, instance: models.ProcrastinateJob) -> str:
        if last_event := instance.procrastinateevent_set.first():  # pyright: ignore[reportAttributeAccessIssue]
            return mark_safe(
                render_to_string(
                    "procrastinate/admin/summary.html",
                    {
                        "last_event": last_event,
                        "job": instance,
                        "now": timezone.now(),
                    },
                ).strip()
            )
        return ""

    @admin.action(description="Retry Job")
    def retry(self, request: HttpRequest, queryset: QuerySet[models.ProcrastinateJob]):
        app_config: ProcrastinateConfig = apps.get_app_config("procrastinate")  # pyright: ignore [reportAssignmentType]
        p_app: App = app_config.app
        for job in queryset.filter(
            status__in=(Status.FAILED.value, Status.DOING.value)
        ):
            p_app.job_manager.retry_job_by_id(
                job.id, utils.utcnow(), job.priority, job.queue_name, job.lock
            )

    @admin.action(description="Cancel Job (only 'todo' jobs)")
    def cancel(self, request: HttpRequest, queryset: QuerySet[models.ProcrastinateJob]):
        app_config: ProcrastinateConfig = apps.get_app_config("procrastinate")  # pyright: ignore [reportAssignmentType]
        p_app: App = app_config.app
        for job in queryset.filter(status=Status.TODO.value):
            p_app.job_manager.cancel_job_by_id(job.id, abort=False)

    @admin.action(description="Abort Job (includes 'todo' & 'doing' jobs)")
    def abort(self, request: HttpRequest, queryset: QuerySet[models.ProcrastinateJob]):
        app_config: ProcrastinateConfig = apps.get_app_config("procrastinate")  # pyright: ignore [reportAssignmentType]
        p_app: App = app_config.app
        for job in queryset.filter(status__in=(Status.TODO.value, Status.DOING.value)):
            p_app.job_manager.cancel_job_by_id(job.id, abort=True)

    actions = [retry, cancel, abort]
