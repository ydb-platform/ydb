from datetime import timedelta
from uuid import UUID

from django.db import models
from django.db.models.query import QuerySet
from django_celery_results.models import GroupResult, TaskResult

class TaskResultManager(models.Manager[TaskResult]):
    def get_task(self, task_id: str) -> TaskResult: ...
    def store_result(
        self,
        content_type: str,
        content_encoding: str,
        task_id: str | UUID,
        result: str,
        status: str,
        traceback: str | None = ...,
        meta: str | None = ...,
        task_name: str | None = ...,
        task_args: str | None = ...,
        task_kwargs: str | None = ...,
        using: str | None = ...,
    ) -> TaskResult: ...
    def get_all_expired(self, expires: timedelta) -> QuerySet[TaskResult]: ...
    def delete_expired(self, expires: timedelta) -> None: ...

class GroupResultManager(models.Manager[GroupResult]):
    def get_group(self, group_id: str) -> GroupResult: ...
    def store_group_result(
        self,
        content_type: str,
        content_encoding: str,
        group_id: str,
        result: str | None,
        using: str | None = ...,
    ) -> GroupResult: ...
