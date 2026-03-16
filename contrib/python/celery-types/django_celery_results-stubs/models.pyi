from datetime import datetime
from typing import TypedDict

from django.db import models
from django_celery_results.managers import GroupResultManager, TaskResultManager

ALL_STATES: list[str]
TASK_STATE_CHOICES: list[tuple[str, str]]

class TaskResultDict(TypedDict):
    task_id: str
    task_name: str
    task_args: str
    task_kwargs: str
    status: str
    result: str
    date_done: datetime
    traceback: str
    meta: str

class TaskResult(models.Model):
    task_id: models.CharField[str]
    task_name: models.CharField[str | None]
    task_args: models.TextField[str | None]
    task_kwargs: models.TextField[str | None]
    status: models.CharField[str | None]
    content_type: models.CharField[str]
    content_encoding: models.CharField[str]
    result: models.TextField[str | None]
    date_done: models.DateTimeField[datetime]
    traceback: models.TextField[str | None]
    hidden: models.BooleanField[bool]
    meta: models.TextField[str | None]
    objects: TaskResultManager
    def as_dict(self) -> TaskResultDict: ...

class GroupResultDict(TypedDict):
    group_id: str
    result: str | None
    date_done: datetime

class GroupResult(models.Model):
    group_id: models.CharField[str]
    date_created: models.DateTimeField[datetime]
    date_done: models.DateTimeField[datetime]
    content_type: models.CharField[str]
    content_encoding: models.CharField[str]
    result: models.TextField[str | None]
    objects: GroupResultManager
    def as_dict(self) -> GroupResultDict: ...
