from typing import Any

from celery.app.base import Celery
from celery.app.task import Task

current_app: Celery
current_task: Task[Any, Any]

def get_current_task() -> Task[Any, Any]: ...
