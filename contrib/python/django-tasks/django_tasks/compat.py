try:
    from django.tasks.base import Task as DjangoTask
except ImportError:
    DjangoTask = None

from .base import Task

__all__ = ["TASK_CLASSES"]

TASK_CLASSES = (Task, DjangoTask) if DjangoTask is not None else (Task,)
