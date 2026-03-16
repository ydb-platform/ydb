from datetime import timedelta
from typing import Callable

from . import mapping

TaskDurationFunction = Callable[[mapping.Objects], timedelta]


def get_const_task_duration_function(task_duration_hint: timedelta) -> TaskDurationFunction:
    def task_duration_function(_):
        return task_duration_hint

    return task_duration_function
