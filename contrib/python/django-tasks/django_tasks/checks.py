from collections.abc import Iterable, Sequence
from typing import Any

from django.apps.config import AppConfig
from django.core import checks

from django_tasks import task_backends


@checks.register
def check_tasks(
    app_configs: Sequence[AppConfig] = None, **kwargs: Any
) -> Iterable[checks.CheckMessage]:
    """Checks all registered task backends."""

    for backend in task_backends.all():
        yield from backend.check()
