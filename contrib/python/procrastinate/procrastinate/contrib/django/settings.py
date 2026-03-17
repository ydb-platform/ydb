from __future__ import annotations

from typing import Any

from django.conf import settings as django_settings
from typing_extensions import dataclass_transform

from procrastinate.app import WorkerOptions


@dataclass_transform()
class BaseSettings:
    def __getattribute__(self, name: str) -> Any:
        return getattr(
            django_settings,
            f"PROCRASTINATE_{name}",
            getattr(type(self), name),
        )


class Settings(BaseSettings):
    AUTODISCOVER_MODULE_NAME: str = "tasks"
    IMPORT_PATHS: list[str] = []
    DATABASE_ALIAS: str = "default"
    WORKER_DEFAULTS: WorkerOptions | None = None
    PERIODIC_DEFAULTS: dict[str, str] | None = None
    ON_APP_READY: str | None = None
    READONLY_MODELS: bool = True


settings = Settings()
