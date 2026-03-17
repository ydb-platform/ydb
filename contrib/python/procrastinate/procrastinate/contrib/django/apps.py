from __future__ import annotations

from collections.abc import Iterable

from django import apps
from django.utils import module_loading

import procrastinate

from . import django_connector, procrastinate_app, settings


class ProcrastinateConfig(apps.AppConfig):
    name = "procrastinate.contrib.django"
    label = "procrastinate"

    def ready(self) -> None:
        procrastinate_app.current_app = create_app(
            blueprint=procrastinate_app.current_app
        )

    @property
    def app(self) -> procrastinate.App:
        return procrastinate_app.app


def get_import_paths() -> Iterable[str]:
    module_name = settings.settings.AUTODISCOVER_MODULE_NAME
    if module_name:
        # It's ok that we don't yield the discovered modules here, the
        # important thing is that they are imported.
        module_loading.autodiscover_modules(module_name)

    yield from settings.settings.IMPORT_PATHS


def create_app(blueprint: procrastinate.Blueprint) -> procrastinate.App:
    connector = django_connector.DjangoConnector(
        alias=settings.settings.DATABASE_ALIAS,
    )
    app = procrastinate.App(
        connector=connector,
        import_paths=list(get_import_paths()),
        worker_defaults=settings.settings.WORKER_DEFAULTS,
        periodic_defaults=settings.settings.PERIODIC_DEFAULTS,
    )

    if blueprint.tasks:
        app.add_tasks_from(blueprint, namespace="")

    on_app_ready_path = settings.settings.ON_APP_READY
    if on_app_ready_path:
        on_app_ready = module_loading.import_string(on_app_ready_path)
        on_app_ready(app)

    return app
