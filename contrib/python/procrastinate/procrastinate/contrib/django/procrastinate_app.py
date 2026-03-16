from __future__ import annotations

import functools
from typing import Any, NoReturn, cast

from procrastinate import app as app_module
from procrastinate import blueprints

from . import exceptions


def _not_ready(_method: str, *args: Any, **kwargs: Any) -> NoReturn:
    base_text = (
        f"Cannot call procrastinate.contrib.app.{_method}() before "
        "the 'procrastinate.contrib.django' django app is ready."
    )
    details = (
        "If this message appears at import time, the app is not ready yet: "
        "move the corresponding code in an app's `AppConfig.ready()` method. "
        "If this message appears in an app's `AppConfig.ready()` method, "
        'make sure `"procrastinate.contrib.django"` appears before '
        "that app when ordering apps in the Django setting `INSTALLED_APPS`. "
        "Alternatively, use the Django setting "
        "PROCRASTINATE_ON_APP_READY (see the doc)."
    )
    raise exceptions.DjangoNotReady(base_text + "\n\n" + details)


class FutureApp(blueprints.Blueprint):
    _shadowed_methods = frozenset(
        [
            "__enter__",
            "__exit__",
            "_register_builtin_tasks",
            "_worker",
            "check_connection_async",
            "check_connection",
            "close_async",
            "close",
            "configure_task",
            "open_async",
            "open",
            "perform_import_paths",
            "run_worker_async",
            "run_worker",
            "schema_manager",
            "with_connector",
            "replace_connector",
            "will_configure_task",
        ]
    )
    for method in _shadowed_methods:
        locals()[method] = staticmethod(functools.partial(_not_ready, method))


class ProxyApp:
    def __repr__(self) -> str:
        return repr(current_app)

    def __getattr__(self, name: str):
        return getattr(current_app, name)


# Users may import the app before it's ready, so we're defining a proxy
# that references either the pre-app or the real app.
app: app_module.App = cast(app_module.App, ProxyApp())  # pyright: ignore[reportInvalidCast]

# Before the Django app is ready, we're defining the app as a blueprint so
# that tasks can be registered. The real app will be initialized in the
# ProcrastinateConfig.ready() method.
# This blueprint has special implementations for App methods so that if
# users try to use the app before it's ready, they get a helpful error message.
current_app: app_module.App = cast(app_module.App, FutureApp())  # pyright: ignore[reportInvalidCast]
