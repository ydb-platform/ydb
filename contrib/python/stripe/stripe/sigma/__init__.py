# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.sigma._scheduled_query_run import (
        ScheduledQueryRun as ScheduledQueryRun,
    )
    from stripe.sigma._scheduled_query_run_service import (
        ScheduledQueryRunService as ScheduledQueryRunService,
    )

# name -> (import_target, is_submodule)
_import_map = {
    "ScheduledQueryRun": ("stripe.sigma._scheduled_query_run", False),
    "ScheduledQueryRunService": (
        "stripe.sigma._scheduled_query_run_service",
        False,
    ),
}
if not TYPE_CHECKING:

    def __getattr__(name):
        try:
            target, is_submodule = _import_map[name]
            module = import_module(target)
            if is_submodule:
                return module

            return getattr(
                module,
                name,
            )
        except KeyError:
            raise AttributeError()
