# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params.sigma._scheduled_query_run_list_params import (
        ScheduledQueryRunListParams as ScheduledQueryRunListParams,
    )
    from stripe.params.sigma._scheduled_query_run_retrieve_params import (
        ScheduledQueryRunRetrieveParams as ScheduledQueryRunRetrieveParams,
    )

# name -> (import_target, is_submodule)
_import_map = {
    "ScheduledQueryRunListParams": (
        "stripe.params.sigma._scheduled_query_run_list_params",
        False,
    ),
    "ScheduledQueryRunRetrieveParams": (
        "stripe.params.sigma._scheduled_query_run_retrieve_params",
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
