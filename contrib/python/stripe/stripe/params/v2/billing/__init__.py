# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params.v2.billing._meter_event_adjustment_create_params import (
        MeterEventAdjustmentCreateParams as MeterEventAdjustmentCreateParams,
        MeterEventAdjustmentCreateParamsCancel as MeterEventAdjustmentCreateParamsCancel,
    )
    from stripe.params.v2.billing._meter_event_create_params import (
        MeterEventCreateParams as MeterEventCreateParams,
    )
    from stripe.params.v2.billing._meter_event_session_create_params import (
        MeterEventSessionCreateParams as MeterEventSessionCreateParams,
    )
    from stripe.params.v2.billing._meter_event_stream_create_params import (
        MeterEventStreamCreateParams as MeterEventStreamCreateParams,
        MeterEventStreamCreateParamsEvent as MeterEventStreamCreateParamsEvent,
    )

# name -> (import_target, is_submodule)
_import_map = {
    "MeterEventAdjustmentCreateParams": (
        "stripe.params.v2.billing._meter_event_adjustment_create_params",
        False,
    ),
    "MeterEventAdjustmentCreateParamsCancel": (
        "stripe.params.v2.billing._meter_event_adjustment_create_params",
        False,
    ),
    "MeterEventCreateParams": (
        "stripe.params.v2.billing._meter_event_create_params",
        False,
    ),
    "MeterEventSessionCreateParams": (
        "stripe.params.v2.billing._meter_event_session_create_params",
        False,
    ),
    "MeterEventStreamCreateParams": (
        "stripe.params.v2.billing._meter_event_stream_create_params",
        False,
    ),
    "MeterEventStreamCreateParamsEvent": (
        "stripe.params.v2.billing._meter_event_stream_create_params",
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
