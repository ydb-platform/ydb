# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.v2.billing._meter_event import MeterEvent as MeterEvent
    from stripe.v2.billing._meter_event_adjustment import (
        MeterEventAdjustment as MeterEventAdjustment,
    )
    from stripe.v2.billing._meter_event_adjustment_service import (
        MeterEventAdjustmentService as MeterEventAdjustmentService,
    )
    from stripe.v2.billing._meter_event_service import (
        MeterEventService as MeterEventService,
    )
    from stripe.v2.billing._meter_event_session import (
        MeterEventSession as MeterEventSession,
    )
    from stripe.v2.billing._meter_event_session_service import (
        MeterEventSessionService as MeterEventSessionService,
    )
    from stripe.v2.billing._meter_event_stream_service import (
        MeterEventStreamService as MeterEventStreamService,
    )

# name -> (import_target, is_submodule)
_import_map = {
    "MeterEvent": ("stripe.v2.billing._meter_event", False),
    "MeterEventAdjustment": (
        "stripe.v2.billing._meter_event_adjustment",
        False,
    ),
    "MeterEventAdjustmentService": (
        "stripe.v2.billing._meter_event_adjustment_service",
        False,
    ),
    "MeterEventService": ("stripe.v2.billing._meter_event_service", False),
    "MeterEventSession": ("stripe.v2.billing._meter_event_session", False),
    "MeterEventSessionService": (
        "stripe.v2.billing._meter_event_session_service",
        False,
    ),
    "MeterEventStreamService": (
        "stripe.v2.billing._meter_event_stream_service",
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
