# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.v2.billing._meter_event_adjustment_service import (
        MeterEventAdjustmentService,
    )
    from stripe.v2.billing._meter_event_service import MeterEventService
    from stripe.v2.billing._meter_event_session_service import (
        MeterEventSessionService,
    )
    from stripe.v2.billing._meter_event_stream_service import (
        MeterEventStreamService,
    )

_subservices = {
    "meter_events": [
        "stripe.v2.billing._meter_event_service",
        "MeterEventService",
    ],
    "meter_event_adjustments": [
        "stripe.v2.billing._meter_event_adjustment_service",
        "MeterEventAdjustmentService",
    ],
    "meter_event_session": [
        "stripe.v2.billing._meter_event_session_service",
        "MeterEventSessionService",
    ],
    "meter_event_stream": [
        "stripe.v2.billing._meter_event_stream_service",
        "MeterEventStreamService",
    ],
}


class BillingService(StripeService):
    meter_events: "MeterEventService"
    meter_event_adjustments: "MeterEventAdjustmentService"
    meter_event_session: "MeterEventSessionService"
    meter_event_stream: "MeterEventStreamService"

    def __init__(self, requestor):
        super().__init__(requestor)

    def __getattr__(self, name):
        try:
            import_from, service = _subservices[name]
            service_class = getattr(
                import_module(import_from),
                service,
            )
            setattr(
                self,
                name,
                service_class(self._requestor),
            )
            return getattr(self, name)
        except KeyError:
            raise AttributeError()
