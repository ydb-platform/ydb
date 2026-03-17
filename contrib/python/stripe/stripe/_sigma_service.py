# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.sigma._scheduled_query_run_service import (
        ScheduledQueryRunService,
    )

_subservices = {
    "scheduled_query_runs": [
        "stripe.sigma._scheduled_query_run_service",
        "ScheduledQueryRunService",
    ],
}


class SigmaService(StripeService):
    scheduled_query_runs: "ScheduledQueryRunService"

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
