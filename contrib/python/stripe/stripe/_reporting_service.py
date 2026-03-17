# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.reporting._report_run_service import ReportRunService
    from stripe.reporting._report_type_service import ReportTypeService

_subservices = {
    "report_runs": [
        "stripe.reporting._report_run_service",
        "ReportRunService",
    ],
    "report_types": [
        "stripe.reporting._report_type_service",
        "ReportTypeService",
    ],
}


class ReportingService(StripeService):
    report_runs: "ReportRunService"
    report_types: "ReportTypeService"

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
