# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.identity._verification_report_service import (
        VerificationReportService,
    )
    from stripe.identity._verification_session_service import (
        VerificationSessionService,
    )

_subservices = {
    "verification_reports": [
        "stripe.identity._verification_report_service",
        "VerificationReportService",
    ],
    "verification_sessions": [
        "stripe.identity._verification_session_service",
        "VerificationSessionService",
    ],
}


class IdentityService(StripeService):
    verification_reports: "VerificationReportService"
    verification_sessions: "VerificationSessionService"

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
