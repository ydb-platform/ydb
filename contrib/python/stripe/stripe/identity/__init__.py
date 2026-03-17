# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.identity._verification_report import (
        VerificationReport as VerificationReport,
    )
    from stripe.identity._verification_report_service import (
        VerificationReportService as VerificationReportService,
    )
    from stripe.identity._verification_session import (
        VerificationSession as VerificationSession,
    )
    from stripe.identity._verification_session_service import (
        VerificationSessionService as VerificationSessionService,
    )

# name -> (import_target, is_submodule)
_import_map = {
    "VerificationReport": ("stripe.identity._verification_report", False),
    "VerificationReportService": (
        "stripe.identity._verification_report_service",
        False,
    ),
    "VerificationSession": ("stripe.identity._verification_session", False),
    "VerificationSessionService": (
        "stripe.identity._verification_session_service",
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
