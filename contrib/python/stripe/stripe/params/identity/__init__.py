# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params.identity._verification_report_list_params import (
        VerificationReportListParams as VerificationReportListParams,
        VerificationReportListParamsCreated as VerificationReportListParamsCreated,
    )
    from stripe.params.identity._verification_report_retrieve_params import (
        VerificationReportRetrieveParams as VerificationReportRetrieveParams,
    )
    from stripe.params.identity._verification_session_cancel_params import (
        VerificationSessionCancelParams as VerificationSessionCancelParams,
    )
    from stripe.params.identity._verification_session_create_params import (
        VerificationSessionCreateParams as VerificationSessionCreateParams,
        VerificationSessionCreateParamsOptions as VerificationSessionCreateParamsOptions,
        VerificationSessionCreateParamsOptionsDocument as VerificationSessionCreateParamsOptionsDocument,
        VerificationSessionCreateParamsProvidedDetails as VerificationSessionCreateParamsProvidedDetails,
        VerificationSessionCreateParamsRelatedPerson as VerificationSessionCreateParamsRelatedPerson,
    )
    from stripe.params.identity._verification_session_list_params import (
        VerificationSessionListParams as VerificationSessionListParams,
        VerificationSessionListParamsCreated as VerificationSessionListParamsCreated,
    )
    from stripe.params.identity._verification_session_modify_params import (
        VerificationSessionModifyParams as VerificationSessionModifyParams,
        VerificationSessionModifyParamsOptions as VerificationSessionModifyParamsOptions,
        VerificationSessionModifyParamsOptionsDocument as VerificationSessionModifyParamsOptionsDocument,
        VerificationSessionModifyParamsProvidedDetails as VerificationSessionModifyParamsProvidedDetails,
    )
    from stripe.params.identity._verification_session_redact_params import (
        VerificationSessionRedactParams as VerificationSessionRedactParams,
    )
    from stripe.params.identity._verification_session_retrieve_params import (
        VerificationSessionRetrieveParams as VerificationSessionRetrieveParams,
    )
    from stripe.params.identity._verification_session_update_params import (
        VerificationSessionUpdateParams as VerificationSessionUpdateParams,
        VerificationSessionUpdateParamsOptions as VerificationSessionUpdateParamsOptions,
        VerificationSessionUpdateParamsOptionsDocument as VerificationSessionUpdateParamsOptionsDocument,
        VerificationSessionUpdateParamsProvidedDetails as VerificationSessionUpdateParamsProvidedDetails,
    )

# name -> (import_target, is_submodule)
_import_map = {
    "VerificationReportListParams": (
        "stripe.params.identity._verification_report_list_params",
        False,
    ),
    "VerificationReportListParamsCreated": (
        "stripe.params.identity._verification_report_list_params",
        False,
    ),
    "VerificationReportRetrieveParams": (
        "stripe.params.identity._verification_report_retrieve_params",
        False,
    ),
    "VerificationSessionCancelParams": (
        "stripe.params.identity._verification_session_cancel_params",
        False,
    ),
    "VerificationSessionCreateParams": (
        "stripe.params.identity._verification_session_create_params",
        False,
    ),
    "VerificationSessionCreateParamsOptions": (
        "stripe.params.identity._verification_session_create_params",
        False,
    ),
    "VerificationSessionCreateParamsOptionsDocument": (
        "stripe.params.identity._verification_session_create_params",
        False,
    ),
    "VerificationSessionCreateParamsProvidedDetails": (
        "stripe.params.identity._verification_session_create_params",
        False,
    ),
    "VerificationSessionCreateParamsRelatedPerson": (
        "stripe.params.identity._verification_session_create_params",
        False,
    ),
    "VerificationSessionListParams": (
        "stripe.params.identity._verification_session_list_params",
        False,
    ),
    "VerificationSessionListParamsCreated": (
        "stripe.params.identity._verification_session_list_params",
        False,
    ),
    "VerificationSessionModifyParams": (
        "stripe.params.identity._verification_session_modify_params",
        False,
    ),
    "VerificationSessionModifyParamsOptions": (
        "stripe.params.identity._verification_session_modify_params",
        False,
    ),
    "VerificationSessionModifyParamsOptionsDocument": (
        "stripe.params.identity._verification_session_modify_params",
        False,
    ),
    "VerificationSessionModifyParamsProvidedDetails": (
        "stripe.params.identity._verification_session_modify_params",
        False,
    ),
    "VerificationSessionRedactParams": (
        "stripe.params.identity._verification_session_redact_params",
        False,
    ),
    "VerificationSessionRetrieveParams": (
        "stripe.params.identity._verification_session_retrieve_params",
        False,
    ),
    "VerificationSessionUpdateParams": (
        "stripe.params.identity._verification_session_update_params",
        False,
    ),
    "VerificationSessionUpdateParamsOptions": (
        "stripe.params.identity._verification_session_update_params",
        False,
    ),
    "VerificationSessionUpdateParamsOptionsDocument": (
        "stripe.params.identity._verification_session_update_params",
        False,
    ),
    "VerificationSessionUpdateParamsProvidedDetails": (
        "stripe.params.identity._verification_session_update_params",
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
