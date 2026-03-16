# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params.test_helpers.issuing._authorization_capture_params import (
        AuthorizationCaptureParams as AuthorizationCaptureParams,
        AuthorizationCaptureParamsPurchaseDetails as AuthorizationCaptureParamsPurchaseDetails,
        AuthorizationCaptureParamsPurchaseDetailsFleet as AuthorizationCaptureParamsPurchaseDetailsFleet,
        AuthorizationCaptureParamsPurchaseDetailsFleetCardholderPromptData as AuthorizationCaptureParamsPurchaseDetailsFleetCardholderPromptData,
        AuthorizationCaptureParamsPurchaseDetailsFleetReportedBreakdown as AuthorizationCaptureParamsPurchaseDetailsFleetReportedBreakdown,
        AuthorizationCaptureParamsPurchaseDetailsFleetReportedBreakdownFuel as AuthorizationCaptureParamsPurchaseDetailsFleetReportedBreakdownFuel,
        AuthorizationCaptureParamsPurchaseDetailsFleetReportedBreakdownNonFuel as AuthorizationCaptureParamsPurchaseDetailsFleetReportedBreakdownNonFuel,
        AuthorizationCaptureParamsPurchaseDetailsFleetReportedBreakdownTax as AuthorizationCaptureParamsPurchaseDetailsFleetReportedBreakdownTax,
        AuthorizationCaptureParamsPurchaseDetailsFlight as AuthorizationCaptureParamsPurchaseDetailsFlight,
        AuthorizationCaptureParamsPurchaseDetailsFlightSegment as AuthorizationCaptureParamsPurchaseDetailsFlightSegment,
        AuthorizationCaptureParamsPurchaseDetailsFuel as AuthorizationCaptureParamsPurchaseDetailsFuel,
        AuthorizationCaptureParamsPurchaseDetailsLodging as AuthorizationCaptureParamsPurchaseDetailsLodging,
        AuthorizationCaptureParamsPurchaseDetailsReceipt as AuthorizationCaptureParamsPurchaseDetailsReceipt,
    )
    from stripe.params.test_helpers.issuing._authorization_create_params import (
        AuthorizationCreateParams as AuthorizationCreateParams,
        AuthorizationCreateParamsAmountDetails as AuthorizationCreateParamsAmountDetails,
        AuthorizationCreateParamsFleet as AuthorizationCreateParamsFleet,
        AuthorizationCreateParamsFleetCardholderPromptData as AuthorizationCreateParamsFleetCardholderPromptData,
        AuthorizationCreateParamsFleetReportedBreakdown as AuthorizationCreateParamsFleetReportedBreakdown,
        AuthorizationCreateParamsFleetReportedBreakdownFuel as AuthorizationCreateParamsFleetReportedBreakdownFuel,
        AuthorizationCreateParamsFleetReportedBreakdownNonFuel as AuthorizationCreateParamsFleetReportedBreakdownNonFuel,
        AuthorizationCreateParamsFleetReportedBreakdownTax as AuthorizationCreateParamsFleetReportedBreakdownTax,
        AuthorizationCreateParamsFuel as AuthorizationCreateParamsFuel,
        AuthorizationCreateParamsMerchantData as AuthorizationCreateParamsMerchantData,
        AuthorizationCreateParamsNetworkData as AuthorizationCreateParamsNetworkData,
        AuthorizationCreateParamsRiskAssessment as AuthorizationCreateParamsRiskAssessment,
        AuthorizationCreateParamsRiskAssessmentCardTestingRisk as AuthorizationCreateParamsRiskAssessmentCardTestingRisk,
        AuthorizationCreateParamsRiskAssessmentFraudRisk as AuthorizationCreateParamsRiskAssessmentFraudRisk,
        AuthorizationCreateParamsRiskAssessmentMerchantDisputeRisk as AuthorizationCreateParamsRiskAssessmentMerchantDisputeRisk,
        AuthorizationCreateParamsVerificationData as AuthorizationCreateParamsVerificationData,
        AuthorizationCreateParamsVerificationDataAuthenticationExemption as AuthorizationCreateParamsVerificationDataAuthenticationExemption,
        AuthorizationCreateParamsVerificationDataThreeDSecure as AuthorizationCreateParamsVerificationDataThreeDSecure,
    )
    from stripe.params.test_helpers.issuing._authorization_expire_params import (
        AuthorizationExpireParams as AuthorizationExpireParams,
    )
    from stripe.params.test_helpers.issuing._authorization_finalize_amount_params import (
        AuthorizationFinalizeAmountParams as AuthorizationFinalizeAmountParams,
        AuthorizationFinalizeAmountParamsFleet as AuthorizationFinalizeAmountParamsFleet,
        AuthorizationFinalizeAmountParamsFleetCardholderPromptData as AuthorizationFinalizeAmountParamsFleetCardholderPromptData,
        AuthorizationFinalizeAmountParamsFleetReportedBreakdown as AuthorizationFinalizeAmountParamsFleetReportedBreakdown,
        AuthorizationFinalizeAmountParamsFleetReportedBreakdownFuel as AuthorizationFinalizeAmountParamsFleetReportedBreakdownFuel,
        AuthorizationFinalizeAmountParamsFleetReportedBreakdownNonFuel as AuthorizationFinalizeAmountParamsFleetReportedBreakdownNonFuel,
        AuthorizationFinalizeAmountParamsFleetReportedBreakdownTax as AuthorizationFinalizeAmountParamsFleetReportedBreakdownTax,
        AuthorizationFinalizeAmountParamsFuel as AuthorizationFinalizeAmountParamsFuel,
    )
    from stripe.params.test_helpers.issuing._authorization_increment_params import (
        AuthorizationIncrementParams as AuthorizationIncrementParams,
    )
    from stripe.params.test_helpers.issuing._authorization_respond_params import (
        AuthorizationRespondParams as AuthorizationRespondParams,
    )
    from stripe.params.test_helpers.issuing._authorization_reverse_params import (
        AuthorizationReverseParams as AuthorizationReverseParams,
    )
    from stripe.params.test_helpers.issuing._card_deliver_card_params import (
        CardDeliverCardParams as CardDeliverCardParams,
    )
    from stripe.params.test_helpers.issuing._card_fail_card_params import (
        CardFailCardParams as CardFailCardParams,
    )
    from stripe.params.test_helpers.issuing._card_return_card_params import (
        CardReturnCardParams as CardReturnCardParams,
    )
    from stripe.params.test_helpers.issuing._card_ship_card_params import (
        CardShipCardParams as CardShipCardParams,
    )
    from stripe.params.test_helpers.issuing._card_submit_card_params import (
        CardSubmitCardParams as CardSubmitCardParams,
    )
    from stripe.params.test_helpers.issuing._personalization_design_activate_params import (
        PersonalizationDesignActivateParams as PersonalizationDesignActivateParams,
    )
    from stripe.params.test_helpers.issuing._personalization_design_deactivate_params import (
        PersonalizationDesignDeactivateParams as PersonalizationDesignDeactivateParams,
    )
    from stripe.params.test_helpers.issuing._personalization_design_reject_params import (
        PersonalizationDesignRejectParams as PersonalizationDesignRejectParams,
        PersonalizationDesignRejectParamsRejectionReasons as PersonalizationDesignRejectParamsRejectionReasons,
    )
    from stripe.params.test_helpers.issuing._transaction_create_force_capture_params import (
        TransactionCreateForceCaptureParams as TransactionCreateForceCaptureParams,
        TransactionCreateForceCaptureParamsMerchantData as TransactionCreateForceCaptureParamsMerchantData,
        TransactionCreateForceCaptureParamsPurchaseDetails as TransactionCreateForceCaptureParamsPurchaseDetails,
        TransactionCreateForceCaptureParamsPurchaseDetailsFleet as TransactionCreateForceCaptureParamsPurchaseDetailsFleet,
        TransactionCreateForceCaptureParamsPurchaseDetailsFleetCardholderPromptData as TransactionCreateForceCaptureParamsPurchaseDetailsFleetCardholderPromptData,
        TransactionCreateForceCaptureParamsPurchaseDetailsFleetReportedBreakdown as TransactionCreateForceCaptureParamsPurchaseDetailsFleetReportedBreakdown,
        TransactionCreateForceCaptureParamsPurchaseDetailsFleetReportedBreakdownFuel as TransactionCreateForceCaptureParamsPurchaseDetailsFleetReportedBreakdownFuel,
        TransactionCreateForceCaptureParamsPurchaseDetailsFleetReportedBreakdownNonFuel as TransactionCreateForceCaptureParamsPurchaseDetailsFleetReportedBreakdownNonFuel,
        TransactionCreateForceCaptureParamsPurchaseDetailsFleetReportedBreakdownTax as TransactionCreateForceCaptureParamsPurchaseDetailsFleetReportedBreakdownTax,
        TransactionCreateForceCaptureParamsPurchaseDetailsFlight as TransactionCreateForceCaptureParamsPurchaseDetailsFlight,
        TransactionCreateForceCaptureParamsPurchaseDetailsFlightSegment as TransactionCreateForceCaptureParamsPurchaseDetailsFlightSegment,
        TransactionCreateForceCaptureParamsPurchaseDetailsFuel as TransactionCreateForceCaptureParamsPurchaseDetailsFuel,
        TransactionCreateForceCaptureParamsPurchaseDetailsLodging as TransactionCreateForceCaptureParamsPurchaseDetailsLodging,
        TransactionCreateForceCaptureParamsPurchaseDetailsReceipt as TransactionCreateForceCaptureParamsPurchaseDetailsReceipt,
    )
    from stripe.params.test_helpers.issuing._transaction_create_unlinked_refund_params import (
        TransactionCreateUnlinkedRefundParams as TransactionCreateUnlinkedRefundParams,
        TransactionCreateUnlinkedRefundParamsMerchantData as TransactionCreateUnlinkedRefundParamsMerchantData,
        TransactionCreateUnlinkedRefundParamsPurchaseDetails as TransactionCreateUnlinkedRefundParamsPurchaseDetails,
        TransactionCreateUnlinkedRefundParamsPurchaseDetailsFleet as TransactionCreateUnlinkedRefundParamsPurchaseDetailsFleet,
        TransactionCreateUnlinkedRefundParamsPurchaseDetailsFleetCardholderPromptData as TransactionCreateUnlinkedRefundParamsPurchaseDetailsFleetCardholderPromptData,
        TransactionCreateUnlinkedRefundParamsPurchaseDetailsFleetReportedBreakdown as TransactionCreateUnlinkedRefundParamsPurchaseDetailsFleetReportedBreakdown,
        TransactionCreateUnlinkedRefundParamsPurchaseDetailsFleetReportedBreakdownFuel as TransactionCreateUnlinkedRefundParamsPurchaseDetailsFleetReportedBreakdownFuel,
        TransactionCreateUnlinkedRefundParamsPurchaseDetailsFleetReportedBreakdownNonFuel as TransactionCreateUnlinkedRefundParamsPurchaseDetailsFleetReportedBreakdownNonFuel,
        TransactionCreateUnlinkedRefundParamsPurchaseDetailsFleetReportedBreakdownTax as TransactionCreateUnlinkedRefundParamsPurchaseDetailsFleetReportedBreakdownTax,
        TransactionCreateUnlinkedRefundParamsPurchaseDetailsFlight as TransactionCreateUnlinkedRefundParamsPurchaseDetailsFlight,
        TransactionCreateUnlinkedRefundParamsPurchaseDetailsFlightSegment as TransactionCreateUnlinkedRefundParamsPurchaseDetailsFlightSegment,
        TransactionCreateUnlinkedRefundParamsPurchaseDetailsFuel as TransactionCreateUnlinkedRefundParamsPurchaseDetailsFuel,
        TransactionCreateUnlinkedRefundParamsPurchaseDetailsLodging as TransactionCreateUnlinkedRefundParamsPurchaseDetailsLodging,
        TransactionCreateUnlinkedRefundParamsPurchaseDetailsReceipt as TransactionCreateUnlinkedRefundParamsPurchaseDetailsReceipt,
    )
    from stripe.params.test_helpers.issuing._transaction_refund_params import (
        TransactionRefundParams as TransactionRefundParams,
    )

# name -> (import_target, is_submodule)
_import_map = {
    "AuthorizationCaptureParams": (
        "stripe.params.test_helpers.issuing._authorization_capture_params",
        False,
    ),
    "AuthorizationCaptureParamsPurchaseDetails": (
        "stripe.params.test_helpers.issuing._authorization_capture_params",
        False,
    ),
    "AuthorizationCaptureParamsPurchaseDetailsFleet": (
        "stripe.params.test_helpers.issuing._authorization_capture_params",
        False,
    ),
    "AuthorizationCaptureParamsPurchaseDetailsFleetCardholderPromptData": (
        "stripe.params.test_helpers.issuing._authorization_capture_params",
        False,
    ),
    "AuthorizationCaptureParamsPurchaseDetailsFleetReportedBreakdown": (
        "stripe.params.test_helpers.issuing._authorization_capture_params",
        False,
    ),
    "AuthorizationCaptureParamsPurchaseDetailsFleetReportedBreakdownFuel": (
        "stripe.params.test_helpers.issuing._authorization_capture_params",
        False,
    ),
    "AuthorizationCaptureParamsPurchaseDetailsFleetReportedBreakdownNonFuel": (
        "stripe.params.test_helpers.issuing._authorization_capture_params",
        False,
    ),
    "AuthorizationCaptureParamsPurchaseDetailsFleetReportedBreakdownTax": (
        "stripe.params.test_helpers.issuing._authorization_capture_params",
        False,
    ),
    "AuthorizationCaptureParamsPurchaseDetailsFlight": (
        "stripe.params.test_helpers.issuing._authorization_capture_params",
        False,
    ),
    "AuthorizationCaptureParamsPurchaseDetailsFlightSegment": (
        "stripe.params.test_helpers.issuing._authorization_capture_params",
        False,
    ),
    "AuthorizationCaptureParamsPurchaseDetailsFuel": (
        "stripe.params.test_helpers.issuing._authorization_capture_params",
        False,
    ),
    "AuthorizationCaptureParamsPurchaseDetailsLodging": (
        "stripe.params.test_helpers.issuing._authorization_capture_params",
        False,
    ),
    "AuthorizationCaptureParamsPurchaseDetailsReceipt": (
        "stripe.params.test_helpers.issuing._authorization_capture_params",
        False,
    ),
    "AuthorizationCreateParams": (
        "stripe.params.test_helpers.issuing._authorization_create_params",
        False,
    ),
    "AuthorizationCreateParamsAmountDetails": (
        "stripe.params.test_helpers.issuing._authorization_create_params",
        False,
    ),
    "AuthorizationCreateParamsFleet": (
        "stripe.params.test_helpers.issuing._authorization_create_params",
        False,
    ),
    "AuthorizationCreateParamsFleetCardholderPromptData": (
        "stripe.params.test_helpers.issuing._authorization_create_params",
        False,
    ),
    "AuthorizationCreateParamsFleetReportedBreakdown": (
        "stripe.params.test_helpers.issuing._authorization_create_params",
        False,
    ),
    "AuthorizationCreateParamsFleetReportedBreakdownFuel": (
        "stripe.params.test_helpers.issuing._authorization_create_params",
        False,
    ),
    "AuthorizationCreateParamsFleetReportedBreakdownNonFuel": (
        "stripe.params.test_helpers.issuing._authorization_create_params",
        False,
    ),
    "AuthorizationCreateParamsFleetReportedBreakdownTax": (
        "stripe.params.test_helpers.issuing._authorization_create_params",
        False,
    ),
    "AuthorizationCreateParamsFuel": (
        "stripe.params.test_helpers.issuing._authorization_create_params",
        False,
    ),
    "AuthorizationCreateParamsMerchantData": (
        "stripe.params.test_helpers.issuing._authorization_create_params",
        False,
    ),
    "AuthorizationCreateParamsNetworkData": (
        "stripe.params.test_helpers.issuing._authorization_create_params",
        False,
    ),
    "AuthorizationCreateParamsRiskAssessment": (
        "stripe.params.test_helpers.issuing._authorization_create_params",
        False,
    ),
    "AuthorizationCreateParamsRiskAssessmentCardTestingRisk": (
        "stripe.params.test_helpers.issuing._authorization_create_params",
        False,
    ),
    "AuthorizationCreateParamsRiskAssessmentFraudRisk": (
        "stripe.params.test_helpers.issuing._authorization_create_params",
        False,
    ),
    "AuthorizationCreateParamsRiskAssessmentMerchantDisputeRisk": (
        "stripe.params.test_helpers.issuing._authorization_create_params",
        False,
    ),
    "AuthorizationCreateParamsVerificationData": (
        "stripe.params.test_helpers.issuing._authorization_create_params",
        False,
    ),
    "AuthorizationCreateParamsVerificationDataAuthenticationExemption": (
        "stripe.params.test_helpers.issuing._authorization_create_params",
        False,
    ),
    "AuthorizationCreateParamsVerificationDataThreeDSecure": (
        "stripe.params.test_helpers.issuing._authorization_create_params",
        False,
    ),
    "AuthorizationExpireParams": (
        "stripe.params.test_helpers.issuing._authorization_expire_params",
        False,
    ),
    "AuthorizationFinalizeAmountParams": (
        "stripe.params.test_helpers.issuing._authorization_finalize_amount_params",
        False,
    ),
    "AuthorizationFinalizeAmountParamsFleet": (
        "stripe.params.test_helpers.issuing._authorization_finalize_amount_params",
        False,
    ),
    "AuthorizationFinalizeAmountParamsFleetCardholderPromptData": (
        "stripe.params.test_helpers.issuing._authorization_finalize_amount_params",
        False,
    ),
    "AuthorizationFinalizeAmountParamsFleetReportedBreakdown": (
        "stripe.params.test_helpers.issuing._authorization_finalize_amount_params",
        False,
    ),
    "AuthorizationFinalizeAmountParamsFleetReportedBreakdownFuel": (
        "stripe.params.test_helpers.issuing._authorization_finalize_amount_params",
        False,
    ),
    "AuthorizationFinalizeAmountParamsFleetReportedBreakdownNonFuel": (
        "stripe.params.test_helpers.issuing._authorization_finalize_amount_params",
        False,
    ),
    "AuthorizationFinalizeAmountParamsFleetReportedBreakdownTax": (
        "stripe.params.test_helpers.issuing._authorization_finalize_amount_params",
        False,
    ),
    "AuthorizationFinalizeAmountParamsFuel": (
        "stripe.params.test_helpers.issuing._authorization_finalize_amount_params",
        False,
    ),
    "AuthorizationIncrementParams": (
        "stripe.params.test_helpers.issuing._authorization_increment_params",
        False,
    ),
    "AuthorizationRespondParams": (
        "stripe.params.test_helpers.issuing._authorization_respond_params",
        False,
    ),
    "AuthorizationReverseParams": (
        "stripe.params.test_helpers.issuing._authorization_reverse_params",
        False,
    ),
    "CardDeliverCardParams": (
        "stripe.params.test_helpers.issuing._card_deliver_card_params",
        False,
    ),
    "CardFailCardParams": (
        "stripe.params.test_helpers.issuing._card_fail_card_params",
        False,
    ),
    "CardReturnCardParams": (
        "stripe.params.test_helpers.issuing._card_return_card_params",
        False,
    ),
    "CardShipCardParams": (
        "stripe.params.test_helpers.issuing._card_ship_card_params",
        False,
    ),
    "CardSubmitCardParams": (
        "stripe.params.test_helpers.issuing._card_submit_card_params",
        False,
    ),
    "PersonalizationDesignActivateParams": (
        "stripe.params.test_helpers.issuing._personalization_design_activate_params",
        False,
    ),
    "PersonalizationDesignDeactivateParams": (
        "stripe.params.test_helpers.issuing._personalization_design_deactivate_params",
        False,
    ),
    "PersonalizationDesignRejectParams": (
        "stripe.params.test_helpers.issuing._personalization_design_reject_params",
        False,
    ),
    "PersonalizationDesignRejectParamsRejectionReasons": (
        "stripe.params.test_helpers.issuing._personalization_design_reject_params",
        False,
    ),
    "TransactionCreateForceCaptureParams": (
        "stripe.params.test_helpers.issuing._transaction_create_force_capture_params",
        False,
    ),
    "TransactionCreateForceCaptureParamsMerchantData": (
        "stripe.params.test_helpers.issuing._transaction_create_force_capture_params",
        False,
    ),
    "TransactionCreateForceCaptureParamsPurchaseDetails": (
        "stripe.params.test_helpers.issuing._transaction_create_force_capture_params",
        False,
    ),
    "TransactionCreateForceCaptureParamsPurchaseDetailsFleet": (
        "stripe.params.test_helpers.issuing._transaction_create_force_capture_params",
        False,
    ),
    "TransactionCreateForceCaptureParamsPurchaseDetailsFleetCardholderPromptData": (
        "stripe.params.test_helpers.issuing._transaction_create_force_capture_params",
        False,
    ),
    "TransactionCreateForceCaptureParamsPurchaseDetailsFleetReportedBreakdown": (
        "stripe.params.test_helpers.issuing._transaction_create_force_capture_params",
        False,
    ),
    "TransactionCreateForceCaptureParamsPurchaseDetailsFleetReportedBreakdownFuel": (
        "stripe.params.test_helpers.issuing._transaction_create_force_capture_params",
        False,
    ),
    "TransactionCreateForceCaptureParamsPurchaseDetailsFleetReportedBreakdownNonFuel": (
        "stripe.params.test_helpers.issuing._transaction_create_force_capture_params",
        False,
    ),
    "TransactionCreateForceCaptureParamsPurchaseDetailsFleetReportedBreakdownTax": (
        "stripe.params.test_helpers.issuing._transaction_create_force_capture_params",
        False,
    ),
    "TransactionCreateForceCaptureParamsPurchaseDetailsFlight": (
        "stripe.params.test_helpers.issuing._transaction_create_force_capture_params",
        False,
    ),
    "TransactionCreateForceCaptureParamsPurchaseDetailsFlightSegment": (
        "stripe.params.test_helpers.issuing._transaction_create_force_capture_params",
        False,
    ),
    "TransactionCreateForceCaptureParamsPurchaseDetailsFuel": (
        "stripe.params.test_helpers.issuing._transaction_create_force_capture_params",
        False,
    ),
    "TransactionCreateForceCaptureParamsPurchaseDetailsLodging": (
        "stripe.params.test_helpers.issuing._transaction_create_force_capture_params",
        False,
    ),
    "TransactionCreateForceCaptureParamsPurchaseDetailsReceipt": (
        "stripe.params.test_helpers.issuing._transaction_create_force_capture_params",
        False,
    ),
    "TransactionCreateUnlinkedRefundParams": (
        "stripe.params.test_helpers.issuing._transaction_create_unlinked_refund_params",
        False,
    ),
    "TransactionCreateUnlinkedRefundParamsMerchantData": (
        "stripe.params.test_helpers.issuing._transaction_create_unlinked_refund_params",
        False,
    ),
    "TransactionCreateUnlinkedRefundParamsPurchaseDetails": (
        "stripe.params.test_helpers.issuing._transaction_create_unlinked_refund_params",
        False,
    ),
    "TransactionCreateUnlinkedRefundParamsPurchaseDetailsFleet": (
        "stripe.params.test_helpers.issuing._transaction_create_unlinked_refund_params",
        False,
    ),
    "TransactionCreateUnlinkedRefundParamsPurchaseDetailsFleetCardholderPromptData": (
        "stripe.params.test_helpers.issuing._transaction_create_unlinked_refund_params",
        False,
    ),
    "TransactionCreateUnlinkedRefundParamsPurchaseDetailsFleetReportedBreakdown": (
        "stripe.params.test_helpers.issuing._transaction_create_unlinked_refund_params",
        False,
    ),
    "TransactionCreateUnlinkedRefundParamsPurchaseDetailsFleetReportedBreakdownFuel": (
        "stripe.params.test_helpers.issuing._transaction_create_unlinked_refund_params",
        False,
    ),
    "TransactionCreateUnlinkedRefundParamsPurchaseDetailsFleetReportedBreakdownNonFuel": (
        "stripe.params.test_helpers.issuing._transaction_create_unlinked_refund_params",
        False,
    ),
    "TransactionCreateUnlinkedRefundParamsPurchaseDetailsFleetReportedBreakdownTax": (
        "stripe.params.test_helpers.issuing._transaction_create_unlinked_refund_params",
        False,
    ),
    "TransactionCreateUnlinkedRefundParamsPurchaseDetailsFlight": (
        "stripe.params.test_helpers.issuing._transaction_create_unlinked_refund_params",
        False,
    ),
    "TransactionCreateUnlinkedRefundParamsPurchaseDetailsFlightSegment": (
        "stripe.params.test_helpers.issuing._transaction_create_unlinked_refund_params",
        False,
    ),
    "TransactionCreateUnlinkedRefundParamsPurchaseDetailsFuel": (
        "stripe.params.test_helpers.issuing._transaction_create_unlinked_refund_params",
        False,
    ),
    "TransactionCreateUnlinkedRefundParamsPurchaseDetailsLodging": (
        "stripe.params.test_helpers.issuing._transaction_create_unlinked_refund_params",
        False,
    ),
    "TransactionCreateUnlinkedRefundParamsPurchaseDetailsReceipt": (
        "stripe.params.test_helpers.issuing._transaction_create_unlinked_refund_params",
        False,
    ),
    "TransactionRefundParams": (
        "stripe.params.test_helpers.issuing._transaction_refund_params",
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
