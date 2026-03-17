# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params.issuing._authorization_approve_params import (
        AuthorizationApproveParams as AuthorizationApproveParams,
    )
    from stripe.params.issuing._authorization_capture_params import (
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
    from stripe.params.issuing._authorization_create_params import (
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
    from stripe.params.issuing._authorization_decline_params import (
        AuthorizationDeclineParams as AuthorizationDeclineParams,
    )
    from stripe.params.issuing._authorization_expire_params import (
        AuthorizationExpireParams as AuthorizationExpireParams,
    )
    from stripe.params.issuing._authorization_finalize_amount_params import (
        AuthorizationFinalizeAmountParams as AuthorizationFinalizeAmountParams,
        AuthorizationFinalizeAmountParamsFleet as AuthorizationFinalizeAmountParamsFleet,
        AuthorizationFinalizeAmountParamsFleetCardholderPromptData as AuthorizationFinalizeAmountParamsFleetCardholderPromptData,
        AuthorizationFinalizeAmountParamsFleetReportedBreakdown as AuthorizationFinalizeAmountParamsFleetReportedBreakdown,
        AuthorizationFinalizeAmountParamsFleetReportedBreakdownFuel as AuthorizationFinalizeAmountParamsFleetReportedBreakdownFuel,
        AuthorizationFinalizeAmountParamsFleetReportedBreakdownNonFuel as AuthorizationFinalizeAmountParamsFleetReportedBreakdownNonFuel,
        AuthorizationFinalizeAmountParamsFleetReportedBreakdownTax as AuthorizationFinalizeAmountParamsFleetReportedBreakdownTax,
        AuthorizationFinalizeAmountParamsFuel as AuthorizationFinalizeAmountParamsFuel,
    )
    from stripe.params.issuing._authorization_increment_params import (
        AuthorizationIncrementParams as AuthorizationIncrementParams,
    )
    from stripe.params.issuing._authorization_list_params import (
        AuthorizationListParams as AuthorizationListParams,
        AuthorizationListParamsCreated as AuthorizationListParamsCreated,
    )
    from stripe.params.issuing._authorization_modify_params import (
        AuthorizationModifyParams as AuthorizationModifyParams,
    )
    from stripe.params.issuing._authorization_respond_params import (
        AuthorizationRespondParams as AuthorizationRespondParams,
    )
    from stripe.params.issuing._authorization_retrieve_params import (
        AuthorizationRetrieveParams as AuthorizationRetrieveParams,
    )
    from stripe.params.issuing._authorization_reverse_params import (
        AuthorizationReverseParams as AuthorizationReverseParams,
    )
    from stripe.params.issuing._authorization_update_params import (
        AuthorizationUpdateParams as AuthorizationUpdateParams,
    )
    from stripe.params.issuing._card_create_params import (
        CardCreateParams as CardCreateParams,
        CardCreateParamsPin as CardCreateParamsPin,
        CardCreateParamsShipping as CardCreateParamsShipping,
        CardCreateParamsShippingAddress as CardCreateParamsShippingAddress,
        CardCreateParamsShippingAddressValidation as CardCreateParamsShippingAddressValidation,
        CardCreateParamsShippingCustoms as CardCreateParamsShippingCustoms,
        CardCreateParamsSpendingControls as CardCreateParamsSpendingControls,
        CardCreateParamsSpendingControlsSpendingLimit as CardCreateParamsSpendingControlsSpendingLimit,
    )
    from stripe.params.issuing._card_deliver_card_params import (
        CardDeliverCardParams as CardDeliverCardParams,
    )
    from stripe.params.issuing._card_fail_card_params import (
        CardFailCardParams as CardFailCardParams,
    )
    from stripe.params.issuing._card_list_params import (
        CardListParams as CardListParams,
        CardListParamsCreated as CardListParamsCreated,
    )
    from stripe.params.issuing._card_modify_params import (
        CardModifyParams as CardModifyParams,
        CardModifyParamsPin as CardModifyParamsPin,
        CardModifyParamsShipping as CardModifyParamsShipping,
        CardModifyParamsShippingAddress as CardModifyParamsShippingAddress,
        CardModifyParamsShippingAddressValidation as CardModifyParamsShippingAddressValidation,
        CardModifyParamsShippingCustoms as CardModifyParamsShippingCustoms,
        CardModifyParamsSpendingControls as CardModifyParamsSpendingControls,
        CardModifyParamsSpendingControlsSpendingLimit as CardModifyParamsSpendingControlsSpendingLimit,
    )
    from stripe.params.issuing._card_retrieve_params import (
        CardRetrieveParams as CardRetrieveParams,
    )
    from stripe.params.issuing._card_return_card_params import (
        CardReturnCardParams as CardReturnCardParams,
    )
    from stripe.params.issuing._card_ship_card_params import (
        CardShipCardParams as CardShipCardParams,
    )
    from stripe.params.issuing._card_submit_card_params import (
        CardSubmitCardParams as CardSubmitCardParams,
    )
    from stripe.params.issuing._card_update_params import (
        CardUpdateParams as CardUpdateParams,
        CardUpdateParamsPin as CardUpdateParamsPin,
        CardUpdateParamsShipping as CardUpdateParamsShipping,
        CardUpdateParamsShippingAddress as CardUpdateParamsShippingAddress,
        CardUpdateParamsShippingAddressValidation as CardUpdateParamsShippingAddressValidation,
        CardUpdateParamsShippingCustoms as CardUpdateParamsShippingCustoms,
        CardUpdateParamsSpendingControls as CardUpdateParamsSpendingControls,
        CardUpdateParamsSpendingControlsSpendingLimit as CardUpdateParamsSpendingControlsSpendingLimit,
    )
    from stripe.params.issuing._cardholder_create_params import (
        CardholderCreateParams as CardholderCreateParams,
        CardholderCreateParamsBilling as CardholderCreateParamsBilling,
        CardholderCreateParamsBillingAddress as CardholderCreateParamsBillingAddress,
        CardholderCreateParamsCompany as CardholderCreateParamsCompany,
        CardholderCreateParamsIndividual as CardholderCreateParamsIndividual,
        CardholderCreateParamsIndividualCardIssuing as CardholderCreateParamsIndividualCardIssuing,
        CardholderCreateParamsIndividualCardIssuingUserTermsAcceptance as CardholderCreateParamsIndividualCardIssuingUserTermsAcceptance,
        CardholderCreateParamsIndividualDob as CardholderCreateParamsIndividualDob,
        CardholderCreateParamsIndividualVerification as CardholderCreateParamsIndividualVerification,
        CardholderCreateParamsIndividualVerificationDocument as CardholderCreateParamsIndividualVerificationDocument,
        CardholderCreateParamsSpendingControls as CardholderCreateParamsSpendingControls,
        CardholderCreateParamsSpendingControlsSpendingLimit as CardholderCreateParamsSpendingControlsSpendingLimit,
    )
    from stripe.params.issuing._cardholder_list_params import (
        CardholderListParams as CardholderListParams,
        CardholderListParamsCreated as CardholderListParamsCreated,
    )
    from stripe.params.issuing._cardholder_modify_params import (
        CardholderModifyParams as CardholderModifyParams,
        CardholderModifyParamsBilling as CardholderModifyParamsBilling,
        CardholderModifyParamsBillingAddress as CardholderModifyParamsBillingAddress,
        CardholderModifyParamsCompany as CardholderModifyParamsCompany,
        CardholderModifyParamsIndividual as CardholderModifyParamsIndividual,
        CardholderModifyParamsIndividualCardIssuing as CardholderModifyParamsIndividualCardIssuing,
        CardholderModifyParamsIndividualCardIssuingUserTermsAcceptance as CardholderModifyParamsIndividualCardIssuingUserTermsAcceptance,
        CardholderModifyParamsIndividualDob as CardholderModifyParamsIndividualDob,
        CardholderModifyParamsIndividualVerification as CardholderModifyParamsIndividualVerification,
        CardholderModifyParamsIndividualVerificationDocument as CardholderModifyParamsIndividualVerificationDocument,
        CardholderModifyParamsSpendingControls as CardholderModifyParamsSpendingControls,
        CardholderModifyParamsSpendingControlsSpendingLimit as CardholderModifyParamsSpendingControlsSpendingLimit,
    )
    from stripe.params.issuing._cardholder_retrieve_params import (
        CardholderRetrieveParams as CardholderRetrieveParams,
    )
    from stripe.params.issuing._cardholder_update_params import (
        CardholderUpdateParams as CardholderUpdateParams,
        CardholderUpdateParamsBilling as CardholderUpdateParamsBilling,
        CardholderUpdateParamsBillingAddress as CardholderUpdateParamsBillingAddress,
        CardholderUpdateParamsCompany as CardholderUpdateParamsCompany,
        CardholderUpdateParamsIndividual as CardholderUpdateParamsIndividual,
        CardholderUpdateParamsIndividualCardIssuing as CardholderUpdateParamsIndividualCardIssuing,
        CardholderUpdateParamsIndividualCardIssuingUserTermsAcceptance as CardholderUpdateParamsIndividualCardIssuingUserTermsAcceptance,
        CardholderUpdateParamsIndividualDob as CardholderUpdateParamsIndividualDob,
        CardholderUpdateParamsIndividualVerification as CardholderUpdateParamsIndividualVerification,
        CardholderUpdateParamsIndividualVerificationDocument as CardholderUpdateParamsIndividualVerificationDocument,
        CardholderUpdateParamsSpendingControls as CardholderUpdateParamsSpendingControls,
        CardholderUpdateParamsSpendingControlsSpendingLimit as CardholderUpdateParamsSpendingControlsSpendingLimit,
    )
    from stripe.params.issuing._dispute_create_params import (
        DisputeCreateParams as DisputeCreateParams,
        DisputeCreateParamsEvidence as DisputeCreateParamsEvidence,
        DisputeCreateParamsEvidenceCanceled as DisputeCreateParamsEvidenceCanceled,
        DisputeCreateParamsEvidenceDuplicate as DisputeCreateParamsEvidenceDuplicate,
        DisputeCreateParamsEvidenceFraudulent as DisputeCreateParamsEvidenceFraudulent,
        DisputeCreateParamsEvidenceMerchandiseNotAsDescribed as DisputeCreateParamsEvidenceMerchandiseNotAsDescribed,
        DisputeCreateParamsEvidenceNoValidAuthorization as DisputeCreateParamsEvidenceNoValidAuthorization,
        DisputeCreateParamsEvidenceNotReceived as DisputeCreateParamsEvidenceNotReceived,
        DisputeCreateParamsEvidenceOther as DisputeCreateParamsEvidenceOther,
        DisputeCreateParamsEvidenceServiceNotAsDescribed as DisputeCreateParamsEvidenceServiceNotAsDescribed,
        DisputeCreateParamsTreasury as DisputeCreateParamsTreasury,
    )
    from stripe.params.issuing._dispute_list_params import (
        DisputeListParams as DisputeListParams,
        DisputeListParamsCreated as DisputeListParamsCreated,
    )
    from stripe.params.issuing._dispute_modify_params import (
        DisputeModifyParams as DisputeModifyParams,
        DisputeModifyParamsEvidence as DisputeModifyParamsEvidence,
        DisputeModifyParamsEvidenceCanceled as DisputeModifyParamsEvidenceCanceled,
        DisputeModifyParamsEvidenceDuplicate as DisputeModifyParamsEvidenceDuplicate,
        DisputeModifyParamsEvidenceFraudulent as DisputeModifyParamsEvidenceFraudulent,
        DisputeModifyParamsEvidenceMerchandiseNotAsDescribed as DisputeModifyParamsEvidenceMerchandiseNotAsDescribed,
        DisputeModifyParamsEvidenceNoValidAuthorization as DisputeModifyParamsEvidenceNoValidAuthorization,
        DisputeModifyParamsEvidenceNotReceived as DisputeModifyParamsEvidenceNotReceived,
        DisputeModifyParamsEvidenceOther as DisputeModifyParamsEvidenceOther,
        DisputeModifyParamsEvidenceServiceNotAsDescribed as DisputeModifyParamsEvidenceServiceNotAsDescribed,
    )
    from stripe.params.issuing._dispute_retrieve_params import (
        DisputeRetrieveParams as DisputeRetrieveParams,
    )
    from stripe.params.issuing._dispute_submit_params import (
        DisputeSubmitParams as DisputeSubmitParams,
    )
    from stripe.params.issuing._dispute_update_params import (
        DisputeUpdateParams as DisputeUpdateParams,
        DisputeUpdateParamsEvidence as DisputeUpdateParamsEvidence,
        DisputeUpdateParamsEvidenceCanceled as DisputeUpdateParamsEvidenceCanceled,
        DisputeUpdateParamsEvidenceDuplicate as DisputeUpdateParamsEvidenceDuplicate,
        DisputeUpdateParamsEvidenceFraudulent as DisputeUpdateParamsEvidenceFraudulent,
        DisputeUpdateParamsEvidenceMerchandiseNotAsDescribed as DisputeUpdateParamsEvidenceMerchandiseNotAsDescribed,
        DisputeUpdateParamsEvidenceNoValidAuthorization as DisputeUpdateParamsEvidenceNoValidAuthorization,
        DisputeUpdateParamsEvidenceNotReceived as DisputeUpdateParamsEvidenceNotReceived,
        DisputeUpdateParamsEvidenceOther as DisputeUpdateParamsEvidenceOther,
        DisputeUpdateParamsEvidenceServiceNotAsDescribed as DisputeUpdateParamsEvidenceServiceNotAsDescribed,
    )
    from stripe.params.issuing._personalization_design_activate_params import (
        PersonalizationDesignActivateParams as PersonalizationDesignActivateParams,
    )
    from stripe.params.issuing._personalization_design_create_params import (
        PersonalizationDesignCreateParams as PersonalizationDesignCreateParams,
        PersonalizationDesignCreateParamsCarrierText as PersonalizationDesignCreateParamsCarrierText,
        PersonalizationDesignCreateParamsPreferences as PersonalizationDesignCreateParamsPreferences,
    )
    from stripe.params.issuing._personalization_design_deactivate_params import (
        PersonalizationDesignDeactivateParams as PersonalizationDesignDeactivateParams,
    )
    from stripe.params.issuing._personalization_design_list_params import (
        PersonalizationDesignListParams as PersonalizationDesignListParams,
        PersonalizationDesignListParamsPreferences as PersonalizationDesignListParamsPreferences,
    )
    from stripe.params.issuing._personalization_design_modify_params import (
        PersonalizationDesignModifyParams as PersonalizationDesignModifyParams,
        PersonalizationDesignModifyParamsCarrierText as PersonalizationDesignModifyParamsCarrierText,
        PersonalizationDesignModifyParamsPreferences as PersonalizationDesignModifyParamsPreferences,
    )
    from stripe.params.issuing._personalization_design_reject_params import (
        PersonalizationDesignRejectParams as PersonalizationDesignRejectParams,
        PersonalizationDesignRejectParamsRejectionReasons as PersonalizationDesignRejectParamsRejectionReasons,
    )
    from stripe.params.issuing._personalization_design_retrieve_params import (
        PersonalizationDesignRetrieveParams as PersonalizationDesignRetrieveParams,
    )
    from stripe.params.issuing._personalization_design_update_params import (
        PersonalizationDesignUpdateParams as PersonalizationDesignUpdateParams,
        PersonalizationDesignUpdateParamsCarrierText as PersonalizationDesignUpdateParamsCarrierText,
        PersonalizationDesignUpdateParamsPreferences as PersonalizationDesignUpdateParamsPreferences,
    )
    from stripe.params.issuing._physical_bundle_list_params import (
        PhysicalBundleListParams as PhysicalBundleListParams,
    )
    from stripe.params.issuing._physical_bundle_retrieve_params import (
        PhysicalBundleRetrieveParams as PhysicalBundleRetrieveParams,
    )
    from stripe.params.issuing._token_list_params import (
        TokenListParams as TokenListParams,
        TokenListParamsCreated as TokenListParamsCreated,
    )
    from stripe.params.issuing._token_modify_params import (
        TokenModifyParams as TokenModifyParams,
    )
    from stripe.params.issuing._token_retrieve_params import (
        TokenRetrieveParams as TokenRetrieveParams,
    )
    from stripe.params.issuing._token_update_params import (
        TokenUpdateParams as TokenUpdateParams,
    )
    from stripe.params.issuing._transaction_create_force_capture_params import (
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
    from stripe.params.issuing._transaction_create_unlinked_refund_params import (
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
    from stripe.params.issuing._transaction_list_params import (
        TransactionListParams as TransactionListParams,
        TransactionListParamsCreated as TransactionListParamsCreated,
    )
    from stripe.params.issuing._transaction_modify_params import (
        TransactionModifyParams as TransactionModifyParams,
    )
    from stripe.params.issuing._transaction_refund_params import (
        TransactionRefundParams as TransactionRefundParams,
    )
    from stripe.params.issuing._transaction_retrieve_params import (
        TransactionRetrieveParams as TransactionRetrieveParams,
    )
    from stripe.params.issuing._transaction_update_params import (
        TransactionUpdateParams as TransactionUpdateParams,
    )

# name -> (import_target, is_submodule)
_import_map = {
    "AuthorizationApproveParams": (
        "stripe.params.issuing._authorization_approve_params",
        False,
    ),
    "AuthorizationCaptureParams": (
        "stripe.params.issuing._authorization_capture_params",
        False,
    ),
    "AuthorizationCaptureParamsPurchaseDetails": (
        "stripe.params.issuing._authorization_capture_params",
        False,
    ),
    "AuthorizationCaptureParamsPurchaseDetailsFleet": (
        "stripe.params.issuing._authorization_capture_params",
        False,
    ),
    "AuthorizationCaptureParamsPurchaseDetailsFleetCardholderPromptData": (
        "stripe.params.issuing._authorization_capture_params",
        False,
    ),
    "AuthorizationCaptureParamsPurchaseDetailsFleetReportedBreakdown": (
        "stripe.params.issuing._authorization_capture_params",
        False,
    ),
    "AuthorizationCaptureParamsPurchaseDetailsFleetReportedBreakdownFuel": (
        "stripe.params.issuing._authorization_capture_params",
        False,
    ),
    "AuthorizationCaptureParamsPurchaseDetailsFleetReportedBreakdownNonFuel": (
        "stripe.params.issuing._authorization_capture_params",
        False,
    ),
    "AuthorizationCaptureParamsPurchaseDetailsFleetReportedBreakdownTax": (
        "stripe.params.issuing._authorization_capture_params",
        False,
    ),
    "AuthorizationCaptureParamsPurchaseDetailsFlight": (
        "stripe.params.issuing._authorization_capture_params",
        False,
    ),
    "AuthorizationCaptureParamsPurchaseDetailsFlightSegment": (
        "stripe.params.issuing._authorization_capture_params",
        False,
    ),
    "AuthorizationCaptureParamsPurchaseDetailsFuel": (
        "stripe.params.issuing._authorization_capture_params",
        False,
    ),
    "AuthorizationCaptureParamsPurchaseDetailsLodging": (
        "stripe.params.issuing._authorization_capture_params",
        False,
    ),
    "AuthorizationCaptureParamsPurchaseDetailsReceipt": (
        "stripe.params.issuing._authorization_capture_params",
        False,
    ),
    "AuthorizationCreateParams": (
        "stripe.params.issuing._authorization_create_params",
        False,
    ),
    "AuthorizationCreateParamsAmountDetails": (
        "stripe.params.issuing._authorization_create_params",
        False,
    ),
    "AuthorizationCreateParamsFleet": (
        "stripe.params.issuing._authorization_create_params",
        False,
    ),
    "AuthorizationCreateParamsFleetCardholderPromptData": (
        "stripe.params.issuing._authorization_create_params",
        False,
    ),
    "AuthorizationCreateParamsFleetReportedBreakdown": (
        "stripe.params.issuing._authorization_create_params",
        False,
    ),
    "AuthorizationCreateParamsFleetReportedBreakdownFuel": (
        "stripe.params.issuing._authorization_create_params",
        False,
    ),
    "AuthorizationCreateParamsFleetReportedBreakdownNonFuel": (
        "stripe.params.issuing._authorization_create_params",
        False,
    ),
    "AuthorizationCreateParamsFleetReportedBreakdownTax": (
        "stripe.params.issuing._authorization_create_params",
        False,
    ),
    "AuthorizationCreateParamsFuel": (
        "stripe.params.issuing._authorization_create_params",
        False,
    ),
    "AuthorizationCreateParamsMerchantData": (
        "stripe.params.issuing._authorization_create_params",
        False,
    ),
    "AuthorizationCreateParamsNetworkData": (
        "stripe.params.issuing._authorization_create_params",
        False,
    ),
    "AuthorizationCreateParamsRiskAssessment": (
        "stripe.params.issuing._authorization_create_params",
        False,
    ),
    "AuthorizationCreateParamsRiskAssessmentCardTestingRisk": (
        "stripe.params.issuing._authorization_create_params",
        False,
    ),
    "AuthorizationCreateParamsRiskAssessmentFraudRisk": (
        "stripe.params.issuing._authorization_create_params",
        False,
    ),
    "AuthorizationCreateParamsRiskAssessmentMerchantDisputeRisk": (
        "stripe.params.issuing._authorization_create_params",
        False,
    ),
    "AuthorizationCreateParamsVerificationData": (
        "stripe.params.issuing._authorization_create_params",
        False,
    ),
    "AuthorizationCreateParamsVerificationDataAuthenticationExemption": (
        "stripe.params.issuing._authorization_create_params",
        False,
    ),
    "AuthorizationCreateParamsVerificationDataThreeDSecure": (
        "stripe.params.issuing._authorization_create_params",
        False,
    ),
    "AuthorizationDeclineParams": (
        "stripe.params.issuing._authorization_decline_params",
        False,
    ),
    "AuthorizationExpireParams": (
        "stripe.params.issuing._authorization_expire_params",
        False,
    ),
    "AuthorizationFinalizeAmountParams": (
        "stripe.params.issuing._authorization_finalize_amount_params",
        False,
    ),
    "AuthorizationFinalizeAmountParamsFleet": (
        "stripe.params.issuing._authorization_finalize_amount_params",
        False,
    ),
    "AuthorizationFinalizeAmountParamsFleetCardholderPromptData": (
        "stripe.params.issuing._authorization_finalize_amount_params",
        False,
    ),
    "AuthorizationFinalizeAmountParamsFleetReportedBreakdown": (
        "stripe.params.issuing._authorization_finalize_amount_params",
        False,
    ),
    "AuthorizationFinalizeAmountParamsFleetReportedBreakdownFuel": (
        "stripe.params.issuing._authorization_finalize_amount_params",
        False,
    ),
    "AuthorizationFinalizeAmountParamsFleetReportedBreakdownNonFuel": (
        "stripe.params.issuing._authorization_finalize_amount_params",
        False,
    ),
    "AuthorizationFinalizeAmountParamsFleetReportedBreakdownTax": (
        "stripe.params.issuing._authorization_finalize_amount_params",
        False,
    ),
    "AuthorizationFinalizeAmountParamsFuel": (
        "stripe.params.issuing._authorization_finalize_amount_params",
        False,
    ),
    "AuthorizationIncrementParams": (
        "stripe.params.issuing._authorization_increment_params",
        False,
    ),
    "AuthorizationListParams": (
        "stripe.params.issuing._authorization_list_params",
        False,
    ),
    "AuthorizationListParamsCreated": (
        "stripe.params.issuing._authorization_list_params",
        False,
    ),
    "AuthorizationModifyParams": (
        "stripe.params.issuing._authorization_modify_params",
        False,
    ),
    "AuthorizationRespondParams": (
        "stripe.params.issuing._authorization_respond_params",
        False,
    ),
    "AuthorizationRetrieveParams": (
        "stripe.params.issuing._authorization_retrieve_params",
        False,
    ),
    "AuthorizationReverseParams": (
        "stripe.params.issuing._authorization_reverse_params",
        False,
    ),
    "AuthorizationUpdateParams": (
        "stripe.params.issuing._authorization_update_params",
        False,
    ),
    "CardCreateParams": ("stripe.params.issuing._card_create_params", False),
    "CardCreateParamsPin": (
        "stripe.params.issuing._card_create_params",
        False,
    ),
    "CardCreateParamsShipping": (
        "stripe.params.issuing._card_create_params",
        False,
    ),
    "CardCreateParamsShippingAddress": (
        "stripe.params.issuing._card_create_params",
        False,
    ),
    "CardCreateParamsShippingAddressValidation": (
        "stripe.params.issuing._card_create_params",
        False,
    ),
    "CardCreateParamsShippingCustoms": (
        "stripe.params.issuing._card_create_params",
        False,
    ),
    "CardCreateParamsSpendingControls": (
        "stripe.params.issuing._card_create_params",
        False,
    ),
    "CardCreateParamsSpendingControlsSpendingLimit": (
        "stripe.params.issuing._card_create_params",
        False,
    ),
    "CardDeliverCardParams": (
        "stripe.params.issuing._card_deliver_card_params",
        False,
    ),
    "CardFailCardParams": (
        "stripe.params.issuing._card_fail_card_params",
        False,
    ),
    "CardListParams": ("stripe.params.issuing._card_list_params", False),
    "CardListParamsCreated": (
        "stripe.params.issuing._card_list_params",
        False,
    ),
    "CardModifyParams": ("stripe.params.issuing._card_modify_params", False),
    "CardModifyParamsPin": (
        "stripe.params.issuing._card_modify_params",
        False,
    ),
    "CardModifyParamsShipping": (
        "stripe.params.issuing._card_modify_params",
        False,
    ),
    "CardModifyParamsShippingAddress": (
        "stripe.params.issuing._card_modify_params",
        False,
    ),
    "CardModifyParamsShippingAddressValidation": (
        "stripe.params.issuing._card_modify_params",
        False,
    ),
    "CardModifyParamsShippingCustoms": (
        "stripe.params.issuing._card_modify_params",
        False,
    ),
    "CardModifyParamsSpendingControls": (
        "stripe.params.issuing._card_modify_params",
        False,
    ),
    "CardModifyParamsSpendingControlsSpendingLimit": (
        "stripe.params.issuing._card_modify_params",
        False,
    ),
    "CardRetrieveParams": (
        "stripe.params.issuing._card_retrieve_params",
        False,
    ),
    "CardReturnCardParams": (
        "stripe.params.issuing._card_return_card_params",
        False,
    ),
    "CardShipCardParams": (
        "stripe.params.issuing._card_ship_card_params",
        False,
    ),
    "CardSubmitCardParams": (
        "stripe.params.issuing._card_submit_card_params",
        False,
    ),
    "CardUpdateParams": ("stripe.params.issuing._card_update_params", False),
    "CardUpdateParamsPin": (
        "stripe.params.issuing._card_update_params",
        False,
    ),
    "CardUpdateParamsShipping": (
        "stripe.params.issuing._card_update_params",
        False,
    ),
    "CardUpdateParamsShippingAddress": (
        "stripe.params.issuing._card_update_params",
        False,
    ),
    "CardUpdateParamsShippingAddressValidation": (
        "stripe.params.issuing._card_update_params",
        False,
    ),
    "CardUpdateParamsShippingCustoms": (
        "stripe.params.issuing._card_update_params",
        False,
    ),
    "CardUpdateParamsSpendingControls": (
        "stripe.params.issuing._card_update_params",
        False,
    ),
    "CardUpdateParamsSpendingControlsSpendingLimit": (
        "stripe.params.issuing._card_update_params",
        False,
    ),
    "CardholderCreateParams": (
        "stripe.params.issuing._cardholder_create_params",
        False,
    ),
    "CardholderCreateParamsBilling": (
        "stripe.params.issuing._cardholder_create_params",
        False,
    ),
    "CardholderCreateParamsBillingAddress": (
        "stripe.params.issuing._cardholder_create_params",
        False,
    ),
    "CardholderCreateParamsCompany": (
        "stripe.params.issuing._cardholder_create_params",
        False,
    ),
    "CardholderCreateParamsIndividual": (
        "stripe.params.issuing._cardholder_create_params",
        False,
    ),
    "CardholderCreateParamsIndividualCardIssuing": (
        "stripe.params.issuing._cardholder_create_params",
        False,
    ),
    "CardholderCreateParamsIndividualCardIssuingUserTermsAcceptance": (
        "stripe.params.issuing._cardholder_create_params",
        False,
    ),
    "CardholderCreateParamsIndividualDob": (
        "stripe.params.issuing._cardholder_create_params",
        False,
    ),
    "CardholderCreateParamsIndividualVerification": (
        "stripe.params.issuing._cardholder_create_params",
        False,
    ),
    "CardholderCreateParamsIndividualVerificationDocument": (
        "stripe.params.issuing._cardholder_create_params",
        False,
    ),
    "CardholderCreateParamsSpendingControls": (
        "stripe.params.issuing._cardholder_create_params",
        False,
    ),
    "CardholderCreateParamsSpendingControlsSpendingLimit": (
        "stripe.params.issuing._cardholder_create_params",
        False,
    ),
    "CardholderListParams": (
        "stripe.params.issuing._cardholder_list_params",
        False,
    ),
    "CardholderListParamsCreated": (
        "stripe.params.issuing._cardholder_list_params",
        False,
    ),
    "CardholderModifyParams": (
        "stripe.params.issuing._cardholder_modify_params",
        False,
    ),
    "CardholderModifyParamsBilling": (
        "stripe.params.issuing._cardholder_modify_params",
        False,
    ),
    "CardholderModifyParamsBillingAddress": (
        "stripe.params.issuing._cardholder_modify_params",
        False,
    ),
    "CardholderModifyParamsCompany": (
        "stripe.params.issuing._cardholder_modify_params",
        False,
    ),
    "CardholderModifyParamsIndividual": (
        "stripe.params.issuing._cardholder_modify_params",
        False,
    ),
    "CardholderModifyParamsIndividualCardIssuing": (
        "stripe.params.issuing._cardholder_modify_params",
        False,
    ),
    "CardholderModifyParamsIndividualCardIssuingUserTermsAcceptance": (
        "stripe.params.issuing._cardholder_modify_params",
        False,
    ),
    "CardholderModifyParamsIndividualDob": (
        "stripe.params.issuing._cardholder_modify_params",
        False,
    ),
    "CardholderModifyParamsIndividualVerification": (
        "stripe.params.issuing._cardholder_modify_params",
        False,
    ),
    "CardholderModifyParamsIndividualVerificationDocument": (
        "stripe.params.issuing._cardholder_modify_params",
        False,
    ),
    "CardholderModifyParamsSpendingControls": (
        "stripe.params.issuing._cardholder_modify_params",
        False,
    ),
    "CardholderModifyParamsSpendingControlsSpendingLimit": (
        "stripe.params.issuing._cardholder_modify_params",
        False,
    ),
    "CardholderRetrieveParams": (
        "stripe.params.issuing._cardholder_retrieve_params",
        False,
    ),
    "CardholderUpdateParams": (
        "stripe.params.issuing._cardholder_update_params",
        False,
    ),
    "CardholderUpdateParamsBilling": (
        "stripe.params.issuing._cardholder_update_params",
        False,
    ),
    "CardholderUpdateParamsBillingAddress": (
        "stripe.params.issuing._cardholder_update_params",
        False,
    ),
    "CardholderUpdateParamsCompany": (
        "stripe.params.issuing._cardholder_update_params",
        False,
    ),
    "CardholderUpdateParamsIndividual": (
        "stripe.params.issuing._cardholder_update_params",
        False,
    ),
    "CardholderUpdateParamsIndividualCardIssuing": (
        "stripe.params.issuing._cardholder_update_params",
        False,
    ),
    "CardholderUpdateParamsIndividualCardIssuingUserTermsAcceptance": (
        "stripe.params.issuing._cardholder_update_params",
        False,
    ),
    "CardholderUpdateParamsIndividualDob": (
        "stripe.params.issuing._cardholder_update_params",
        False,
    ),
    "CardholderUpdateParamsIndividualVerification": (
        "stripe.params.issuing._cardholder_update_params",
        False,
    ),
    "CardholderUpdateParamsIndividualVerificationDocument": (
        "stripe.params.issuing._cardholder_update_params",
        False,
    ),
    "CardholderUpdateParamsSpendingControls": (
        "stripe.params.issuing._cardholder_update_params",
        False,
    ),
    "CardholderUpdateParamsSpendingControlsSpendingLimit": (
        "stripe.params.issuing._cardholder_update_params",
        False,
    ),
    "DisputeCreateParams": (
        "stripe.params.issuing._dispute_create_params",
        False,
    ),
    "DisputeCreateParamsEvidence": (
        "stripe.params.issuing._dispute_create_params",
        False,
    ),
    "DisputeCreateParamsEvidenceCanceled": (
        "stripe.params.issuing._dispute_create_params",
        False,
    ),
    "DisputeCreateParamsEvidenceDuplicate": (
        "stripe.params.issuing._dispute_create_params",
        False,
    ),
    "DisputeCreateParamsEvidenceFraudulent": (
        "stripe.params.issuing._dispute_create_params",
        False,
    ),
    "DisputeCreateParamsEvidenceMerchandiseNotAsDescribed": (
        "stripe.params.issuing._dispute_create_params",
        False,
    ),
    "DisputeCreateParamsEvidenceNoValidAuthorization": (
        "stripe.params.issuing._dispute_create_params",
        False,
    ),
    "DisputeCreateParamsEvidenceNotReceived": (
        "stripe.params.issuing._dispute_create_params",
        False,
    ),
    "DisputeCreateParamsEvidenceOther": (
        "stripe.params.issuing._dispute_create_params",
        False,
    ),
    "DisputeCreateParamsEvidenceServiceNotAsDescribed": (
        "stripe.params.issuing._dispute_create_params",
        False,
    ),
    "DisputeCreateParamsTreasury": (
        "stripe.params.issuing._dispute_create_params",
        False,
    ),
    "DisputeListParams": ("stripe.params.issuing._dispute_list_params", False),
    "DisputeListParamsCreated": (
        "stripe.params.issuing._dispute_list_params",
        False,
    ),
    "DisputeModifyParams": (
        "stripe.params.issuing._dispute_modify_params",
        False,
    ),
    "DisputeModifyParamsEvidence": (
        "stripe.params.issuing._dispute_modify_params",
        False,
    ),
    "DisputeModifyParamsEvidenceCanceled": (
        "stripe.params.issuing._dispute_modify_params",
        False,
    ),
    "DisputeModifyParamsEvidenceDuplicate": (
        "stripe.params.issuing._dispute_modify_params",
        False,
    ),
    "DisputeModifyParamsEvidenceFraudulent": (
        "stripe.params.issuing._dispute_modify_params",
        False,
    ),
    "DisputeModifyParamsEvidenceMerchandiseNotAsDescribed": (
        "stripe.params.issuing._dispute_modify_params",
        False,
    ),
    "DisputeModifyParamsEvidenceNoValidAuthorization": (
        "stripe.params.issuing._dispute_modify_params",
        False,
    ),
    "DisputeModifyParamsEvidenceNotReceived": (
        "stripe.params.issuing._dispute_modify_params",
        False,
    ),
    "DisputeModifyParamsEvidenceOther": (
        "stripe.params.issuing._dispute_modify_params",
        False,
    ),
    "DisputeModifyParamsEvidenceServiceNotAsDescribed": (
        "stripe.params.issuing._dispute_modify_params",
        False,
    ),
    "DisputeRetrieveParams": (
        "stripe.params.issuing._dispute_retrieve_params",
        False,
    ),
    "DisputeSubmitParams": (
        "stripe.params.issuing._dispute_submit_params",
        False,
    ),
    "DisputeUpdateParams": (
        "stripe.params.issuing._dispute_update_params",
        False,
    ),
    "DisputeUpdateParamsEvidence": (
        "stripe.params.issuing._dispute_update_params",
        False,
    ),
    "DisputeUpdateParamsEvidenceCanceled": (
        "stripe.params.issuing._dispute_update_params",
        False,
    ),
    "DisputeUpdateParamsEvidenceDuplicate": (
        "stripe.params.issuing._dispute_update_params",
        False,
    ),
    "DisputeUpdateParamsEvidenceFraudulent": (
        "stripe.params.issuing._dispute_update_params",
        False,
    ),
    "DisputeUpdateParamsEvidenceMerchandiseNotAsDescribed": (
        "stripe.params.issuing._dispute_update_params",
        False,
    ),
    "DisputeUpdateParamsEvidenceNoValidAuthorization": (
        "stripe.params.issuing._dispute_update_params",
        False,
    ),
    "DisputeUpdateParamsEvidenceNotReceived": (
        "stripe.params.issuing._dispute_update_params",
        False,
    ),
    "DisputeUpdateParamsEvidenceOther": (
        "stripe.params.issuing._dispute_update_params",
        False,
    ),
    "DisputeUpdateParamsEvidenceServiceNotAsDescribed": (
        "stripe.params.issuing._dispute_update_params",
        False,
    ),
    "PersonalizationDesignActivateParams": (
        "stripe.params.issuing._personalization_design_activate_params",
        False,
    ),
    "PersonalizationDesignCreateParams": (
        "stripe.params.issuing._personalization_design_create_params",
        False,
    ),
    "PersonalizationDesignCreateParamsCarrierText": (
        "stripe.params.issuing._personalization_design_create_params",
        False,
    ),
    "PersonalizationDesignCreateParamsPreferences": (
        "stripe.params.issuing._personalization_design_create_params",
        False,
    ),
    "PersonalizationDesignDeactivateParams": (
        "stripe.params.issuing._personalization_design_deactivate_params",
        False,
    ),
    "PersonalizationDesignListParams": (
        "stripe.params.issuing._personalization_design_list_params",
        False,
    ),
    "PersonalizationDesignListParamsPreferences": (
        "stripe.params.issuing._personalization_design_list_params",
        False,
    ),
    "PersonalizationDesignModifyParams": (
        "stripe.params.issuing._personalization_design_modify_params",
        False,
    ),
    "PersonalizationDesignModifyParamsCarrierText": (
        "stripe.params.issuing._personalization_design_modify_params",
        False,
    ),
    "PersonalizationDesignModifyParamsPreferences": (
        "stripe.params.issuing._personalization_design_modify_params",
        False,
    ),
    "PersonalizationDesignRejectParams": (
        "stripe.params.issuing._personalization_design_reject_params",
        False,
    ),
    "PersonalizationDesignRejectParamsRejectionReasons": (
        "stripe.params.issuing._personalization_design_reject_params",
        False,
    ),
    "PersonalizationDesignRetrieveParams": (
        "stripe.params.issuing._personalization_design_retrieve_params",
        False,
    ),
    "PersonalizationDesignUpdateParams": (
        "stripe.params.issuing._personalization_design_update_params",
        False,
    ),
    "PersonalizationDesignUpdateParamsCarrierText": (
        "stripe.params.issuing._personalization_design_update_params",
        False,
    ),
    "PersonalizationDesignUpdateParamsPreferences": (
        "stripe.params.issuing._personalization_design_update_params",
        False,
    ),
    "PhysicalBundleListParams": (
        "stripe.params.issuing._physical_bundle_list_params",
        False,
    ),
    "PhysicalBundleRetrieveParams": (
        "stripe.params.issuing._physical_bundle_retrieve_params",
        False,
    ),
    "TokenListParams": ("stripe.params.issuing._token_list_params", False),
    "TokenListParamsCreated": (
        "stripe.params.issuing._token_list_params",
        False,
    ),
    "TokenModifyParams": ("stripe.params.issuing._token_modify_params", False),
    "TokenRetrieveParams": (
        "stripe.params.issuing._token_retrieve_params",
        False,
    ),
    "TokenUpdateParams": ("stripe.params.issuing._token_update_params", False),
    "TransactionCreateForceCaptureParams": (
        "stripe.params.issuing._transaction_create_force_capture_params",
        False,
    ),
    "TransactionCreateForceCaptureParamsMerchantData": (
        "stripe.params.issuing._transaction_create_force_capture_params",
        False,
    ),
    "TransactionCreateForceCaptureParamsPurchaseDetails": (
        "stripe.params.issuing._transaction_create_force_capture_params",
        False,
    ),
    "TransactionCreateForceCaptureParamsPurchaseDetailsFleet": (
        "stripe.params.issuing._transaction_create_force_capture_params",
        False,
    ),
    "TransactionCreateForceCaptureParamsPurchaseDetailsFleetCardholderPromptData": (
        "stripe.params.issuing._transaction_create_force_capture_params",
        False,
    ),
    "TransactionCreateForceCaptureParamsPurchaseDetailsFleetReportedBreakdown": (
        "stripe.params.issuing._transaction_create_force_capture_params",
        False,
    ),
    "TransactionCreateForceCaptureParamsPurchaseDetailsFleetReportedBreakdownFuel": (
        "stripe.params.issuing._transaction_create_force_capture_params",
        False,
    ),
    "TransactionCreateForceCaptureParamsPurchaseDetailsFleetReportedBreakdownNonFuel": (
        "stripe.params.issuing._transaction_create_force_capture_params",
        False,
    ),
    "TransactionCreateForceCaptureParamsPurchaseDetailsFleetReportedBreakdownTax": (
        "stripe.params.issuing._transaction_create_force_capture_params",
        False,
    ),
    "TransactionCreateForceCaptureParamsPurchaseDetailsFlight": (
        "stripe.params.issuing._transaction_create_force_capture_params",
        False,
    ),
    "TransactionCreateForceCaptureParamsPurchaseDetailsFlightSegment": (
        "stripe.params.issuing._transaction_create_force_capture_params",
        False,
    ),
    "TransactionCreateForceCaptureParamsPurchaseDetailsFuel": (
        "stripe.params.issuing._transaction_create_force_capture_params",
        False,
    ),
    "TransactionCreateForceCaptureParamsPurchaseDetailsLodging": (
        "stripe.params.issuing._transaction_create_force_capture_params",
        False,
    ),
    "TransactionCreateForceCaptureParamsPurchaseDetailsReceipt": (
        "stripe.params.issuing._transaction_create_force_capture_params",
        False,
    ),
    "TransactionCreateUnlinkedRefundParams": (
        "stripe.params.issuing._transaction_create_unlinked_refund_params",
        False,
    ),
    "TransactionCreateUnlinkedRefundParamsMerchantData": (
        "stripe.params.issuing._transaction_create_unlinked_refund_params",
        False,
    ),
    "TransactionCreateUnlinkedRefundParamsPurchaseDetails": (
        "stripe.params.issuing._transaction_create_unlinked_refund_params",
        False,
    ),
    "TransactionCreateUnlinkedRefundParamsPurchaseDetailsFleet": (
        "stripe.params.issuing._transaction_create_unlinked_refund_params",
        False,
    ),
    "TransactionCreateUnlinkedRefundParamsPurchaseDetailsFleetCardholderPromptData": (
        "stripe.params.issuing._transaction_create_unlinked_refund_params",
        False,
    ),
    "TransactionCreateUnlinkedRefundParamsPurchaseDetailsFleetReportedBreakdown": (
        "stripe.params.issuing._transaction_create_unlinked_refund_params",
        False,
    ),
    "TransactionCreateUnlinkedRefundParamsPurchaseDetailsFleetReportedBreakdownFuel": (
        "stripe.params.issuing._transaction_create_unlinked_refund_params",
        False,
    ),
    "TransactionCreateUnlinkedRefundParamsPurchaseDetailsFleetReportedBreakdownNonFuel": (
        "stripe.params.issuing._transaction_create_unlinked_refund_params",
        False,
    ),
    "TransactionCreateUnlinkedRefundParamsPurchaseDetailsFleetReportedBreakdownTax": (
        "stripe.params.issuing._transaction_create_unlinked_refund_params",
        False,
    ),
    "TransactionCreateUnlinkedRefundParamsPurchaseDetailsFlight": (
        "stripe.params.issuing._transaction_create_unlinked_refund_params",
        False,
    ),
    "TransactionCreateUnlinkedRefundParamsPurchaseDetailsFlightSegment": (
        "stripe.params.issuing._transaction_create_unlinked_refund_params",
        False,
    ),
    "TransactionCreateUnlinkedRefundParamsPurchaseDetailsFuel": (
        "stripe.params.issuing._transaction_create_unlinked_refund_params",
        False,
    ),
    "TransactionCreateUnlinkedRefundParamsPurchaseDetailsLodging": (
        "stripe.params.issuing._transaction_create_unlinked_refund_params",
        False,
    ),
    "TransactionCreateUnlinkedRefundParamsPurchaseDetailsReceipt": (
        "stripe.params.issuing._transaction_create_unlinked_refund_params",
        False,
    ),
    "TransactionListParams": (
        "stripe.params.issuing._transaction_list_params",
        False,
    ),
    "TransactionListParamsCreated": (
        "stripe.params.issuing._transaction_list_params",
        False,
    ),
    "TransactionModifyParams": (
        "stripe.params.issuing._transaction_modify_params",
        False,
    ),
    "TransactionRefundParams": (
        "stripe.params.issuing._transaction_refund_params",
        False,
    ),
    "TransactionRetrieveParams": (
        "stripe.params.issuing._transaction_retrieve_params",
        False,
    ),
    "TransactionUpdateParams": (
        "stripe.params.issuing._transaction_update_params",
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
