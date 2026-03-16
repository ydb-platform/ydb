# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params.billing_portal._configuration_create_params import (
        ConfigurationCreateParams as ConfigurationCreateParams,
        ConfigurationCreateParamsBusinessProfile as ConfigurationCreateParamsBusinessProfile,
        ConfigurationCreateParamsFeatures as ConfigurationCreateParamsFeatures,
        ConfigurationCreateParamsFeaturesCustomerUpdate as ConfigurationCreateParamsFeaturesCustomerUpdate,
        ConfigurationCreateParamsFeaturesInvoiceHistory as ConfigurationCreateParamsFeaturesInvoiceHistory,
        ConfigurationCreateParamsFeaturesPaymentMethodUpdate as ConfigurationCreateParamsFeaturesPaymentMethodUpdate,
        ConfigurationCreateParamsFeaturesSubscriptionCancel as ConfigurationCreateParamsFeaturesSubscriptionCancel,
        ConfigurationCreateParamsFeaturesSubscriptionCancelCancellationReason as ConfigurationCreateParamsFeaturesSubscriptionCancelCancellationReason,
        ConfigurationCreateParamsFeaturesSubscriptionUpdate as ConfigurationCreateParamsFeaturesSubscriptionUpdate,
        ConfigurationCreateParamsFeaturesSubscriptionUpdateProduct as ConfigurationCreateParamsFeaturesSubscriptionUpdateProduct,
        ConfigurationCreateParamsFeaturesSubscriptionUpdateProductAdjustableQuantity as ConfigurationCreateParamsFeaturesSubscriptionUpdateProductAdjustableQuantity,
        ConfigurationCreateParamsFeaturesSubscriptionUpdateScheduleAtPeriodEnd as ConfigurationCreateParamsFeaturesSubscriptionUpdateScheduleAtPeriodEnd,
        ConfigurationCreateParamsFeaturesSubscriptionUpdateScheduleAtPeriodEndCondition as ConfigurationCreateParamsFeaturesSubscriptionUpdateScheduleAtPeriodEndCondition,
        ConfigurationCreateParamsLoginPage as ConfigurationCreateParamsLoginPage,
    )
    from stripe.params.billing_portal._configuration_list_params import (
        ConfigurationListParams as ConfigurationListParams,
    )
    from stripe.params.billing_portal._configuration_modify_params import (
        ConfigurationModifyParams as ConfigurationModifyParams,
        ConfigurationModifyParamsBusinessProfile as ConfigurationModifyParamsBusinessProfile,
        ConfigurationModifyParamsFeatures as ConfigurationModifyParamsFeatures,
        ConfigurationModifyParamsFeaturesCustomerUpdate as ConfigurationModifyParamsFeaturesCustomerUpdate,
        ConfigurationModifyParamsFeaturesInvoiceHistory as ConfigurationModifyParamsFeaturesInvoiceHistory,
        ConfigurationModifyParamsFeaturesPaymentMethodUpdate as ConfigurationModifyParamsFeaturesPaymentMethodUpdate,
        ConfigurationModifyParamsFeaturesSubscriptionCancel as ConfigurationModifyParamsFeaturesSubscriptionCancel,
        ConfigurationModifyParamsFeaturesSubscriptionCancelCancellationReason as ConfigurationModifyParamsFeaturesSubscriptionCancelCancellationReason,
        ConfigurationModifyParamsFeaturesSubscriptionUpdate as ConfigurationModifyParamsFeaturesSubscriptionUpdate,
        ConfigurationModifyParamsFeaturesSubscriptionUpdateProduct as ConfigurationModifyParamsFeaturesSubscriptionUpdateProduct,
        ConfigurationModifyParamsFeaturesSubscriptionUpdateProductAdjustableQuantity as ConfigurationModifyParamsFeaturesSubscriptionUpdateProductAdjustableQuantity,
        ConfigurationModifyParamsFeaturesSubscriptionUpdateScheduleAtPeriodEnd as ConfigurationModifyParamsFeaturesSubscriptionUpdateScheduleAtPeriodEnd,
        ConfigurationModifyParamsFeaturesSubscriptionUpdateScheduleAtPeriodEndCondition as ConfigurationModifyParamsFeaturesSubscriptionUpdateScheduleAtPeriodEndCondition,
        ConfigurationModifyParamsLoginPage as ConfigurationModifyParamsLoginPage,
    )
    from stripe.params.billing_portal._configuration_retrieve_params import (
        ConfigurationRetrieveParams as ConfigurationRetrieveParams,
    )
    from stripe.params.billing_portal._configuration_update_params import (
        ConfigurationUpdateParams as ConfigurationUpdateParams,
        ConfigurationUpdateParamsBusinessProfile as ConfigurationUpdateParamsBusinessProfile,
        ConfigurationUpdateParamsFeatures as ConfigurationUpdateParamsFeatures,
        ConfigurationUpdateParamsFeaturesCustomerUpdate as ConfigurationUpdateParamsFeaturesCustomerUpdate,
        ConfigurationUpdateParamsFeaturesInvoiceHistory as ConfigurationUpdateParamsFeaturesInvoiceHistory,
        ConfigurationUpdateParamsFeaturesPaymentMethodUpdate as ConfigurationUpdateParamsFeaturesPaymentMethodUpdate,
        ConfigurationUpdateParamsFeaturesSubscriptionCancel as ConfigurationUpdateParamsFeaturesSubscriptionCancel,
        ConfigurationUpdateParamsFeaturesSubscriptionCancelCancellationReason as ConfigurationUpdateParamsFeaturesSubscriptionCancelCancellationReason,
        ConfigurationUpdateParamsFeaturesSubscriptionUpdate as ConfigurationUpdateParamsFeaturesSubscriptionUpdate,
        ConfigurationUpdateParamsFeaturesSubscriptionUpdateProduct as ConfigurationUpdateParamsFeaturesSubscriptionUpdateProduct,
        ConfigurationUpdateParamsFeaturesSubscriptionUpdateProductAdjustableQuantity as ConfigurationUpdateParamsFeaturesSubscriptionUpdateProductAdjustableQuantity,
        ConfigurationUpdateParamsFeaturesSubscriptionUpdateScheduleAtPeriodEnd as ConfigurationUpdateParamsFeaturesSubscriptionUpdateScheduleAtPeriodEnd,
        ConfigurationUpdateParamsFeaturesSubscriptionUpdateScheduleAtPeriodEndCondition as ConfigurationUpdateParamsFeaturesSubscriptionUpdateScheduleAtPeriodEndCondition,
        ConfigurationUpdateParamsLoginPage as ConfigurationUpdateParamsLoginPage,
    )
    from stripe.params.billing_portal._session_create_params import (
        SessionCreateParams as SessionCreateParams,
        SessionCreateParamsFlowData as SessionCreateParamsFlowData,
        SessionCreateParamsFlowDataAfterCompletion as SessionCreateParamsFlowDataAfterCompletion,
        SessionCreateParamsFlowDataAfterCompletionHostedConfirmation as SessionCreateParamsFlowDataAfterCompletionHostedConfirmation,
        SessionCreateParamsFlowDataAfterCompletionRedirect as SessionCreateParamsFlowDataAfterCompletionRedirect,
        SessionCreateParamsFlowDataSubscriptionCancel as SessionCreateParamsFlowDataSubscriptionCancel,
        SessionCreateParamsFlowDataSubscriptionCancelRetention as SessionCreateParamsFlowDataSubscriptionCancelRetention,
        SessionCreateParamsFlowDataSubscriptionCancelRetentionCouponOffer as SessionCreateParamsFlowDataSubscriptionCancelRetentionCouponOffer,
        SessionCreateParamsFlowDataSubscriptionUpdate as SessionCreateParamsFlowDataSubscriptionUpdate,
        SessionCreateParamsFlowDataSubscriptionUpdateConfirm as SessionCreateParamsFlowDataSubscriptionUpdateConfirm,
        SessionCreateParamsFlowDataSubscriptionUpdateConfirmDiscount as SessionCreateParamsFlowDataSubscriptionUpdateConfirmDiscount,
        SessionCreateParamsFlowDataSubscriptionUpdateConfirmItem as SessionCreateParamsFlowDataSubscriptionUpdateConfirmItem,
    )

# name -> (import_target, is_submodule)
_import_map = {
    "ConfigurationCreateParams": (
        "stripe.params.billing_portal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsBusinessProfile": (
        "stripe.params.billing_portal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsFeatures": (
        "stripe.params.billing_portal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsFeaturesCustomerUpdate": (
        "stripe.params.billing_portal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsFeaturesInvoiceHistory": (
        "stripe.params.billing_portal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsFeaturesPaymentMethodUpdate": (
        "stripe.params.billing_portal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsFeaturesSubscriptionCancel": (
        "stripe.params.billing_portal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsFeaturesSubscriptionCancelCancellationReason": (
        "stripe.params.billing_portal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsFeaturesSubscriptionUpdate": (
        "stripe.params.billing_portal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsFeaturesSubscriptionUpdateProduct": (
        "stripe.params.billing_portal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsFeaturesSubscriptionUpdateProductAdjustableQuantity": (
        "stripe.params.billing_portal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsFeaturesSubscriptionUpdateScheduleAtPeriodEnd": (
        "stripe.params.billing_portal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsFeaturesSubscriptionUpdateScheduleAtPeriodEndCondition": (
        "stripe.params.billing_portal._configuration_create_params",
        False,
    ),
    "ConfigurationCreateParamsLoginPage": (
        "stripe.params.billing_portal._configuration_create_params",
        False,
    ),
    "ConfigurationListParams": (
        "stripe.params.billing_portal._configuration_list_params",
        False,
    ),
    "ConfigurationModifyParams": (
        "stripe.params.billing_portal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsBusinessProfile": (
        "stripe.params.billing_portal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsFeatures": (
        "stripe.params.billing_portal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsFeaturesCustomerUpdate": (
        "stripe.params.billing_portal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsFeaturesInvoiceHistory": (
        "stripe.params.billing_portal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsFeaturesPaymentMethodUpdate": (
        "stripe.params.billing_portal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsFeaturesSubscriptionCancel": (
        "stripe.params.billing_portal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsFeaturesSubscriptionCancelCancellationReason": (
        "stripe.params.billing_portal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsFeaturesSubscriptionUpdate": (
        "stripe.params.billing_portal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsFeaturesSubscriptionUpdateProduct": (
        "stripe.params.billing_portal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsFeaturesSubscriptionUpdateProductAdjustableQuantity": (
        "stripe.params.billing_portal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsFeaturesSubscriptionUpdateScheduleAtPeriodEnd": (
        "stripe.params.billing_portal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsFeaturesSubscriptionUpdateScheduleAtPeriodEndCondition": (
        "stripe.params.billing_portal._configuration_modify_params",
        False,
    ),
    "ConfigurationModifyParamsLoginPage": (
        "stripe.params.billing_portal._configuration_modify_params",
        False,
    ),
    "ConfigurationRetrieveParams": (
        "stripe.params.billing_portal._configuration_retrieve_params",
        False,
    ),
    "ConfigurationUpdateParams": (
        "stripe.params.billing_portal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsBusinessProfile": (
        "stripe.params.billing_portal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsFeatures": (
        "stripe.params.billing_portal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsFeaturesCustomerUpdate": (
        "stripe.params.billing_portal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsFeaturesInvoiceHistory": (
        "stripe.params.billing_portal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsFeaturesPaymentMethodUpdate": (
        "stripe.params.billing_portal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsFeaturesSubscriptionCancel": (
        "stripe.params.billing_portal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsFeaturesSubscriptionCancelCancellationReason": (
        "stripe.params.billing_portal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsFeaturesSubscriptionUpdate": (
        "stripe.params.billing_portal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsFeaturesSubscriptionUpdateProduct": (
        "stripe.params.billing_portal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsFeaturesSubscriptionUpdateProductAdjustableQuantity": (
        "stripe.params.billing_portal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsFeaturesSubscriptionUpdateScheduleAtPeriodEnd": (
        "stripe.params.billing_portal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsFeaturesSubscriptionUpdateScheduleAtPeriodEndCondition": (
        "stripe.params.billing_portal._configuration_update_params",
        False,
    ),
    "ConfigurationUpdateParamsLoginPage": (
        "stripe.params.billing_portal._configuration_update_params",
        False,
    ),
    "SessionCreateParams": (
        "stripe.params.billing_portal._session_create_params",
        False,
    ),
    "SessionCreateParamsFlowData": (
        "stripe.params.billing_portal._session_create_params",
        False,
    ),
    "SessionCreateParamsFlowDataAfterCompletion": (
        "stripe.params.billing_portal._session_create_params",
        False,
    ),
    "SessionCreateParamsFlowDataAfterCompletionHostedConfirmation": (
        "stripe.params.billing_portal._session_create_params",
        False,
    ),
    "SessionCreateParamsFlowDataAfterCompletionRedirect": (
        "stripe.params.billing_portal._session_create_params",
        False,
    ),
    "SessionCreateParamsFlowDataSubscriptionCancel": (
        "stripe.params.billing_portal._session_create_params",
        False,
    ),
    "SessionCreateParamsFlowDataSubscriptionCancelRetention": (
        "stripe.params.billing_portal._session_create_params",
        False,
    ),
    "SessionCreateParamsFlowDataSubscriptionCancelRetentionCouponOffer": (
        "stripe.params.billing_portal._session_create_params",
        False,
    ),
    "SessionCreateParamsFlowDataSubscriptionUpdate": (
        "stripe.params.billing_portal._session_create_params",
        False,
    ),
    "SessionCreateParamsFlowDataSubscriptionUpdateConfirm": (
        "stripe.params.billing_portal._session_create_params",
        False,
    ),
    "SessionCreateParamsFlowDataSubscriptionUpdateConfirmDiscount": (
        "stripe.params.billing_portal._session_create_params",
        False,
    ),
    "SessionCreateParamsFlowDataSubscriptionUpdateConfirmItem": (
        "stripe.params.billing_portal._session_create_params",
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
