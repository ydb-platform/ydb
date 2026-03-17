# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params.radar._early_fraud_warning_list_params import (
        EarlyFraudWarningListParams as EarlyFraudWarningListParams,
        EarlyFraudWarningListParamsCreated as EarlyFraudWarningListParamsCreated,
    )
    from stripe.params.radar._early_fraud_warning_retrieve_params import (
        EarlyFraudWarningRetrieveParams as EarlyFraudWarningRetrieveParams,
    )
    from stripe.params.radar._payment_evaluation_create_params import (
        PaymentEvaluationCreateParams as PaymentEvaluationCreateParams,
        PaymentEvaluationCreateParamsClientDeviceMetadataDetails as PaymentEvaluationCreateParamsClientDeviceMetadataDetails,
        PaymentEvaluationCreateParamsCustomerDetails as PaymentEvaluationCreateParamsCustomerDetails,
        PaymentEvaluationCreateParamsPaymentDetails as PaymentEvaluationCreateParamsPaymentDetails,
        PaymentEvaluationCreateParamsPaymentDetailsMoneyMovementDetails as PaymentEvaluationCreateParamsPaymentDetailsMoneyMovementDetails,
        PaymentEvaluationCreateParamsPaymentDetailsMoneyMovementDetailsCard as PaymentEvaluationCreateParamsPaymentDetailsMoneyMovementDetailsCard,
        PaymentEvaluationCreateParamsPaymentDetailsPaymentMethodDetails as PaymentEvaluationCreateParamsPaymentDetailsPaymentMethodDetails,
        PaymentEvaluationCreateParamsPaymentDetailsPaymentMethodDetailsBillingDetails as PaymentEvaluationCreateParamsPaymentDetailsPaymentMethodDetailsBillingDetails,
        PaymentEvaluationCreateParamsPaymentDetailsPaymentMethodDetailsBillingDetailsAddress as PaymentEvaluationCreateParamsPaymentDetailsPaymentMethodDetailsBillingDetailsAddress,
        PaymentEvaluationCreateParamsPaymentDetailsShippingDetails as PaymentEvaluationCreateParamsPaymentDetailsShippingDetails,
        PaymentEvaluationCreateParamsPaymentDetailsShippingDetailsAddress as PaymentEvaluationCreateParamsPaymentDetailsShippingDetailsAddress,
    )
    from stripe.params.radar._value_list_create_params import (
        ValueListCreateParams as ValueListCreateParams,
    )
    from stripe.params.radar._value_list_delete_params import (
        ValueListDeleteParams as ValueListDeleteParams,
    )
    from stripe.params.radar._value_list_item_create_params import (
        ValueListItemCreateParams as ValueListItemCreateParams,
    )
    from stripe.params.radar._value_list_item_delete_params import (
        ValueListItemDeleteParams as ValueListItemDeleteParams,
    )
    from stripe.params.radar._value_list_item_list_params import (
        ValueListItemListParams as ValueListItemListParams,
        ValueListItemListParamsCreated as ValueListItemListParamsCreated,
    )
    from stripe.params.radar._value_list_item_retrieve_params import (
        ValueListItemRetrieveParams as ValueListItemRetrieveParams,
    )
    from stripe.params.radar._value_list_list_params import (
        ValueListListParams as ValueListListParams,
        ValueListListParamsCreated as ValueListListParamsCreated,
    )
    from stripe.params.radar._value_list_modify_params import (
        ValueListModifyParams as ValueListModifyParams,
    )
    from stripe.params.radar._value_list_retrieve_params import (
        ValueListRetrieveParams as ValueListRetrieveParams,
    )
    from stripe.params.radar._value_list_update_params import (
        ValueListUpdateParams as ValueListUpdateParams,
    )

# name -> (import_target, is_submodule)
_import_map = {
    "EarlyFraudWarningListParams": (
        "stripe.params.radar._early_fraud_warning_list_params",
        False,
    ),
    "EarlyFraudWarningListParamsCreated": (
        "stripe.params.radar._early_fraud_warning_list_params",
        False,
    ),
    "EarlyFraudWarningRetrieveParams": (
        "stripe.params.radar._early_fraud_warning_retrieve_params",
        False,
    ),
    "PaymentEvaluationCreateParams": (
        "stripe.params.radar._payment_evaluation_create_params",
        False,
    ),
    "PaymentEvaluationCreateParamsClientDeviceMetadataDetails": (
        "stripe.params.radar._payment_evaluation_create_params",
        False,
    ),
    "PaymentEvaluationCreateParamsCustomerDetails": (
        "stripe.params.radar._payment_evaluation_create_params",
        False,
    ),
    "PaymentEvaluationCreateParamsPaymentDetails": (
        "stripe.params.radar._payment_evaluation_create_params",
        False,
    ),
    "PaymentEvaluationCreateParamsPaymentDetailsMoneyMovementDetails": (
        "stripe.params.radar._payment_evaluation_create_params",
        False,
    ),
    "PaymentEvaluationCreateParamsPaymentDetailsMoneyMovementDetailsCard": (
        "stripe.params.radar._payment_evaluation_create_params",
        False,
    ),
    "PaymentEvaluationCreateParamsPaymentDetailsPaymentMethodDetails": (
        "stripe.params.radar._payment_evaluation_create_params",
        False,
    ),
    "PaymentEvaluationCreateParamsPaymentDetailsPaymentMethodDetailsBillingDetails": (
        "stripe.params.radar._payment_evaluation_create_params",
        False,
    ),
    "PaymentEvaluationCreateParamsPaymentDetailsPaymentMethodDetailsBillingDetailsAddress": (
        "stripe.params.radar._payment_evaluation_create_params",
        False,
    ),
    "PaymentEvaluationCreateParamsPaymentDetailsShippingDetails": (
        "stripe.params.radar._payment_evaluation_create_params",
        False,
    ),
    "PaymentEvaluationCreateParamsPaymentDetailsShippingDetailsAddress": (
        "stripe.params.radar._payment_evaluation_create_params",
        False,
    ),
    "ValueListCreateParams": (
        "stripe.params.radar._value_list_create_params",
        False,
    ),
    "ValueListDeleteParams": (
        "stripe.params.radar._value_list_delete_params",
        False,
    ),
    "ValueListItemCreateParams": (
        "stripe.params.radar._value_list_item_create_params",
        False,
    ),
    "ValueListItemDeleteParams": (
        "stripe.params.radar._value_list_item_delete_params",
        False,
    ),
    "ValueListItemListParams": (
        "stripe.params.radar._value_list_item_list_params",
        False,
    ),
    "ValueListItemListParamsCreated": (
        "stripe.params.radar._value_list_item_list_params",
        False,
    ),
    "ValueListItemRetrieveParams": (
        "stripe.params.radar._value_list_item_retrieve_params",
        False,
    ),
    "ValueListListParams": (
        "stripe.params.radar._value_list_list_params",
        False,
    ),
    "ValueListListParamsCreated": (
        "stripe.params.radar._value_list_list_params",
        False,
    ),
    "ValueListModifyParams": (
        "stripe.params.radar._value_list_modify_params",
        False,
    ),
    "ValueListRetrieveParams": (
        "stripe.params.radar._value_list_retrieve_params",
        False,
    ),
    "ValueListUpdateParams": (
        "stripe.params.radar._value_list_update_params",
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
