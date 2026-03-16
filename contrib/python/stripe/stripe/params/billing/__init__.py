# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params.billing._alert_activate_params import (
        AlertActivateParams as AlertActivateParams,
    )
    from stripe.params.billing._alert_archive_params import (
        AlertArchiveParams as AlertArchiveParams,
    )
    from stripe.params.billing._alert_create_params import (
        AlertCreateParams as AlertCreateParams,
        AlertCreateParamsUsageThreshold as AlertCreateParamsUsageThreshold,
        AlertCreateParamsUsageThresholdFilter as AlertCreateParamsUsageThresholdFilter,
    )
    from stripe.params.billing._alert_deactivate_params import (
        AlertDeactivateParams as AlertDeactivateParams,
    )
    from stripe.params.billing._alert_list_params import (
        AlertListParams as AlertListParams,
    )
    from stripe.params.billing._alert_retrieve_params import (
        AlertRetrieveParams as AlertRetrieveParams,
    )
    from stripe.params.billing._credit_balance_summary_retrieve_params import (
        CreditBalanceSummaryRetrieveParams as CreditBalanceSummaryRetrieveParams,
        CreditBalanceSummaryRetrieveParamsFilter as CreditBalanceSummaryRetrieveParamsFilter,
        CreditBalanceSummaryRetrieveParamsFilterApplicabilityScope as CreditBalanceSummaryRetrieveParamsFilterApplicabilityScope,
        CreditBalanceSummaryRetrieveParamsFilterApplicabilityScopePrice as CreditBalanceSummaryRetrieveParamsFilterApplicabilityScopePrice,
    )
    from stripe.params.billing._credit_balance_transaction_list_params import (
        CreditBalanceTransactionListParams as CreditBalanceTransactionListParams,
    )
    from stripe.params.billing._credit_balance_transaction_retrieve_params import (
        CreditBalanceTransactionRetrieveParams as CreditBalanceTransactionRetrieveParams,
    )
    from stripe.params.billing._credit_grant_create_params import (
        CreditGrantCreateParams as CreditGrantCreateParams,
        CreditGrantCreateParamsAmount as CreditGrantCreateParamsAmount,
        CreditGrantCreateParamsAmountMonetary as CreditGrantCreateParamsAmountMonetary,
        CreditGrantCreateParamsApplicabilityConfig as CreditGrantCreateParamsApplicabilityConfig,
        CreditGrantCreateParamsApplicabilityConfigScope as CreditGrantCreateParamsApplicabilityConfigScope,
        CreditGrantCreateParamsApplicabilityConfigScopePrice as CreditGrantCreateParamsApplicabilityConfigScopePrice,
    )
    from stripe.params.billing._credit_grant_expire_params import (
        CreditGrantExpireParams as CreditGrantExpireParams,
    )
    from stripe.params.billing._credit_grant_list_params import (
        CreditGrantListParams as CreditGrantListParams,
    )
    from stripe.params.billing._credit_grant_modify_params import (
        CreditGrantModifyParams as CreditGrantModifyParams,
    )
    from stripe.params.billing._credit_grant_retrieve_params import (
        CreditGrantRetrieveParams as CreditGrantRetrieveParams,
    )
    from stripe.params.billing._credit_grant_update_params import (
        CreditGrantUpdateParams as CreditGrantUpdateParams,
    )
    from stripe.params.billing._credit_grant_void_grant_params import (
        CreditGrantVoidGrantParams as CreditGrantVoidGrantParams,
    )
    from stripe.params.billing._meter_create_params import (
        MeterCreateParams as MeterCreateParams,
        MeterCreateParamsCustomerMapping as MeterCreateParamsCustomerMapping,
        MeterCreateParamsDefaultAggregation as MeterCreateParamsDefaultAggregation,
        MeterCreateParamsValueSettings as MeterCreateParamsValueSettings,
    )
    from stripe.params.billing._meter_deactivate_params import (
        MeterDeactivateParams as MeterDeactivateParams,
    )
    from stripe.params.billing._meter_event_adjustment_create_params import (
        MeterEventAdjustmentCreateParams as MeterEventAdjustmentCreateParams,
        MeterEventAdjustmentCreateParamsCancel as MeterEventAdjustmentCreateParamsCancel,
    )
    from stripe.params.billing._meter_event_create_params import (
        MeterEventCreateParams as MeterEventCreateParams,
    )
    from stripe.params.billing._meter_event_summary_list_params import (
        MeterEventSummaryListParams as MeterEventSummaryListParams,
    )
    from stripe.params.billing._meter_list_event_summaries_params import (
        MeterListEventSummariesParams as MeterListEventSummariesParams,
    )
    from stripe.params.billing._meter_list_params import (
        MeterListParams as MeterListParams,
    )
    from stripe.params.billing._meter_modify_params import (
        MeterModifyParams as MeterModifyParams,
    )
    from stripe.params.billing._meter_reactivate_params import (
        MeterReactivateParams as MeterReactivateParams,
    )
    from stripe.params.billing._meter_retrieve_params import (
        MeterRetrieveParams as MeterRetrieveParams,
    )
    from stripe.params.billing._meter_update_params import (
        MeterUpdateParams as MeterUpdateParams,
    )

# name -> (import_target, is_submodule)
_import_map = {
    "AlertActivateParams": (
        "stripe.params.billing._alert_activate_params",
        False,
    ),
    "AlertArchiveParams": (
        "stripe.params.billing._alert_archive_params",
        False,
    ),
    "AlertCreateParams": ("stripe.params.billing._alert_create_params", False),
    "AlertCreateParamsUsageThreshold": (
        "stripe.params.billing._alert_create_params",
        False,
    ),
    "AlertCreateParamsUsageThresholdFilter": (
        "stripe.params.billing._alert_create_params",
        False,
    ),
    "AlertDeactivateParams": (
        "stripe.params.billing._alert_deactivate_params",
        False,
    ),
    "AlertListParams": ("stripe.params.billing._alert_list_params", False),
    "AlertRetrieveParams": (
        "stripe.params.billing._alert_retrieve_params",
        False,
    ),
    "CreditBalanceSummaryRetrieveParams": (
        "stripe.params.billing._credit_balance_summary_retrieve_params",
        False,
    ),
    "CreditBalanceSummaryRetrieveParamsFilter": (
        "stripe.params.billing._credit_balance_summary_retrieve_params",
        False,
    ),
    "CreditBalanceSummaryRetrieveParamsFilterApplicabilityScope": (
        "stripe.params.billing._credit_balance_summary_retrieve_params",
        False,
    ),
    "CreditBalanceSummaryRetrieveParamsFilterApplicabilityScopePrice": (
        "stripe.params.billing._credit_balance_summary_retrieve_params",
        False,
    ),
    "CreditBalanceTransactionListParams": (
        "stripe.params.billing._credit_balance_transaction_list_params",
        False,
    ),
    "CreditBalanceTransactionRetrieveParams": (
        "stripe.params.billing._credit_balance_transaction_retrieve_params",
        False,
    ),
    "CreditGrantCreateParams": (
        "stripe.params.billing._credit_grant_create_params",
        False,
    ),
    "CreditGrantCreateParamsAmount": (
        "stripe.params.billing._credit_grant_create_params",
        False,
    ),
    "CreditGrantCreateParamsAmountMonetary": (
        "stripe.params.billing._credit_grant_create_params",
        False,
    ),
    "CreditGrantCreateParamsApplicabilityConfig": (
        "stripe.params.billing._credit_grant_create_params",
        False,
    ),
    "CreditGrantCreateParamsApplicabilityConfigScope": (
        "stripe.params.billing._credit_grant_create_params",
        False,
    ),
    "CreditGrantCreateParamsApplicabilityConfigScopePrice": (
        "stripe.params.billing._credit_grant_create_params",
        False,
    ),
    "CreditGrantExpireParams": (
        "stripe.params.billing._credit_grant_expire_params",
        False,
    ),
    "CreditGrantListParams": (
        "stripe.params.billing._credit_grant_list_params",
        False,
    ),
    "CreditGrantModifyParams": (
        "stripe.params.billing._credit_grant_modify_params",
        False,
    ),
    "CreditGrantRetrieveParams": (
        "stripe.params.billing._credit_grant_retrieve_params",
        False,
    ),
    "CreditGrantUpdateParams": (
        "stripe.params.billing._credit_grant_update_params",
        False,
    ),
    "CreditGrantVoidGrantParams": (
        "stripe.params.billing._credit_grant_void_grant_params",
        False,
    ),
    "MeterCreateParams": ("stripe.params.billing._meter_create_params", False),
    "MeterCreateParamsCustomerMapping": (
        "stripe.params.billing._meter_create_params",
        False,
    ),
    "MeterCreateParamsDefaultAggregation": (
        "stripe.params.billing._meter_create_params",
        False,
    ),
    "MeterCreateParamsValueSettings": (
        "stripe.params.billing._meter_create_params",
        False,
    ),
    "MeterDeactivateParams": (
        "stripe.params.billing._meter_deactivate_params",
        False,
    ),
    "MeterEventAdjustmentCreateParams": (
        "stripe.params.billing._meter_event_adjustment_create_params",
        False,
    ),
    "MeterEventAdjustmentCreateParamsCancel": (
        "stripe.params.billing._meter_event_adjustment_create_params",
        False,
    ),
    "MeterEventCreateParams": (
        "stripe.params.billing._meter_event_create_params",
        False,
    ),
    "MeterEventSummaryListParams": (
        "stripe.params.billing._meter_event_summary_list_params",
        False,
    ),
    "MeterListEventSummariesParams": (
        "stripe.params.billing._meter_list_event_summaries_params",
        False,
    ),
    "MeterListParams": ("stripe.params.billing._meter_list_params", False),
    "MeterModifyParams": ("stripe.params.billing._meter_modify_params", False),
    "MeterReactivateParams": (
        "stripe.params.billing._meter_reactivate_params",
        False,
    ),
    "MeterRetrieveParams": (
        "stripe.params.billing._meter_retrieve_params",
        False,
    ),
    "MeterUpdateParams": ("stripe.params.billing._meter_update_params", False),
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
