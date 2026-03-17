# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.billing._alert import Alert as Alert
    from stripe.billing._alert_service import AlertService as AlertService
    from stripe.billing._alert_triggered import (
        AlertTriggered as AlertTriggered,
    )
    from stripe.billing._credit_balance_summary import (
        CreditBalanceSummary as CreditBalanceSummary,
    )
    from stripe.billing._credit_balance_summary_service import (
        CreditBalanceSummaryService as CreditBalanceSummaryService,
    )
    from stripe.billing._credit_balance_transaction import (
        CreditBalanceTransaction as CreditBalanceTransaction,
    )
    from stripe.billing._credit_balance_transaction_service import (
        CreditBalanceTransactionService as CreditBalanceTransactionService,
    )
    from stripe.billing._credit_grant import CreditGrant as CreditGrant
    from stripe.billing._credit_grant_service import (
        CreditGrantService as CreditGrantService,
    )
    from stripe.billing._meter import Meter as Meter
    from stripe.billing._meter_event import MeterEvent as MeterEvent
    from stripe.billing._meter_event_adjustment import (
        MeterEventAdjustment as MeterEventAdjustment,
    )
    from stripe.billing._meter_event_adjustment_service import (
        MeterEventAdjustmentService as MeterEventAdjustmentService,
    )
    from stripe.billing._meter_event_service import (
        MeterEventService as MeterEventService,
    )
    from stripe.billing._meter_event_summary import (
        MeterEventSummary as MeterEventSummary,
    )
    from stripe.billing._meter_event_summary_service import (
        MeterEventSummaryService as MeterEventSummaryService,
    )
    from stripe.billing._meter_service import MeterService as MeterService

# name -> (import_target, is_submodule)
_import_map = {
    "Alert": ("stripe.billing._alert", False),
    "AlertService": ("stripe.billing._alert_service", False),
    "AlertTriggered": ("stripe.billing._alert_triggered", False),
    "CreditBalanceSummary": ("stripe.billing._credit_balance_summary", False),
    "CreditBalanceSummaryService": (
        "stripe.billing._credit_balance_summary_service",
        False,
    ),
    "CreditBalanceTransaction": (
        "stripe.billing._credit_balance_transaction",
        False,
    ),
    "CreditBalanceTransactionService": (
        "stripe.billing._credit_balance_transaction_service",
        False,
    ),
    "CreditGrant": ("stripe.billing._credit_grant", False),
    "CreditGrantService": ("stripe.billing._credit_grant_service", False),
    "Meter": ("stripe.billing._meter", False),
    "MeterEvent": ("stripe.billing._meter_event", False),
    "MeterEventAdjustment": ("stripe.billing._meter_event_adjustment", False),
    "MeterEventAdjustmentService": (
        "stripe.billing._meter_event_adjustment_service",
        False,
    ),
    "MeterEventService": ("stripe.billing._meter_event_service", False),
    "MeterEventSummary": ("stripe.billing._meter_event_summary", False),
    "MeterEventSummaryService": (
        "stripe.billing._meter_event_summary_service",
        False,
    ),
    "MeterService": ("stripe.billing._meter_service", False),
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
