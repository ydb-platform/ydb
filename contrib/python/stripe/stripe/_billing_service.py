# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.billing._alert_service import AlertService
    from stripe.billing._credit_balance_summary_service import (
        CreditBalanceSummaryService,
    )
    from stripe.billing._credit_balance_transaction_service import (
        CreditBalanceTransactionService,
    )
    from stripe.billing._credit_grant_service import CreditGrantService
    from stripe.billing._meter_event_adjustment_service import (
        MeterEventAdjustmentService,
    )
    from stripe.billing._meter_event_service import MeterEventService
    from stripe.billing._meter_service import MeterService

_subservices = {
    "alerts": ["stripe.billing._alert_service", "AlertService"],
    "credit_balance_summary": [
        "stripe.billing._credit_balance_summary_service",
        "CreditBalanceSummaryService",
    ],
    "credit_balance_transactions": [
        "stripe.billing._credit_balance_transaction_service",
        "CreditBalanceTransactionService",
    ],
    "credit_grants": [
        "stripe.billing._credit_grant_service",
        "CreditGrantService",
    ],
    "meters": ["stripe.billing._meter_service", "MeterService"],
    "meter_events": [
        "stripe.billing._meter_event_service",
        "MeterEventService",
    ],
    "meter_event_adjustments": [
        "stripe.billing._meter_event_adjustment_service",
        "MeterEventAdjustmentService",
    ],
}


class BillingService(StripeService):
    alerts: "AlertService"
    credit_balance_summary: "CreditBalanceSummaryService"
    credit_balance_transactions: "CreditBalanceTransactionService"
    credit_grants: "CreditGrantService"
    meters: "MeterService"
    meter_events: "MeterEventService"
    meter_event_adjustments: "MeterEventAdjustmentService"

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
