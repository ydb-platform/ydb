# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.radar._early_fraud_warning_service import (
        EarlyFraudWarningService,
    )
    from stripe.radar._payment_evaluation_service import (
        PaymentEvaluationService,
    )
    from stripe.radar._value_list_item_service import ValueListItemService
    from stripe.radar._value_list_service import ValueListService

_subservices = {
    "early_fraud_warnings": [
        "stripe.radar._early_fraud_warning_service",
        "EarlyFraudWarningService",
    ],
    "payment_evaluations": [
        "stripe.radar._payment_evaluation_service",
        "PaymentEvaluationService",
    ],
    "value_lists": ["stripe.radar._value_list_service", "ValueListService"],
    "value_list_items": [
        "stripe.radar._value_list_item_service",
        "ValueListItemService",
    ],
}


class RadarService(StripeService):
    early_fraud_warnings: "EarlyFraudWarningService"
    payment_evaluations: "PaymentEvaluationService"
    value_lists: "ValueListService"
    value_list_items: "ValueListItemService"

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
