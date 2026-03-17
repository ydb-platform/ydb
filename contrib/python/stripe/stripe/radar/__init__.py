# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.radar._early_fraud_warning import (
        EarlyFraudWarning as EarlyFraudWarning,
    )
    from stripe.radar._early_fraud_warning_service import (
        EarlyFraudWarningService as EarlyFraudWarningService,
    )
    from stripe.radar._payment_evaluation import (
        PaymentEvaluation as PaymentEvaluation,
    )
    from stripe.radar._payment_evaluation_service import (
        PaymentEvaluationService as PaymentEvaluationService,
    )
    from stripe.radar._value_list import ValueList as ValueList
    from stripe.radar._value_list_item import ValueListItem as ValueListItem
    from stripe.radar._value_list_item_service import (
        ValueListItemService as ValueListItemService,
    )
    from stripe.radar._value_list_service import (
        ValueListService as ValueListService,
    )

# name -> (import_target, is_submodule)
_import_map = {
    "EarlyFraudWarning": ("stripe.radar._early_fraud_warning", False),
    "EarlyFraudWarningService": (
        "stripe.radar._early_fraud_warning_service",
        False,
    ),
    "PaymentEvaluation": ("stripe.radar._payment_evaluation", False),
    "PaymentEvaluationService": (
        "stripe.radar._payment_evaluation_service",
        False,
    ),
    "ValueList": ("stripe.radar._value_list", False),
    "ValueListItem": ("stripe.radar._value_list_item", False),
    "ValueListItemService": ("stripe.radar._value_list_item_service", False),
    "ValueListService": ("stripe.radar._value_list_service", False),
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
