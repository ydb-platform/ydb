# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.tax._association import Association as Association
    from stripe.tax._association_service import (
        AssociationService as AssociationService,
    )
    from stripe.tax._calculation import Calculation as Calculation
    from stripe.tax._calculation_line_item import (
        CalculationLineItem as CalculationLineItem,
    )
    from stripe.tax._calculation_line_item_service import (
        CalculationLineItemService as CalculationLineItemService,
    )
    from stripe.tax._calculation_service import (
        CalculationService as CalculationService,
    )
    from stripe.tax._registration import Registration as Registration
    from stripe.tax._registration_service import (
        RegistrationService as RegistrationService,
    )
    from stripe.tax._settings import Settings as Settings
    from stripe.tax._settings_service import SettingsService as SettingsService
    from stripe.tax._transaction import Transaction as Transaction
    from stripe.tax._transaction_line_item import (
        TransactionLineItem as TransactionLineItem,
    )
    from stripe.tax._transaction_line_item_service import (
        TransactionLineItemService as TransactionLineItemService,
    )
    from stripe.tax._transaction_service import (
        TransactionService as TransactionService,
    )

# name -> (import_target, is_submodule)
_import_map = {
    "Association": ("stripe.tax._association", False),
    "AssociationService": ("stripe.tax._association_service", False),
    "Calculation": ("stripe.tax._calculation", False),
    "CalculationLineItem": ("stripe.tax._calculation_line_item", False),
    "CalculationLineItemService": (
        "stripe.tax._calculation_line_item_service",
        False,
    ),
    "CalculationService": ("stripe.tax._calculation_service", False),
    "Registration": ("stripe.tax._registration", False),
    "RegistrationService": ("stripe.tax._registration_service", False),
    "Settings": ("stripe.tax._settings", False),
    "SettingsService": ("stripe.tax._settings_service", False),
    "Transaction": ("stripe.tax._transaction", False),
    "TransactionLineItem": ("stripe.tax._transaction_line_item", False),
    "TransactionLineItemService": (
        "stripe.tax._transaction_line_item_service",
        False,
    ),
    "TransactionService": ("stripe.tax._transaction_service", False),
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
