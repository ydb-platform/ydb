# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.tax._association_service import AssociationService
    from stripe.tax._calculation_service import CalculationService
    from stripe.tax._registration_service import RegistrationService
    from stripe.tax._settings_service import SettingsService
    from stripe.tax._transaction_service import TransactionService

_subservices = {
    "associations": ["stripe.tax._association_service", "AssociationService"],
    "calculations": ["stripe.tax._calculation_service", "CalculationService"],
    "registrations": [
        "stripe.tax._registration_service",
        "RegistrationService",
    ],
    "settings": ["stripe.tax._settings_service", "SettingsService"],
    "transactions": ["stripe.tax._transaction_service", "TransactionService"],
}


class TaxService(StripeService):
    associations: "AssociationService"
    calculations: "CalculationService"
    registrations: "RegistrationService"
    settings: "SettingsService"
    transactions: "TransactionService"

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
