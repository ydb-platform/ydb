# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.test_helpers.issuing._authorization_service import (
        AuthorizationService,
    )
    from stripe.test_helpers.issuing._card_service import CardService
    from stripe.test_helpers.issuing._personalization_design_service import (
        PersonalizationDesignService,
    )
    from stripe.test_helpers.issuing._transaction_service import (
        TransactionService,
    )

_subservices = {
    "authorizations": [
        "stripe.test_helpers.issuing._authorization_service",
        "AuthorizationService",
    ],
    "cards": ["stripe.test_helpers.issuing._card_service", "CardService"],
    "personalization_designs": [
        "stripe.test_helpers.issuing._personalization_design_service",
        "PersonalizationDesignService",
    ],
    "transactions": [
        "stripe.test_helpers.issuing._transaction_service",
        "TransactionService",
    ],
}


class IssuingService(StripeService):
    authorizations: "AuthorizationService"
    cards: "CardService"
    personalization_designs: "PersonalizationDesignService"
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
