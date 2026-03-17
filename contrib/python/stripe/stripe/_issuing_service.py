# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.issuing._authorization_service import AuthorizationService
    from stripe.issuing._card_service import CardService
    from stripe.issuing._cardholder_service import CardholderService
    from stripe.issuing._dispute_service import DisputeService
    from stripe.issuing._personalization_design_service import (
        PersonalizationDesignService,
    )
    from stripe.issuing._physical_bundle_service import PhysicalBundleService
    from stripe.issuing._token_service import TokenService
    from stripe.issuing._transaction_service import TransactionService

_subservices = {
    "authorizations": [
        "stripe.issuing._authorization_service",
        "AuthorizationService",
    ],
    "cards": ["stripe.issuing._card_service", "CardService"],
    "cardholders": ["stripe.issuing._cardholder_service", "CardholderService"],
    "disputes": ["stripe.issuing._dispute_service", "DisputeService"],
    "personalization_designs": [
        "stripe.issuing._personalization_design_service",
        "PersonalizationDesignService",
    ],
    "physical_bundles": [
        "stripe.issuing._physical_bundle_service",
        "PhysicalBundleService",
    ],
    "tokens": ["stripe.issuing._token_service", "TokenService"],
    "transactions": [
        "stripe.issuing._transaction_service",
        "TransactionService",
    ],
}


class IssuingService(StripeService):
    authorizations: "AuthorizationService"
    cards: "CardService"
    cardholders: "CardholderService"
    disputes: "DisputeService"
    personalization_designs: "PersonalizationDesignService"
    physical_bundles: "PhysicalBundleService"
    tokens: "TokenService"
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
