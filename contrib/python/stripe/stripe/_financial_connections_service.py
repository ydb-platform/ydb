# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.financial_connections._account_service import AccountService
    from stripe.financial_connections._session_service import SessionService
    from stripe.financial_connections._transaction_service import (
        TransactionService,
    )

_subservices = {
    "accounts": [
        "stripe.financial_connections._account_service",
        "AccountService",
    ],
    "sessions": [
        "stripe.financial_connections._session_service",
        "SessionService",
    ],
    "transactions": [
        "stripe.financial_connections._transaction_service",
        "TransactionService",
    ],
}


class FinancialConnectionsService(StripeService):
    accounts: "AccountService"
    sessions: "SessionService"
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
