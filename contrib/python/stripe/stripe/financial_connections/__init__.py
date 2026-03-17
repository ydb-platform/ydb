# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.financial_connections._account import Account as Account
    from stripe.financial_connections._account_owner import (
        AccountOwner as AccountOwner,
    )
    from stripe.financial_connections._account_owner_service import (
        AccountOwnerService as AccountOwnerService,
    )
    from stripe.financial_connections._account_ownership import (
        AccountOwnership as AccountOwnership,
    )
    from stripe.financial_connections._account_service import (
        AccountService as AccountService,
    )
    from stripe.financial_connections._session import Session as Session
    from stripe.financial_connections._session_service import (
        SessionService as SessionService,
    )
    from stripe.financial_connections._transaction import (
        Transaction as Transaction,
    )
    from stripe.financial_connections._transaction_service import (
        TransactionService as TransactionService,
    )

# name -> (import_target, is_submodule)
_import_map = {
    "Account": ("stripe.financial_connections._account", False),
    "AccountOwner": ("stripe.financial_connections._account_owner", False),
    "AccountOwnerService": (
        "stripe.financial_connections._account_owner_service",
        False,
    ),
    "AccountOwnership": (
        "stripe.financial_connections._account_ownership",
        False,
    ),
    "AccountService": ("stripe.financial_connections._account_service", False),
    "Session": ("stripe.financial_connections._session", False),
    "SessionService": ("stripe.financial_connections._session_service", False),
    "Transaction": ("stripe.financial_connections._transaction", False),
    "TransactionService": (
        "stripe.financial_connections._transaction_service",
        False,
    ),
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
