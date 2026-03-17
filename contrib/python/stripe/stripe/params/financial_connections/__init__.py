# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params.financial_connections._account_disconnect_params import (
        AccountDisconnectParams as AccountDisconnectParams,
    )
    from stripe.params.financial_connections._account_list_owners_params import (
        AccountListOwnersParams as AccountListOwnersParams,
    )
    from stripe.params.financial_connections._account_list_params import (
        AccountListParams as AccountListParams,
        AccountListParamsAccountHolder as AccountListParamsAccountHolder,
    )
    from stripe.params.financial_connections._account_owner_list_params import (
        AccountOwnerListParams as AccountOwnerListParams,
    )
    from stripe.params.financial_connections._account_refresh_account_params import (
        AccountRefreshAccountParams as AccountRefreshAccountParams,
    )
    from stripe.params.financial_connections._account_refresh_params import (
        AccountRefreshParams as AccountRefreshParams,
    )
    from stripe.params.financial_connections._account_retrieve_params import (
        AccountRetrieveParams as AccountRetrieveParams,
    )
    from stripe.params.financial_connections._account_subscribe_params import (
        AccountSubscribeParams as AccountSubscribeParams,
    )
    from stripe.params.financial_connections._account_unsubscribe_params import (
        AccountUnsubscribeParams as AccountUnsubscribeParams,
    )
    from stripe.params.financial_connections._session_create_params import (
        SessionCreateParams as SessionCreateParams,
        SessionCreateParamsAccountHolder as SessionCreateParamsAccountHolder,
        SessionCreateParamsFilters as SessionCreateParamsFilters,
    )
    from stripe.params.financial_connections._session_retrieve_params import (
        SessionRetrieveParams as SessionRetrieveParams,
    )
    from stripe.params.financial_connections._transaction_list_params import (
        TransactionListParams as TransactionListParams,
        TransactionListParamsTransactedAt as TransactionListParamsTransactedAt,
        TransactionListParamsTransactionRefresh as TransactionListParamsTransactionRefresh,
    )
    from stripe.params.financial_connections._transaction_retrieve_params import (
        TransactionRetrieveParams as TransactionRetrieveParams,
    )

# name -> (import_target, is_submodule)
_import_map = {
    "AccountDisconnectParams": (
        "stripe.params.financial_connections._account_disconnect_params",
        False,
    ),
    "AccountListOwnersParams": (
        "stripe.params.financial_connections._account_list_owners_params",
        False,
    ),
    "AccountListParams": (
        "stripe.params.financial_connections._account_list_params",
        False,
    ),
    "AccountListParamsAccountHolder": (
        "stripe.params.financial_connections._account_list_params",
        False,
    ),
    "AccountOwnerListParams": (
        "stripe.params.financial_connections._account_owner_list_params",
        False,
    ),
    "AccountRefreshAccountParams": (
        "stripe.params.financial_connections._account_refresh_account_params",
        False,
    ),
    "AccountRefreshParams": (
        "stripe.params.financial_connections._account_refresh_params",
        False,
    ),
    "AccountRetrieveParams": (
        "stripe.params.financial_connections._account_retrieve_params",
        False,
    ),
    "AccountSubscribeParams": (
        "stripe.params.financial_connections._account_subscribe_params",
        False,
    ),
    "AccountUnsubscribeParams": (
        "stripe.params.financial_connections._account_unsubscribe_params",
        False,
    ),
    "SessionCreateParams": (
        "stripe.params.financial_connections._session_create_params",
        False,
    ),
    "SessionCreateParamsAccountHolder": (
        "stripe.params.financial_connections._session_create_params",
        False,
    ),
    "SessionCreateParamsFilters": (
        "stripe.params.financial_connections._session_create_params",
        False,
    ),
    "SessionRetrieveParams": (
        "stripe.params.financial_connections._session_retrieve_params",
        False,
    ),
    "TransactionListParams": (
        "stripe.params.financial_connections._transaction_list_params",
        False,
    ),
    "TransactionListParamsTransactedAt": (
        "stripe.params.financial_connections._transaction_list_params",
        False,
    ),
    "TransactionListParamsTransactionRefresh": (
        "stripe.params.financial_connections._transaction_list_params",
        False,
    ),
    "TransactionRetrieveParams": (
        "stripe.params.financial_connections._transaction_retrieve_params",
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
