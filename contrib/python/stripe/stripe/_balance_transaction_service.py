# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._balance_transaction import BalanceTransaction
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.params._balance_transaction_list_params import (
        BalanceTransactionListParams,
    )
    from stripe.params._balance_transaction_retrieve_params import (
        BalanceTransactionRetrieveParams,
    )


class BalanceTransactionService(StripeService):
    def list(
        self,
        params: Optional["BalanceTransactionListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[BalanceTransaction]":
        """
        Returns a list of transactions that have contributed to the Stripe account balance (e.g., charges, transfers, and so forth). The transactions are returned in sorted order, with the most recent transactions appearing first.

        Note that this endpoint was previously called “Balance history” and used the path /v1/balance/history.
        """
        return cast(
            "ListObject[BalanceTransaction]",
            self._request(
                "get",
                "/v1/balance_transactions",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["BalanceTransactionListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[BalanceTransaction]":
        """
        Returns a list of transactions that have contributed to the Stripe account balance (e.g., charges, transfers, and so forth). The transactions are returned in sorted order, with the most recent transactions appearing first.

        Note that this endpoint was previously called “Balance history” and used the path /v1/balance/history.
        """
        return cast(
            "ListObject[BalanceTransaction]",
            await self._request_async(
                "get",
                "/v1/balance_transactions",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        id: str,
        params: Optional["BalanceTransactionRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "BalanceTransaction":
        """
        Retrieves the balance transaction with the given ID.

        Note that this endpoint previously used the path /v1/balance/history/:id.
        """
        return cast(
            "BalanceTransaction",
            self._request(
                "get",
                "/v1/balance_transactions/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        id: str,
        params: Optional["BalanceTransactionRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "BalanceTransaction":
        """
        Retrieves the balance transaction with the given ID.

        Note that this endpoint previously used the path /v1/balance/history/:id.
        """
        return cast(
            "BalanceTransaction",
            await self._request_async(
                "get",
                "/v1/balance_transactions/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )
