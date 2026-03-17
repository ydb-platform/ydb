# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.financial_connections._transaction import Transaction
    from stripe.params.financial_connections._transaction_list_params import (
        TransactionListParams,
    )
    from stripe.params.financial_connections._transaction_retrieve_params import (
        TransactionRetrieveParams,
    )


class TransactionService(StripeService):
    def list(
        self,
        params: "TransactionListParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Transaction]":
        """
        Returns a list of Financial Connections Transaction objects.
        """
        return cast(
            "ListObject[Transaction]",
            self._request(
                "get",
                "/v1/financial_connections/transactions",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: "TransactionListParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Transaction]":
        """
        Returns a list of Financial Connections Transaction objects.
        """
        return cast(
            "ListObject[Transaction]",
            await self._request_async(
                "get",
                "/v1/financial_connections/transactions",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        transaction: str,
        params: Optional["TransactionRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Transaction":
        """
        Retrieves the details of a Financial Connections Transaction
        """
        return cast(
            "Transaction",
            self._request(
                "get",
                "/v1/financial_connections/transactions/{transaction}".format(
                    transaction=sanitize_id(transaction),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        transaction: str,
        params: Optional["TransactionRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Transaction":
        """
        Retrieves the details of a Financial Connections Transaction
        """
        return cast(
            "Transaction",
            await self._request_async(
                "get",
                "/v1/financial_connections/transactions/{transaction}".format(
                    transaction=sanitize_id(transaction),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
