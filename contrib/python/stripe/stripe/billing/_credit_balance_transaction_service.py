# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.billing._credit_balance_transaction import (
        CreditBalanceTransaction,
    )
    from stripe.params.billing._credit_balance_transaction_list_params import (
        CreditBalanceTransactionListParams,
    )
    from stripe.params.billing._credit_balance_transaction_retrieve_params import (
        CreditBalanceTransactionRetrieveParams,
    )


class CreditBalanceTransactionService(StripeService):
    def list(
        self,
        params: Optional["CreditBalanceTransactionListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[CreditBalanceTransaction]":
        """
        Retrieve a list of credit balance transactions.
        """
        return cast(
            "ListObject[CreditBalanceTransaction]",
            self._request(
                "get",
                "/v1/billing/credit_balance_transactions",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["CreditBalanceTransactionListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[CreditBalanceTransaction]":
        """
        Retrieve a list of credit balance transactions.
        """
        return cast(
            "ListObject[CreditBalanceTransaction]",
            await self._request_async(
                "get",
                "/v1/billing/credit_balance_transactions",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        id: str,
        params: Optional["CreditBalanceTransactionRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "CreditBalanceTransaction":
        """
        Retrieves a credit balance transaction.
        """
        return cast(
            "CreditBalanceTransaction",
            self._request(
                "get",
                "/v1/billing/credit_balance_transactions/{id}".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        id: str,
        params: Optional["CreditBalanceTransactionRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "CreditBalanceTransaction":
        """
        Retrieves a credit balance transaction.
        """
        return cast(
            "CreditBalanceTransaction",
            await self._request_async(
                "get",
                "/v1/billing/credit_balance_transactions/{id}".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
