# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.issuing._transaction import Transaction
    from stripe.params.issuing._transaction_list_params import (
        TransactionListParams,
    )
    from stripe.params.issuing._transaction_retrieve_params import (
        TransactionRetrieveParams,
    )
    from stripe.params.issuing._transaction_update_params import (
        TransactionUpdateParams,
    )


class TransactionService(StripeService):
    def list(
        self,
        params: Optional["TransactionListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Transaction]":
        """
        Returns a list of Issuing Transaction objects. The objects are sorted in descending order by creation date, with the most recently created object appearing first.
        """
        return cast(
            "ListObject[Transaction]",
            self._request(
                "get",
                "/v1/issuing/transactions",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["TransactionListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Transaction]":
        """
        Returns a list of Issuing Transaction objects. The objects are sorted in descending order by creation date, with the most recently created object appearing first.
        """
        return cast(
            "ListObject[Transaction]",
            await self._request_async(
                "get",
                "/v1/issuing/transactions",
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
        Retrieves an Issuing Transaction object.
        """
        return cast(
            "Transaction",
            self._request(
                "get",
                "/v1/issuing/transactions/{transaction}".format(
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
        Retrieves an Issuing Transaction object.
        """
        return cast(
            "Transaction",
            await self._request_async(
                "get",
                "/v1/issuing/transactions/{transaction}".format(
                    transaction=sanitize_id(transaction),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        transaction: str,
        params: Optional["TransactionUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Transaction":
        """
        Updates the specified Issuing Transaction object by setting the values of the parameters passed. Any parameters not provided will be left unchanged.
        """
        return cast(
            "Transaction",
            self._request(
                "post",
                "/v1/issuing/transactions/{transaction}".format(
                    transaction=sanitize_id(transaction),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        transaction: str,
        params: Optional["TransactionUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Transaction":
        """
        Updates the specified Issuing Transaction object by setting the values of the parameters passed. Any parameters not provided will be left unchanged.
        """
        return cast(
            "Transaction",
            await self._request_async(
                "post",
                "/v1/issuing/transactions/{transaction}".format(
                    transaction=sanitize_id(transaction),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
