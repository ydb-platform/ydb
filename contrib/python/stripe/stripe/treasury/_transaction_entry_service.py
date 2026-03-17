# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.params.treasury._transaction_entry_list_params import (
        TransactionEntryListParams,
    )
    from stripe.params.treasury._transaction_entry_retrieve_params import (
        TransactionEntryRetrieveParams,
    )
    from stripe.treasury._transaction_entry import TransactionEntry


class TransactionEntryService(StripeService):
    def list(
        self,
        params: "TransactionEntryListParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[TransactionEntry]":
        """
        Retrieves a list of TransactionEntry objects.
        """
        return cast(
            "ListObject[TransactionEntry]",
            self._request(
                "get",
                "/v1/treasury/transaction_entries",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: "TransactionEntryListParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[TransactionEntry]":
        """
        Retrieves a list of TransactionEntry objects.
        """
        return cast(
            "ListObject[TransactionEntry]",
            await self._request_async(
                "get",
                "/v1/treasury/transaction_entries",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        id: str,
        params: Optional["TransactionEntryRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "TransactionEntry":
        """
        Retrieves a TransactionEntry object.
        """
        return cast(
            "TransactionEntry",
            self._request(
                "get",
                "/v1/treasury/transaction_entries/{id}".format(
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
        params: Optional["TransactionEntryRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "TransactionEntry":
        """
        Retrieves a TransactionEntry object.
        """
        return cast(
            "TransactionEntry",
            await self._request_async(
                "get",
                "/v1/treasury/transaction_entries/{id}".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
