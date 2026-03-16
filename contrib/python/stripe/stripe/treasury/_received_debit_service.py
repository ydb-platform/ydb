# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.params.treasury._received_debit_list_params import (
        ReceivedDebitListParams,
    )
    from stripe.params.treasury._received_debit_retrieve_params import (
        ReceivedDebitRetrieveParams,
    )
    from stripe.treasury._received_debit import ReceivedDebit


class ReceivedDebitService(StripeService):
    def list(
        self,
        params: "ReceivedDebitListParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[ReceivedDebit]":
        """
        Returns a list of ReceivedDebits.
        """
        return cast(
            "ListObject[ReceivedDebit]",
            self._request(
                "get",
                "/v1/treasury/received_debits",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: "ReceivedDebitListParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[ReceivedDebit]":
        """
        Returns a list of ReceivedDebits.
        """
        return cast(
            "ListObject[ReceivedDebit]",
            await self._request_async(
                "get",
                "/v1/treasury/received_debits",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        id: str,
        params: Optional["ReceivedDebitRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ReceivedDebit":
        """
        Retrieves the details of an existing ReceivedDebit by passing the unique ReceivedDebit ID from the ReceivedDebit list
        """
        return cast(
            "ReceivedDebit",
            self._request(
                "get",
                "/v1/treasury/received_debits/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        id: str,
        params: Optional["ReceivedDebitRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ReceivedDebit":
        """
        Retrieves the details of an existing ReceivedDebit by passing the unique ReceivedDebit ID from the ReceivedDebit list
        """
        return cast(
            "ReceivedDebit",
            await self._request_async(
                "get",
                "/v1/treasury/received_debits/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )
