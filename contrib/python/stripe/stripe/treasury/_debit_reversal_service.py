# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.params.treasury._debit_reversal_create_params import (
        DebitReversalCreateParams,
    )
    from stripe.params.treasury._debit_reversal_list_params import (
        DebitReversalListParams,
    )
    from stripe.params.treasury._debit_reversal_retrieve_params import (
        DebitReversalRetrieveParams,
    )
    from stripe.treasury._debit_reversal import DebitReversal


class DebitReversalService(StripeService):
    def list(
        self,
        params: "DebitReversalListParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[DebitReversal]":
        """
        Returns a list of DebitReversals.
        """
        return cast(
            "ListObject[DebitReversal]",
            self._request(
                "get",
                "/v1/treasury/debit_reversals",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: "DebitReversalListParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[DebitReversal]":
        """
        Returns a list of DebitReversals.
        """
        return cast(
            "ListObject[DebitReversal]",
            await self._request_async(
                "get",
                "/v1/treasury/debit_reversals",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: "DebitReversalCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "DebitReversal":
        """
        Reverses a ReceivedDebit and creates a DebitReversal object.
        """
        return cast(
            "DebitReversal",
            self._request(
                "post",
                "/v1/treasury/debit_reversals",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "DebitReversalCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "DebitReversal":
        """
        Reverses a ReceivedDebit and creates a DebitReversal object.
        """
        return cast(
            "DebitReversal",
            await self._request_async(
                "post",
                "/v1/treasury/debit_reversals",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        debit_reversal: str,
        params: Optional["DebitReversalRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "DebitReversal":
        """
        Retrieves a DebitReversal object.
        """
        return cast(
            "DebitReversal",
            self._request(
                "get",
                "/v1/treasury/debit_reversals/{debit_reversal}".format(
                    debit_reversal=sanitize_id(debit_reversal),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        debit_reversal: str,
        params: Optional["DebitReversalRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "DebitReversal":
        """
        Retrieves a DebitReversal object.
        """
        return cast(
            "DebitReversal",
            await self._request_async(
                "get",
                "/v1/treasury/debit_reversals/{debit_reversal}".format(
                    debit_reversal=sanitize_id(debit_reversal),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
