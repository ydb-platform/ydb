# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe._reversal import Reversal
    from stripe.params._transfer_reversal_create_params import (
        TransferReversalCreateParams,
    )
    from stripe.params._transfer_reversal_list_params import (
        TransferReversalListParams,
    )
    from stripe.params._transfer_reversal_retrieve_params import (
        TransferReversalRetrieveParams,
    )
    from stripe.params._transfer_reversal_update_params import (
        TransferReversalUpdateParams,
    )


class TransferReversalService(StripeService):
    def list(
        self,
        id: str,
        params: Optional["TransferReversalListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Reversal]":
        """
        You can see a list of the reversals belonging to a specific transfer. Note that the 10 most recent reversals are always available by default on the transfer object. If you need more than those 10, you can use this API method and the limit and starting_after parameters to page through additional reversals.
        """
        return cast(
            "ListObject[Reversal]",
            self._request(
                "get",
                "/v1/transfers/{id}/reversals".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        id: str,
        params: Optional["TransferReversalListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Reversal]":
        """
        You can see a list of the reversals belonging to a specific transfer. Note that the 10 most recent reversals are always available by default on the transfer object. If you need more than those 10, you can use this API method and the limit and starting_after parameters to page through additional reversals.
        """
        return cast(
            "ListObject[Reversal]",
            await self._request_async(
                "get",
                "/v1/transfers/{id}/reversals".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        id: str,
        params: Optional["TransferReversalCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Reversal":
        """
        When you create a new reversal, you must specify a transfer to create it on.

        When reversing transfers, you can optionally reverse part of the transfer. You can do so as many times as you wish until the entire transfer has been reversed.

        Once entirely reversed, a transfer can't be reversed again. This method will return an error when called on an already-reversed transfer, or when trying to reverse more money than is left on a transfer.
        """
        return cast(
            "Reversal",
            self._request(
                "post",
                "/v1/transfers/{id}/reversals".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        id: str,
        params: Optional["TransferReversalCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Reversal":
        """
        When you create a new reversal, you must specify a transfer to create it on.

        When reversing transfers, you can optionally reverse part of the transfer. You can do so as many times as you wish until the entire transfer has been reversed.

        Once entirely reversed, a transfer can't be reversed again. This method will return an error when called on an already-reversed transfer, or when trying to reverse more money than is left on a transfer.
        """
        return cast(
            "Reversal",
            await self._request_async(
                "post",
                "/v1/transfers/{id}/reversals".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        transfer: str,
        id: str,
        params: Optional["TransferReversalRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Reversal":
        """
        By default, you can see the 10 most recent reversals stored directly on the transfer object, but you can also retrieve details about a specific reversal stored on the transfer.
        """
        return cast(
            "Reversal",
            self._request(
                "get",
                "/v1/transfers/{transfer}/reversals/{id}".format(
                    transfer=sanitize_id(transfer),
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        transfer: str,
        id: str,
        params: Optional["TransferReversalRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Reversal":
        """
        By default, you can see the 10 most recent reversals stored directly on the transfer object, but you can also retrieve details about a specific reversal stored on the transfer.
        """
        return cast(
            "Reversal",
            await self._request_async(
                "get",
                "/v1/transfers/{transfer}/reversals/{id}".format(
                    transfer=sanitize_id(transfer),
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        transfer: str,
        id: str,
        params: Optional["TransferReversalUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Reversal":
        """
        Updates the specified reversal by setting the values of the parameters passed. Any parameters not provided will be left unchanged.

        This request only accepts metadata and description as arguments.
        """
        return cast(
            "Reversal",
            self._request(
                "post",
                "/v1/transfers/{transfer}/reversals/{id}".format(
                    transfer=sanitize_id(transfer),
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        transfer: str,
        id: str,
        params: Optional["TransferReversalUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Reversal":
        """
        Updates the specified reversal by setting the values of the parameters passed. Any parameters not provided will be left unchanged.

        This request only accepts metadata and description as arguments.
        """
        return cast(
            "Reversal",
            await self._request_async(
                "post",
                "/v1/transfers/{transfer}/reversals/{id}".format(
                    transfer=sanitize_id(transfer),
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
