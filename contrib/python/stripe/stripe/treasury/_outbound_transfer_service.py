# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.params.treasury._outbound_transfer_cancel_params import (
        OutboundTransferCancelParams,
    )
    from stripe.params.treasury._outbound_transfer_create_params import (
        OutboundTransferCreateParams,
    )
    from stripe.params.treasury._outbound_transfer_list_params import (
        OutboundTransferListParams,
    )
    from stripe.params.treasury._outbound_transfer_retrieve_params import (
        OutboundTransferRetrieveParams,
    )
    from stripe.treasury._outbound_transfer import OutboundTransfer


class OutboundTransferService(StripeService):
    def list(
        self,
        params: "OutboundTransferListParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[OutboundTransfer]":
        """
        Returns a list of OutboundTransfers sent from the specified FinancialAccount.
        """
        return cast(
            "ListObject[OutboundTransfer]",
            self._request(
                "get",
                "/v1/treasury/outbound_transfers",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: "OutboundTransferListParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[OutboundTransfer]":
        """
        Returns a list of OutboundTransfers sent from the specified FinancialAccount.
        """
        return cast(
            "ListObject[OutboundTransfer]",
            await self._request_async(
                "get",
                "/v1/treasury/outbound_transfers",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: "OutboundTransferCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "OutboundTransfer":
        """
        Creates an OutboundTransfer.
        """
        return cast(
            "OutboundTransfer",
            self._request(
                "post",
                "/v1/treasury/outbound_transfers",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "OutboundTransferCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "OutboundTransfer":
        """
        Creates an OutboundTransfer.
        """
        return cast(
            "OutboundTransfer",
            await self._request_async(
                "post",
                "/v1/treasury/outbound_transfers",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        outbound_transfer: str,
        params: Optional["OutboundTransferRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "OutboundTransfer":
        """
        Retrieves the details of an existing OutboundTransfer by passing the unique OutboundTransfer ID from either the OutboundTransfer creation request or OutboundTransfer list.
        """
        return cast(
            "OutboundTransfer",
            self._request(
                "get",
                "/v1/treasury/outbound_transfers/{outbound_transfer}".format(
                    outbound_transfer=sanitize_id(outbound_transfer),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        outbound_transfer: str,
        params: Optional["OutboundTransferRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "OutboundTransfer":
        """
        Retrieves the details of an existing OutboundTransfer by passing the unique OutboundTransfer ID from either the OutboundTransfer creation request or OutboundTransfer list.
        """
        return cast(
            "OutboundTransfer",
            await self._request_async(
                "get",
                "/v1/treasury/outbound_transfers/{outbound_transfer}".format(
                    outbound_transfer=sanitize_id(outbound_transfer),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def cancel(
        self,
        outbound_transfer: str,
        params: Optional["OutboundTransferCancelParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "OutboundTransfer":
        """
        An OutboundTransfer can be canceled if the funds have not yet been paid out.
        """
        return cast(
            "OutboundTransfer",
            self._request(
                "post",
                "/v1/treasury/outbound_transfers/{outbound_transfer}/cancel".format(
                    outbound_transfer=sanitize_id(outbound_transfer),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def cancel_async(
        self,
        outbound_transfer: str,
        params: Optional["OutboundTransferCancelParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "OutboundTransfer":
        """
        An OutboundTransfer can be canceled if the funds have not yet been paid out.
        """
        return cast(
            "OutboundTransfer",
            await self._request_async(
                "post",
                "/v1/treasury/outbound_transfers/{outbound_transfer}/cancel".format(
                    outbound_transfer=sanitize_id(outbound_transfer),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
