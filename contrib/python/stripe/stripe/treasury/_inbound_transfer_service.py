# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.params.treasury._inbound_transfer_cancel_params import (
        InboundTransferCancelParams,
    )
    from stripe.params.treasury._inbound_transfer_create_params import (
        InboundTransferCreateParams,
    )
    from stripe.params.treasury._inbound_transfer_list_params import (
        InboundTransferListParams,
    )
    from stripe.params.treasury._inbound_transfer_retrieve_params import (
        InboundTransferRetrieveParams,
    )
    from stripe.treasury._inbound_transfer import InboundTransfer


class InboundTransferService(StripeService):
    def list(
        self,
        params: "InboundTransferListParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[InboundTransfer]":
        """
        Returns a list of InboundTransfers sent from the specified FinancialAccount.
        """
        return cast(
            "ListObject[InboundTransfer]",
            self._request(
                "get",
                "/v1/treasury/inbound_transfers",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: "InboundTransferListParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[InboundTransfer]":
        """
        Returns a list of InboundTransfers sent from the specified FinancialAccount.
        """
        return cast(
            "ListObject[InboundTransfer]",
            await self._request_async(
                "get",
                "/v1/treasury/inbound_transfers",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: "InboundTransferCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "InboundTransfer":
        """
        Creates an InboundTransfer.
        """
        return cast(
            "InboundTransfer",
            self._request(
                "post",
                "/v1/treasury/inbound_transfers",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "InboundTransferCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "InboundTransfer":
        """
        Creates an InboundTransfer.
        """
        return cast(
            "InboundTransfer",
            await self._request_async(
                "post",
                "/v1/treasury/inbound_transfers",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        id: str,
        params: Optional["InboundTransferRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "InboundTransfer":
        """
        Retrieves the details of an existing InboundTransfer.
        """
        return cast(
            "InboundTransfer",
            self._request(
                "get",
                "/v1/treasury/inbound_transfers/{id}".format(
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
        params: Optional["InboundTransferRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "InboundTransfer":
        """
        Retrieves the details of an existing InboundTransfer.
        """
        return cast(
            "InboundTransfer",
            await self._request_async(
                "get",
                "/v1/treasury/inbound_transfers/{id}".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def cancel(
        self,
        inbound_transfer: str,
        params: Optional["InboundTransferCancelParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "InboundTransfer":
        """
        Cancels an InboundTransfer.
        """
        return cast(
            "InboundTransfer",
            self._request(
                "post",
                "/v1/treasury/inbound_transfers/{inbound_transfer}/cancel".format(
                    inbound_transfer=sanitize_id(inbound_transfer),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def cancel_async(
        self,
        inbound_transfer: str,
        params: Optional["InboundTransferCancelParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "InboundTransfer":
        """
        Cancels an InboundTransfer.
        """
        return cast(
            "InboundTransfer",
            await self._request_async(
                "post",
                "/v1/treasury/inbound_transfers/{inbound_transfer}/cancel".format(
                    inbound_transfer=sanitize_id(inbound_transfer),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
