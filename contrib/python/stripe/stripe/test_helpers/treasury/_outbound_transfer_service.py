# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._request_options import RequestOptions
    from stripe.params.test_helpers.treasury._outbound_transfer_fail_params import (
        OutboundTransferFailParams,
    )
    from stripe.params.test_helpers.treasury._outbound_transfer_post_params import (
        OutboundTransferPostParams,
    )
    from stripe.params.test_helpers.treasury._outbound_transfer_return_outbound_transfer_params import (
        OutboundTransferReturnOutboundTransferParams,
    )
    from stripe.params.test_helpers.treasury._outbound_transfer_update_params import (
        OutboundTransferUpdateParams,
    )
    from stripe.treasury._outbound_transfer import OutboundTransfer


class OutboundTransferService(StripeService):
    def update(
        self,
        outbound_transfer: str,
        params: "OutboundTransferUpdateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "OutboundTransfer":
        """
        Updates a test mode created OutboundTransfer with tracking details. The OutboundTransfer must not be cancelable, and cannot be in the canceled or failed states.
        """
        return cast(
            "OutboundTransfer",
            self._request(
                "post",
                "/v1/test_helpers/treasury/outbound_transfers/{outbound_transfer}".format(
                    outbound_transfer=sanitize_id(outbound_transfer),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        outbound_transfer: str,
        params: "OutboundTransferUpdateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "OutboundTransfer":
        """
        Updates a test mode created OutboundTransfer with tracking details. The OutboundTransfer must not be cancelable, and cannot be in the canceled or failed states.
        """
        return cast(
            "OutboundTransfer",
            await self._request_async(
                "post",
                "/v1/test_helpers/treasury/outbound_transfers/{outbound_transfer}".format(
                    outbound_transfer=sanitize_id(outbound_transfer),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def fail(
        self,
        outbound_transfer: str,
        params: Optional["OutboundTransferFailParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "OutboundTransfer":
        """
        Transitions a test mode created OutboundTransfer to the failed status. The OutboundTransfer must already be in the processing state.
        """
        return cast(
            "OutboundTransfer",
            self._request(
                "post",
                "/v1/test_helpers/treasury/outbound_transfers/{outbound_transfer}/fail".format(
                    outbound_transfer=sanitize_id(outbound_transfer),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def fail_async(
        self,
        outbound_transfer: str,
        params: Optional["OutboundTransferFailParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "OutboundTransfer":
        """
        Transitions a test mode created OutboundTransfer to the failed status. The OutboundTransfer must already be in the processing state.
        """
        return cast(
            "OutboundTransfer",
            await self._request_async(
                "post",
                "/v1/test_helpers/treasury/outbound_transfers/{outbound_transfer}/fail".format(
                    outbound_transfer=sanitize_id(outbound_transfer),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def post(
        self,
        outbound_transfer: str,
        params: Optional["OutboundTransferPostParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "OutboundTransfer":
        """
        Transitions a test mode created OutboundTransfer to the posted status. The OutboundTransfer must already be in the processing state.
        """
        return cast(
            "OutboundTransfer",
            self._request(
                "post",
                "/v1/test_helpers/treasury/outbound_transfers/{outbound_transfer}/post".format(
                    outbound_transfer=sanitize_id(outbound_transfer),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def post_async(
        self,
        outbound_transfer: str,
        params: Optional["OutboundTransferPostParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "OutboundTransfer":
        """
        Transitions a test mode created OutboundTransfer to the posted status. The OutboundTransfer must already be in the processing state.
        """
        return cast(
            "OutboundTransfer",
            await self._request_async(
                "post",
                "/v1/test_helpers/treasury/outbound_transfers/{outbound_transfer}/post".format(
                    outbound_transfer=sanitize_id(outbound_transfer),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def return_outbound_transfer(
        self,
        outbound_transfer: str,
        params: Optional[
            "OutboundTransferReturnOutboundTransferParams"
        ] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "OutboundTransfer":
        """
        Transitions a test mode created OutboundTransfer to the returned status. The OutboundTransfer must already be in the processing state.
        """
        return cast(
            "OutboundTransfer",
            self._request(
                "post",
                "/v1/test_helpers/treasury/outbound_transfers/{outbound_transfer}/return".format(
                    outbound_transfer=sanitize_id(outbound_transfer),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def return_outbound_transfer_async(
        self,
        outbound_transfer: str,
        params: Optional[
            "OutboundTransferReturnOutboundTransferParams"
        ] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "OutboundTransfer":
        """
        Transitions a test mode created OutboundTransfer to the returned status. The OutboundTransfer must already be in the processing state.
        """
        return cast(
            "OutboundTransfer",
            await self._request_async(
                "post",
                "/v1/test_helpers/treasury/outbound_transfers/{outbound_transfer}/return".format(
                    outbound_transfer=sanitize_id(outbound_transfer),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
