# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._request_options import RequestOptions
    from stripe.params.test_helpers.treasury._inbound_transfer_fail_params import (
        InboundTransferFailParams,
    )
    from stripe.params.test_helpers.treasury._inbound_transfer_return_inbound_transfer_params import (
        InboundTransferReturnInboundTransferParams,
    )
    from stripe.params.test_helpers.treasury._inbound_transfer_succeed_params import (
        InboundTransferSucceedParams,
    )
    from stripe.treasury._inbound_transfer import InboundTransfer


class InboundTransferService(StripeService):
    def fail(
        self,
        id: str,
        params: Optional["InboundTransferFailParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "InboundTransfer":
        """
        Transitions a test mode created InboundTransfer to the failed status. The InboundTransfer must already be in the processing state.
        """
        return cast(
            "InboundTransfer",
            self._request(
                "post",
                "/v1/test_helpers/treasury/inbound_transfers/{id}/fail".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def fail_async(
        self,
        id: str,
        params: Optional["InboundTransferFailParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "InboundTransfer":
        """
        Transitions a test mode created InboundTransfer to the failed status. The InboundTransfer must already be in the processing state.
        """
        return cast(
            "InboundTransfer",
            await self._request_async(
                "post",
                "/v1/test_helpers/treasury/inbound_transfers/{id}/fail".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def return_inbound_transfer(
        self,
        id: str,
        params: Optional["InboundTransferReturnInboundTransferParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "InboundTransfer":
        """
        Marks the test mode InboundTransfer object as returned and links the InboundTransfer to a ReceivedDebit. The InboundTransfer must already be in the succeeded state.
        """
        return cast(
            "InboundTransfer",
            self._request(
                "post",
                "/v1/test_helpers/treasury/inbound_transfers/{id}/return".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def return_inbound_transfer_async(
        self,
        id: str,
        params: Optional["InboundTransferReturnInboundTransferParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "InboundTransfer":
        """
        Marks the test mode InboundTransfer object as returned and links the InboundTransfer to a ReceivedDebit. The InboundTransfer must already be in the succeeded state.
        """
        return cast(
            "InboundTransfer",
            await self._request_async(
                "post",
                "/v1/test_helpers/treasury/inbound_transfers/{id}/return".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def succeed(
        self,
        id: str,
        params: Optional["InboundTransferSucceedParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "InboundTransfer":
        """
        Transitions a test mode created InboundTransfer to the succeeded status. The InboundTransfer must already be in the processing state.
        """
        return cast(
            "InboundTransfer",
            self._request(
                "post",
                "/v1/test_helpers/treasury/inbound_transfers/{id}/succeed".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def succeed_async(
        self,
        id: str,
        params: Optional["InboundTransferSucceedParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "InboundTransfer":
        """
        Transitions a test mode created InboundTransfer to the succeeded status. The InboundTransfer must already be in the processing state.
        """
        return cast(
            "InboundTransfer",
            await self._request_async(
                "post",
                "/v1/test_helpers/treasury/inbound_transfers/{id}/succeed".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
