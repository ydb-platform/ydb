# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._request_options import RequestOptions
    from stripe.params.test_helpers.treasury._outbound_payment_fail_params import (
        OutboundPaymentFailParams,
    )
    from stripe.params.test_helpers.treasury._outbound_payment_post_params import (
        OutboundPaymentPostParams,
    )
    from stripe.params.test_helpers.treasury._outbound_payment_return_outbound_payment_params import (
        OutboundPaymentReturnOutboundPaymentParams,
    )
    from stripe.params.test_helpers.treasury._outbound_payment_update_params import (
        OutboundPaymentUpdateParams,
    )
    from stripe.treasury._outbound_payment import OutboundPayment


class OutboundPaymentService(StripeService):
    def update(
        self,
        id: str,
        params: "OutboundPaymentUpdateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "OutboundPayment":
        """
        Updates a test mode created OutboundPayment with tracking details. The OutboundPayment must not be cancelable, and cannot be in the canceled or failed states.
        """
        return cast(
            "OutboundPayment",
            self._request(
                "post",
                "/v1/test_helpers/treasury/outbound_payments/{id}".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        id: str,
        params: "OutboundPaymentUpdateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "OutboundPayment":
        """
        Updates a test mode created OutboundPayment with tracking details. The OutboundPayment must not be cancelable, and cannot be in the canceled or failed states.
        """
        return cast(
            "OutboundPayment",
            await self._request_async(
                "post",
                "/v1/test_helpers/treasury/outbound_payments/{id}".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def fail(
        self,
        id: str,
        params: Optional["OutboundPaymentFailParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "OutboundPayment":
        """
        Transitions a test mode created OutboundPayment to the failed status. The OutboundPayment must already be in the processing state.
        """
        return cast(
            "OutboundPayment",
            self._request(
                "post",
                "/v1/test_helpers/treasury/outbound_payments/{id}/fail".format(
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
        params: Optional["OutboundPaymentFailParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "OutboundPayment":
        """
        Transitions a test mode created OutboundPayment to the failed status. The OutboundPayment must already be in the processing state.
        """
        return cast(
            "OutboundPayment",
            await self._request_async(
                "post",
                "/v1/test_helpers/treasury/outbound_payments/{id}/fail".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def post(
        self,
        id: str,
        params: Optional["OutboundPaymentPostParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "OutboundPayment":
        """
        Transitions a test mode created OutboundPayment to the posted status. The OutboundPayment must already be in the processing state.
        """
        return cast(
            "OutboundPayment",
            self._request(
                "post",
                "/v1/test_helpers/treasury/outbound_payments/{id}/post".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def post_async(
        self,
        id: str,
        params: Optional["OutboundPaymentPostParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "OutboundPayment":
        """
        Transitions a test mode created OutboundPayment to the posted status. The OutboundPayment must already be in the processing state.
        """
        return cast(
            "OutboundPayment",
            await self._request_async(
                "post",
                "/v1/test_helpers/treasury/outbound_payments/{id}/post".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def return_outbound_payment(
        self,
        id: str,
        params: Optional["OutboundPaymentReturnOutboundPaymentParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "OutboundPayment":
        """
        Transitions a test mode created OutboundPayment to the returned status. The OutboundPayment must already be in the processing state.
        """
        return cast(
            "OutboundPayment",
            self._request(
                "post",
                "/v1/test_helpers/treasury/outbound_payments/{id}/return".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def return_outbound_payment_async(
        self,
        id: str,
        params: Optional["OutboundPaymentReturnOutboundPaymentParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "OutboundPayment":
        """
        Transitions a test mode created OutboundPayment to the returned status. The OutboundPayment must already be in the processing state.
        """
        return cast(
            "OutboundPayment",
            await self._request_async(
                "post",
                "/v1/test_helpers/treasury/outbound_payments/{id}/return".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
