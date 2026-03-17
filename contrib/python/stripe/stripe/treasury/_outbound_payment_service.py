# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.params.treasury._outbound_payment_cancel_params import (
        OutboundPaymentCancelParams,
    )
    from stripe.params.treasury._outbound_payment_create_params import (
        OutboundPaymentCreateParams,
    )
    from stripe.params.treasury._outbound_payment_list_params import (
        OutboundPaymentListParams,
    )
    from stripe.params.treasury._outbound_payment_retrieve_params import (
        OutboundPaymentRetrieveParams,
    )
    from stripe.treasury._outbound_payment import OutboundPayment


class OutboundPaymentService(StripeService):
    def list(
        self,
        params: "OutboundPaymentListParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[OutboundPayment]":
        """
        Returns a list of OutboundPayments sent from the specified FinancialAccount.
        """
        return cast(
            "ListObject[OutboundPayment]",
            self._request(
                "get",
                "/v1/treasury/outbound_payments",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: "OutboundPaymentListParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[OutboundPayment]":
        """
        Returns a list of OutboundPayments sent from the specified FinancialAccount.
        """
        return cast(
            "ListObject[OutboundPayment]",
            await self._request_async(
                "get",
                "/v1/treasury/outbound_payments",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: "OutboundPaymentCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "OutboundPayment":
        """
        Creates an OutboundPayment.
        """
        return cast(
            "OutboundPayment",
            self._request(
                "post",
                "/v1/treasury/outbound_payments",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "OutboundPaymentCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "OutboundPayment":
        """
        Creates an OutboundPayment.
        """
        return cast(
            "OutboundPayment",
            await self._request_async(
                "post",
                "/v1/treasury/outbound_payments",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        id: str,
        params: Optional["OutboundPaymentRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "OutboundPayment":
        """
        Retrieves the details of an existing OutboundPayment by passing the unique OutboundPayment ID from either the OutboundPayment creation request or OutboundPayment list.
        """
        return cast(
            "OutboundPayment",
            self._request(
                "get",
                "/v1/treasury/outbound_payments/{id}".format(
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
        params: Optional["OutboundPaymentRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "OutboundPayment":
        """
        Retrieves the details of an existing OutboundPayment by passing the unique OutboundPayment ID from either the OutboundPayment creation request or OutboundPayment list.
        """
        return cast(
            "OutboundPayment",
            await self._request_async(
                "get",
                "/v1/treasury/outbound_payments/{id}".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def cancel(
        self,
        id: str,
        params: Optional["OutboundPaymentCancelParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "OutboundPayment":
        """
        Cancel an OutboundPayment.
        """
        return cast(
            "OutboundPayment",
            self._request(
                "post",
                "/v1/treasury/outbound_payments/{id}/cancel".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def cancel_async(
        self,
        id: str,
        params: Optional["OutboundPaymentCancelParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "OutboundPayment":
        """
        Cancel an OutboundPayment.
        """
        return cast(
            "OutboundPayment",
            await self._request_async(
                "post",
                "/v1/treasury/outbound_payments/{id}/cancel".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
