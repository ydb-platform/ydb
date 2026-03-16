# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.climate._order import Order
    from stripe.params.climate._order_cancel_params import OrderCancelParams
    from stripe.params.climate._order_create_params import OrderCreateParams
    from stripe.params.climate._order_list_params import OrderListParams
    from stripe.params.climate._order_retrieve_params import (
        OrderRetrieveParams,
    )
    from stripe.params.climate._order_update_params import OrderUpdateParams


class OrderService(StripeService):
    def list(
        self,
        params: Optional["OrderListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Order]":
        """
        Lists all Climate order objects. The orders are returned sorted by creation date, with the
        most recently created orders appearing first.
        """
        return cast(
            "ListObject[Order]",
            self._request(
                "get",
                "/v1/climate/orders",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["OrderListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Order]":
        """
        Lists all Climate order objects. The orders are returned sorted by creation date, with the
        most recently created orders appearing first.
        """
        return cast(
            "ListObject[Order]",
            await self._request_async(
                "get",
                "/v1/climate/orders",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: "OrderCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Order":
        """
        Creates a Climate order object for a given Climate product. The order will be processed immediately
        after creation and payment will be deducted your Stripe balance.
        """
        return cast(
            "Order",
            self._request(
                "post",
                "/v1/climate/orders",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "OrderCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Order":
        """
        Creates a Climate order object for a given Climate product. The order will be processed immediately
        after creation and payment will be deducted your Stripe balance.
        """
        return cast(
            "Order",
            await self._request_async(
                "post",
                "/v1/climate/orders",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        order: str,
        params: Optional["OrderRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Order":
        """
        Retrieves the details of a Climate order object with the given ID.
        """
        return cast(
            "Order",
            self._request(
                "get",
                "/v1/climate/orders/{order}".format(order=sanitize_id(order)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        order: str,
        params: Optional["OrderRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Order":
        """
        Retrieves the details of a Climate order object with the given ID.
        """
        return cast(
            "Order",
            await self._request_async(
                "get",
                "/v1/climate/orders/{order}".format(order=sanitize_id(order)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        order: str,
        params: Optional["OrderUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Order":
        """
        Updates the specified order by setting the values of the parameters passed.
        """
        return cast(
            "Order",
            self._request(
                "post",
                "/v1/climate/orders/{order}".format(order=sanitize_id(order)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        order: str,
        params: Optional["OrderUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Order":
        """
        Updates the specified order by setting the values of the parameters passed.
        """
        return cast(
            "Order",
            await self._request_async(
                "post",
                "/v1/climate/orders/{order}".format(order=sanitize_id(order)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def cancel(
        self,
        order: str,
        params: Optional["OrderCancelParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Order":
        """
        Cancels a Climate order. You can cancel an order within 24 hours of creation. Stripe refunds the
        reservation amount_subtotal, but not the amount_fees for user-triggered cancellations. Frontier
        might cancel reservations if suppliers fail to deliver. If Frontier cancels the reservation, Stripe
        provides 90 days advance notice and refunds the amount_total.
        """
        return cast(
            "Order",
            self._request(
                "post",
                "/v1/climate/orders/{order}/cancel".format(
                    order=sanitize_id(order),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def cancel_async(
        self,
        order: str,
        params: Optional["OrderCancelParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Order":
        """
        Cancels a Climate order. You can cancel an order within 24 hours of creation. Stripe refunds the
        reservation amount_subtotal, but not the amount_fees for user-triggered cancellations. Frontier
        might cancel reservations if suppliers fail to deliver. If Frontier cancels the reservation, Stripe
        provides 90 days advance notice and refunds the amount_total.
        """
        return cast(
            "Order",
            await self._request_async(
                "post",
                "/v1/climate/orders/{order}/cancel".format(
                    order=sanitize_id(order),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
