# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._coupon import Coupon
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.params._coupon_create_params import CouponCreateParams
    from stripe.params._coupon_delete_params import CouponDeleteParams
    from stripe.params._coupon_list_params import CouponListParams
    from stripe.params._coupon_retrieve_params import CouponRetrieveParams
    from stripe.params._coupon_update_params import CouponUpdateParams


class CouponService(StripeService):
    def delete(
        self,
        coupon: str,
        params: Optional["CouponDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Coupon":
        """
        You can delete coupons via the [coupon management](https://dashboard.stripe.com/coupons) page of the Stripe dashboard. However, deleting a coupon does not affect any customers who have already applied the coupon; it means that new customers can't redeem the coupon. You can also delete coupons via the API.
        """
        return cast(
            "Coupon",
            self._request(
                "delete",
                "/v1/coupons/{coupon}".format(coupon=sanitize_id(coupon)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def delete_async(
        self,
        coupon: str,
        params: Optional["CouponDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Coupon":
        """
        You can delete coupons via the [coupon management](https://dashboard.stripe.com/coupons) page of the Stripe dashboard. However, deleting a coupon does not affect any customers who have already applied the coupon; it means that new customers can't redeem the coupon. You can also delete coupons via the API.
        """
        return cast(
            "Coupon",
            await self._request_async(
                "delete",
                "/v1/coupons/{coupon}".format(coupon=sanitize_id(coupon)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        coupon: str,
        params: Optional["CouponRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Coupon":
        """
        Retrieves the coupon with the given ID.
        """
        return cast(
            "Coupon",
            self._request(
                "get",
                "/v1/coupons/{coupon}".format(coupon=sanitize_id(coupon)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        coupon: str,
        params: Optional["CouponRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Coupon":
        """
        Retrieves the coupon with the given ID.
        """
        return cast(
            "Coupon",
            await self._request_async(
                "get",
                "/v1/coupons/{coupon}".format(coupon=sanitize_id(coupon)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        coupon: str,
        params: Optional["CouponUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Coupon":
        """
        Updates the metadata of a coupon. Other coupon details (currency, duration, amount_off) are, by design, not editable.
        """
        return cast(
            "Coupon",
            self._request(
                "post",
                "/v1/coupons/{coupon}".format(coupon=sanitize_id(coupon)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        coupon: str,
        params: Optional["CouponUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Coupon":
        """
        Updates the metadata of a coupon. Other coupon details (currency, duration, amount_off) are, by design, not editable.
        """
        return cast(
            "Coupon",
            await self._request_async(
                "post",
                "/v1/coupons/{coupon}".format(coupon=sanitize_id(coupon)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def list(
        self,
        params: Optional["CouponListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Coupon]":
        """
        Returns a list of your coupons.
        """
        return cast(
            "ListObject[Coupon]",
            self._request(
                "get",
                "/v1/coupons",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["CouponListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Coupon]":
        """
        Returns a list of your coupons.
        """
        return cast(
            "ListObject[Coupon]",
            await self._request_async(
                "get",
                "/v1/coupons",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: Optional["CouponCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Coupon":
        """
        You can create coupons easily via the [coupon management](https://dashboard.stripe.com/coupons) page of the Stripe dashboard. Coupon creation is also accessible via the API if you need to create coupons on the fly.

        A coupon has either a percent_off or an amount_off and currency. If you set an amount_off, that amount will be subtracted from any invoice's subtotal. For example, an invoice with a subtotal of 100 will have a final total of 0 if a coupon with an amount_off of 200 is applied to it and an invoice with a subtotal of 300 will have a final total of 100 if a coupon with an amount_off of 200 is applied to it.
        """
        return cast(
            "Coupon",
            self._request(
                "post",
                "/v1/coupons",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: Optional["CouponCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Coupon":
        """
        You can create coupons easily via the [coupon management](https://dashboard.stripe.com/coupons) page of the Stripe dashboard. Coupon creation is also accessible via the API if you need to create coupons on the fly.

        A coupon has either a percent_off or an amount_off and currency. If you set an amount_off, that amount will be subtracted from any invoice's subtotal. For example, an invoice with a subtotal of 100 will have a final total of 0 if a coupon with an amount_off of 200 is applied to it and an invoice with a subtotal of 300 will have a final total of 100 if a coupon with an amount_off of 200 is applied to it.
        """
        return cast(
            "Coupon",
            await self._request_async(
                "post",
                "/v1/coupons",
                base_address="api",
                params=params,
                options=options,
            ),
        )
