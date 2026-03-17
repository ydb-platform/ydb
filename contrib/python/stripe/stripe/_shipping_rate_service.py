# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe._shipping_rate import ShippingRate
    from stripe.params._shipping_rate_create_params import (
        ShippingRateCreateParams,
    )
    from stripe.params._shipping_rate_list_params import ShippingRateListParams
    from stripe.params._shipping_rate_retrieve_params import (
        ShippingRateRetrieveParams,
    )
    from stripe.params._shipping_rate_update_params import (
        ShippingRateUpdateParams,
    )


class ShippingRateService(StripeService):
    def list(
        self,
        params: Optional["ShippingRateListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[ShippingRate]":
        """
        Returns a list of your shipping rates.
        """
        return cast(
            "ListObject[ShippingRate]",
            self._request(
                "get",
                "/v1/shipping_rates",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["ShippingRateListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[ShippingRate]":
        """
        Returns a list of your shipping rates.
        """
        return cast(
            "ListObject[ShippingRate]",
            await self._request_async(
                "get",
                "/v1/shipping_rates",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: "ShippingRateCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ShippingRate":
        """
        Creates a new shipping rate object.
        """
        return cast(
            "ShippingRate",
            self._request(
                "post",
                "/v1/shipping_rates",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "ShippingRateCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ShippingRate":
        """
        Creates a new shipping rate object.
        """
        return cast(
            "ShippingRate",
            await self._request_async(
                "post",
                "/v1/shipping_rates",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        shipping_rate_token: str,
        params: Optional["ShippingRateRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ShippingRate":
        """
        Returns the shipping rate object with the given ID.
        """
        return cast(
            "ShippingRate",
            self._request(
                "get",
                "/v1/shipping_rates/{shipping_rate_token}".format(
                    shipping_rate_token=sanitize_id(shipping_rate_token),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        shipping_rate_token: str,
        params: Optional["ShippingRateRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ShippingRate":
        """
        Returns the shipping rate object with the given ID.
        """
        return cast(
            "ShippingRate",
            await self._request_async(
                "get",
                "/v1/shipping_rates/{shipping_rate_token}".format(
                    shipping_rate_token=sanitize_id(shipping_rate_token),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        shipping_rate_token: str,
        params: Optional["ShippingRateUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ShippingRate":
        """
        Updates an existing shipping rate object.
        """
        return cast(
            "ShippingRate",
            self._request(
                "post",
                "/v1/shipping_rates/{shipping_rate_token}".format(
                    shipping_rate_token=sanitize_id(shipping_rate_token),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        shipping_rate_token: str,
        params: Optional["ShippingRateUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ShippingRate":
        """
        Updates an existing shipping rate object.
        """
        return cast(
            "ShippingRate",
            await self._request_async(
                "post",
                "/v1/shipping_rates/{shipping_rate_token}".format(
                    shipping_rate_token=sanitize_id(shipping_rate_token),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
