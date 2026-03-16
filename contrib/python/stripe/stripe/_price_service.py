# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._price import Price
    from stripe._request_options import RequestOptions
    from stripe._search_result_object import SearchResultObject
    from stripe.params._price_create_params import PriceCreateParams
    from stripe.params._price_list_params import PriceListParams
    from stripe.params._price_retrieve_params import PriceRetrieveParams
    from stripe.params._price_search_params import PriceSearchParams
    from stripe.params._price_update_params import PriceUpdateParams


class PriceService(StripeService):
    def list(
        self,
        params: Optional["PriceListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Price]":
        """
        Returns a list of your active prices, excluding [inline prices](https://docs.stripe.com/docs/products-prices/pricing-models#inline-pricing). For the list of inactive prices, set active to false.
        """
        return cast(
            "ListObject[Price]",
            self._request(
                "get",
                "/v1/prices",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["PriceListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Price]":
        """
        Returns a list of your active prices, excluding [inline prices](https://docs.stripe.com/docs/products-prices/pricing-models#inline-pricing). For the list of inactive prices, set active to false.
        """
        return cast(
            "ListObject[Price]",
            await self._request_async(
                "get",
                "/v1/prices",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: "PriceCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Price":
        """
        Creates a new [Price for an existing <a href="https://docs.stripe.com/api/products">Product](https://docs.stripe.com/api/prices). The Price can be recurring or one-time.
        """
        return cast(
            "Price",
            self._request(
                "post",
                "/v1/prices",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "PriceCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Price":
        """
        Creates a new [Price for an existing <a href="https://docs.stripe.com/api/products">Product](https://docs.stripe.com/api/prices). The Price can be recurring or one-time.
        """
        return cast(
            "Price",
            await self._request_async(
                "post",
                "/v1/prices",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        price: str,
        params: Optional["PriceRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Price":
        """
        Retrieves the price with the given ID.
        """
        return cast(
            "Price",
            self._request(
                "get",
                "/v1/prices/{price}".format(price=sanitize_id(price)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        price: str,
        params: Optional["PriceRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Price":
        """
        Retrieves the price with the given ID.
        """
        return cast(
            "Price",
            await self._request_async(
                "get",
                "/v1/prices/{price}".format(price=sanitize_id(price)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        price: str,
        params: Optional["PriceUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Price":
        """
        Updates the specified price by setting the values of the parameters passed. Any parameters not provided are left unchanged.
        """
        return cast(
            "Price",
            self._request(
                "post",
                "/v1/prices/{price}".format(price=sanitize_id(price)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        price: str,
        params: Optional["PriceUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Price":
        """
        Updates the specified price by setting the values of the parameters passed. Any parameters not provided are left unchanged.
        """
        return cast(
            "Price",
            await self._request_async(
                "post",
                "/v1/prices/{price}".format(price=sanitize_id(price)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def search(
        self,
        params: "PriceSearchParams",
        options: Optional["RequestOptions"] = None,
    ) -> "SearchResultObject[Price]":
        """
        Search for prices you've previously created using Stripe's [Search Query Language](https://docs.stripe.com/docs/search#search-query-language).
        Don't use search in read-after-write flows where strict consistency is necessary. Under normal operating
        conditions, data is searchable in less than a minute. Occasionally, propagation of new or updated data can be up
        to an hour behind during outages. Search functionality is not available to merchants in India.
        """
        return cast(
            "SearchResultObject[Price]",
            self._request(
                "get",
                "/v1/prices/search",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def search_async(
        self,
        params: "PriceSearchParams",
        options: Optional["RequestOptions"] = None,
    ) -> "SearchResultObject[Price]":
        """
        Search for prices you've previously created using Stripe's [Search Query Language](https://docs.stripe.com/docs/search#search-query-language).
        Don't use search in read-after-write flows where strict consistency is necessary. Under normal operating
        conditions, data is searchable in less than a minute. Occasionally, propagation of new or updated data can be up
        to an hour behind during outages. Search functionality is not available to merchants in India.
        """
        return cast(
            "SearchResultObject[Price]",
            await self._request_async(
                "get",
                "/v1/prices/search",
                base_address="api",
                params=params,
                options=options,
            ),
        )
