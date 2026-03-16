# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.climate._product import Product
    from stripe.params.climate._product_list_params import ProductListParams
    from stripe.params.climate._product_retrieve_params import (
        ProductRetrieveParams,
    )


class ProductService(StripeService):
    def list(
        self,
        params: Optional["ProductListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Product]":
        """
        Lists all available Climate product objects.
        """
        return cast(
            "ListObject[Product]",
            self._request(
                "get",
                "/v1/climate/products",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["ProductListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Product]":
        """
        Lists all available Climate product objects.
        """
        return cast(
            "ListObject[Product]",
            await self._request_async(
                "get",
                "/v1/climate/products",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        product: str,
        params: Optional["ProductRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Product":
        """
        Retrieves the details of a Climate product with the given ID.
        """
        return cast(
            "Product",
            self._request(
                "get",
                "/v1/climate/products/{product}".format(
                    product=sanitize_id(product),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        product: str,
        params: Optional["ProductRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Product":
        """
        Retrieves the details of a Climate product with the given ID.
        """
        return cast(
            "Product",
            await self._request_async(
                "get",
                "/v1/climate/products/{product}".format(
                    product=sanitize_id(product),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
