# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._product import Product
    from stripe._product_feature_service import ProductFeatureService
    from stripe._request_options import RequestOptions
    from stripe._search_result_object import SearchResultObject
    from stripe.params._product_create_params import ProductCreateParams
    from stripe.params._product_delete_params import ProductDeleteParams
    from stripe.params._product_list_params import ProductListParams
    from stripe.params._product_retrieve_params import ProductRetrieveParams
    from stripe.params._product_search_params import ProductSearchParams
    from stripe.params._product_update_params import ProductUpdateParams

_subservices = {
    "features": ["stripe._product_feature_service", "ProductFeatureService"],
}


class ProductService(StripeService):
    features: "ProductFeatureService"

    def __init__(self, requestor):
        super().__init__(requestor)

    def __getattr__(self, name):
        try:
            import_from, service = _subservices[name]
            service_class = getattr(
                import_module(import_from),
                service,
            )
            setattr(
                self,
                name,
                service_class(self._requestor),
            )
            return getattr(self, name)
        except KeyError:
            raise AttributeError()

    def delete(
        self,
        id: str,
        params: Optional["ProductDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Product":
        """
        Delete a product. Deleting a product is only possible if it has no prices associated with it. Additionally, deleting a product with type=good is only possible if it has no SKUs associated with it.
        """
        return cast(
            "Product",
            self._request(
                "delete",
                "/v1/products/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def delete_async(
        self,
        id: str,
        params: Optional["ProductDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Product":
        """
        Delete a product. Deleting a product is only possible if it has no prices associated with it. Additionally, deleting a product with type=good is only possible if it has no SKUs associated with it.
        """
        return cast(
            "Product",
            await self._request_async(
                "delete",
                "/v1/products/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        id: str,
        params: Optional["ProductRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Product":
        """
        Retrieves the details of an existing product. Supply the unique product ID from either a product creation request or the product list, and Stripe will return the corresponding product information.
        """
        return cast(
            "Product",
            self._request(
                "get",
                "/v1/products/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        id: str,
        params: Optional["ProductRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Product":
        """
        Retrieves the details of an existing product. Supply the unique product ID from either a product creation request or the product list, and Stripe will return the corresponding product information.
        """
        return cast(
            "Product",
            await self._request_async(
                "get",
                "/v1/products/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        id: str,
        params: Optional["ProductUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Product":
        """
        Updates the specific product by setting the values of the parameters passed. Any parameters not provided will be left unchanged.
        """
        return cast(
            "Product",
            self._request(
                "post",
                "/v1/products/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        id: str,
        params: Optional["ProductUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Product":
        """
        Updates the specific product by setting the values of the parameters passed. Any parameters not provided will be left unchanged.
        """
        return cast(
            "Product",
            await self._request_async(
                "post",
                "/v1/products/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def list(
        self,
        params: Optional["ProductListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Product]":
        """
        Returns a list of your products. The products are returned sorted by creation date, with the most recently created products appearing first.
        """
        return cast(
            "ListObject[Product]",
            self._request(
                "get",
                "/v1/products",
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
        Returns a list of your products. The products are returned sorted by creation date, with the most recently created products appearing first.
        """
        return cast(
            "ListObject[Product]",
            await self._request_async(
                "get",
                "/v1/products",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: "ProductCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Product":
        """
        Creates a new product object.
        """
        return cast(
            "Product",
            self._request(
                "post",
                "/v1/products",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "ProductCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Product":
        """
        Creates a new product object.
        """
        return cast(
            "Product",
            await self._request_async(
                "post",
                "/v1/products",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def search(
        self,
        params: "ProductSearchParams",
        options: Optional["RequestOptions"] = None,
    ) -> "SearchResultObject[Product]":
        """
        Search for products you've previously created using Stripe's [Search Query Language](https://docs.stripe.com/docs/search#search-query-language).
        Don't use search in read-after-write flows where strict consistency is necessary. Under normal operating
        conditions, data is searchable in less than a minute. Occasionally, propagation of new or updated data can be up
        to an hour behind during outages. Search functionality is not available to merchants in India.
        """
        return cast(
            "SearchResultObject[Product]",
            self._request(
                "get",
                "/v1/products/search",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def search_async(
        self,
        params: "ProductSearchParams",
        options: Optional["RequestOptions"] = None,
    ) -> "SearchResultObject[Product]":
        """
        Search for products you've previously created using Stripe's [Search Query Language](https://docs.stripe.com/docs/search#search-query-language).
        Don't use search in read-after-write flows where strict consistency is necessary. Under normal operating
        conditions, data is searchable in less than a minute. Occasionally, propagation of new or updated data can be up
        to an hour behind during outages. Search functionality is not available to merchants in India.
        """
        return cast(
            "SearchResultObject[Product]",
            await self._request_async(
                "get",
                "/v1/products/search",
                base_address="api",
                params=params,
                options=options,
            ),
        )
