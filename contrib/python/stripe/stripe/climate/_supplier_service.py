# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.climate._supplier import Supplier
    from stripe.params.climate._supplier_list_params import SupplierListParams
    from stripe.params.climate._supplier_retrieve_params import (
        SupplierRetrieveParams,
    )


class SupplierService(StripeService):
    def list(
        self,
        params: Optional["SupplierListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Supplier]":
        """
        Lists all available Climate supplier objects.
        """
        return cast(
            "ListObject[Supplier]",
            self._request(
                "get",
                "/v1/climate/suppliers",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["SupplierListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Supplier]":
        """
        Lists all available Climate supplier objects.
        """
        return cast(
            "ListObject[Supplier]",
            await self._request_async(
                "get",
                "/v1/climate/suppliers",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        supplier: str,
        params: Optional["SupplierRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Supplier":
        """
        Retrieves a Climate supplier object.
        """
        return cast(
            "Supplier",
            self._request(
                "get",
                "/v1/climate/suppliers/{supplier}".format(
                    supplier=sanitize_id(supplier),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        supplier: str,
        params: Optional["SupplierRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Supplier":
        """
        Retrieves a Climate supplier object.
        """
        return cast(
            "Supplier",
            await self._request_async(
                "get",
                "/v1/climate/suppliers/{supplier}".format(
                    supplier=sanitize_id(supplier),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
