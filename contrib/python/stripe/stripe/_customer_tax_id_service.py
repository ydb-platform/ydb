# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe._tax_id import TaxId
    from stripe.params._customer_tax_id_create_params import (
        CustomerTaxIdCreateParams,
    )
    from stripe.params._customer_tax_id_delete_params import (
        CustomerTaxIdDeleteParams,
    )
    from stripe.params._customer_tax_id_list_params import (
        CustomerTaxIdListParams,
    )
    from stripe.params._customer_tax_id_retrieve_params import (
        CustomerTaxIdRetrieveParams,
    )


class CustomerTaxIdService(StripeService):
    def delete(
        self,
        customer: str,
        id: str,
        params: Optional["CustomerTaxIdDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "TaxId":
        """
        Deletes an existing tax_id object.
        """
        return cast(
            "TaxId",
            self._request(
                "delete",
                "/v1/customers/{customer}/tax_ids/{id}".format(
                    customer=sanitize_id(customer),
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def delete_async(
        self,
        customer: str,
        id: str,
        params: Optional["CustomerTaxIdDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "TaxId":
        """
        Deletes an existing tax_id object.
        """
        return cast(
            "TaxId",
            await self._request_async(
                "delete",
                "/v1/customers/{customer}/tax_ids/{id}".format(
                    customer=sanitize_id(customer),
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        customer: str,
        id: str,
        params: Optional["CustomerTaxIdRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "TaxId":
        """
        Retrieves the tax_id object with the given identifier.
        """
        return cast(
            "TaxId",
            self._request(
                "get",
                "/v1/customers/{customer}/tax_ids/{id}".format(
                    customer=sanitize_id(customer),
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        customer: str,
        id: str,
        params: Optional["CustomerTaxIdRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "TaxId":
        """
        Retrieves the tax_id object with the given identifier.
        """
        return cast(
            "TaxId",
            await self._request_async(
                "get",
                "/v1/customers/{customer}/tax_ids/{id}".format(
                    customer=sanitize_id(customer),
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def list(
        self,
        customer: str,
        params: Optional["CustomerTaxIdListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[TaxId]":
        """
        Returns a list of tax IDs for a customer.
        """
        return cast(
            "ListObject[TaxId]",
            self._request(
                "get",
                "/v1/customers/{customer}/tax_ids".format(
                    customer=sanitize_id(customer),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        customer: str,
        params: Optional["CustomerTaxIdListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[TaxId]":
        """
        Returns a list of tax IDs for a customer.
        """
        return cast(
            "ListObject[TaxId]",
            await self._request_async(
                "get",
                "/v1/customers/{customer}/tax_ids".format(
                    customer=sanitize_id(customer),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        customer: str,
        params: "CustomerTaxIdCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "TaxId":
        """
        Creates a new tax_id object for a customer.
        """
        return cast(
            "TaxId",
            self._request(
                "post",
                "/v1/customers/{customer}/tax_ids".format(
                    customer=sanitize_id(customer),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        customer: str,
        params: "CustomerTaxIdCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "TaxId":
        """
        Creates a new tax_id object for a customer.
        """
        return cast(
            "TaxId",
            await self._request_async(
                "post",
                "/v1/customers/{customer}/tax_ids".format(
                    customer=sanitize_id(customer),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
