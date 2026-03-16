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
    from stripe.params._tax_id_create_params import TaxIdCreateParams
    from stripe.params._tax_id_delete_params import TaxIdDeleteParams
    from stripe.params._tax_id_list_params import TaxIdListParams
    from stripe.params._tax_id_retrieve_params import TaxIdRetrieveParams


class TaxIdService(StripeService):
    def delete(
        self,
        id: str,
        params: Optional["TaxIdDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "TaxId":
        """
        Deletes an existing account or customer tax_id object.
        """
        return cast(
            "TaxId",
            self._request(
                "delete",
                "/v1/tax_ids/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def delete_async(
        self,
        id: str,
        params: Optional["TaxIdDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "TaxId":
        """
        Deletes an existing account or customer tax_id object.
        """
        return cast(
            "TaxId",
            await self._request_async(
                "delete",
                "/v1/tax_ids/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        id: str,
        params: Optional["TaxIdRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "TaxId":
        """
        Retrieves an account or customer tax_id object.
        """
        return cast(
            "TaxId",
            self._request(
                "get",
                "/v1/tax_ids/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        id: str,
        params: Optional["TaxIdRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "TaxId":
        """
        Retrieves an account or customer tax_id object.
        """
        return cast(
            "TaxId",
            await self._request_async(
                "get",
                "/v1/tax_ids/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def list(
        self,
        params: Optional["TaxIdListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[TaxId]":
        """
        Returns a list of tax IDs.
        """
        return cast(
            "ListObject[TaxId]",
            self._request(
                "get",
                "/v1/tax_ids",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["TaxIdListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[TaxId]":
        """
        Returns a list of tax IDs.
        """
        return cast(
            "ListObject[TaxId]",
            await self._request_async(
                "get",
                "/v1/tax_ids",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: "TaxIdCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "TaxId":
        """
        Creates a new account or customer tax_id object.
        """
        return cast(
            "TaxId",
            self._request(
                "post",
                "/v1/tax_ids",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "TaxIdCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "TaxId":
        """
        Creates a new account or customer tax_id object.
        """
        return cast(
            "TaxId",
            await self._request_async(
                "post",
                "/v1/tax_ids",
                base_address="api",
                params=params,
                options=options,
            ),
        )
