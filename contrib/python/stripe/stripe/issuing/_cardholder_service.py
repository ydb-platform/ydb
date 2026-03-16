# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.issuing._cardholder import Cardholder
    from stripe.params.issuing._cardholder_create_params import (
        CardholderCreateParams,
    )
    from stripe.params.issuing._cardholder_list_params import (
        CardholderListParams,
    )
    from stripe.params.issuing._cardholder_retrieve_params import (
        CardholderRetrieveParams,
    )
    from stripe.params.issuing._cardholder_update_params import (
        CardholderUpdateParams,
    )


class CardholderService(StripeService):
    def list(
        self,
        params: Optional["CardholderListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Cardholder]":
        """
        Returns a list of Issuing Cardholder objects. The objects are sorted in descending order by creation date, with the most recently created object appearing first.
        """
        return cast(
            "ListObject[Cardholder]",
            self._request(
                "get",
                "/v1/issuing/cardholders",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["CardholderListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Cardholder]":
        """
        Returns a list of Issuing Cardholder objects. The objects are sorted in descending order by creation date, with the most recently created object appearing first.
        """
        return cast(
            "ListObject[Cardholder]",
            await self._request_async(
                "get",
                "/v1/issuing/cardholders",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: "CardholderCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Cardholder":
        """
        Creates a new Issuing Cardholder object that can be issued cards.
        """
        return cast(
            "Cardholder",
            self._request(
                "post",
                "/v1/issuing/cardholders",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "CardholderCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Cardholder":
        """
        Creates a new Issuing Cardholder object that can be issued cards.
        """
        return cast(
            "Cardholder",
            await self._request_async(
                "post",
                "/v1/issuing/cardholders",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        cardholder: str,
        params: Optional["CardholderRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Cardholder":
        """
        Retrieves an Issuing Cardholder object.
        """
        return cast(
            "Cardholder",
            self._request(
                "get",
                "/v1/issuing/cardholders/{cardholder}".format(
                    cardholder=sanitize_id(cardholder),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        cardholder: str,
        params: Optional["CardholderRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Cardholder":
        """
        Retrieves an Issuing Cardholder object.
        """
        return cast(
            "Cardholder",
            await self._request_async(
                "get",
                "/v1/issuing/cardholders/{cardholder}".format(
                    cardholder=sanitize_id(cardholder),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        cardholder: str,
        params: Optional["CardholderUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Cardholder":
        """
        Updates the specified Issuing Cardholder object by setting the values of the parameters passed. Any parameters not provided will be left unchanged.
        """
        return cast(
            "Cardholder",
            self._request(
                "post",
                "/v1/issuing/cardholders/{cardholder}".format(
                    cardholder=sanitize_id(cardholder),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        cardholder: str,
        params: Optional["CardholderUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Cardholder":
        """
        Updates the specified Issuing Cardholder object by setting the values of the parameters passed. Any parameters not provided will be left unchanged.
        """
        return cast(
            "Cardholder",
            await self._request_async(
                "post",
                "/v1/issuing/cardholders/{cardholder}".format(
                    cardholder=sanitize_id(cardholder),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
