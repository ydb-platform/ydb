# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.issuing._card import Card
    from stripe.params.issuing._card_create_params import CardCreateParams
    from stripe.params.issuing._card_list_params import CardListParams
    from stripe.params.issuing._card_retrieve_params import CardRetrieveParams
    from stripe.params.issuing._card_update_params import CardUpdateParams


class CardService(StripeService):
    def list(
        self,
        params: Optional["CardListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Card]":
        """
        Returns a list of Issuing Card objects. The objects are sorted in descending order by creation date, with the most recently created object appearing first.
        """
        return cast(
            "ListObject[Card]",
            self._request(
                "get",
                "/v1/issuing/cards",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["CardListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Card]":
        """
        Returns a list of Issuing Card objects. The objects are sorted in descending order by creation date, with the most recently created object appearing first.
        """
        return cast(
            "ListObject[Card]",
            await self._request_async(
                "get",
                "/v1/issuing/cards",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: "CardCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Card":
        """
        Creates an Issuing Card object.
        """
        return cast(
            "Card",
            self._request(
                "post",
                "/v1/issuing/cards",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "CardCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Card":
        """
        Creates an Issuing Card object.
        """
        return cast(
            "Card",
            await self._request_async(
                "post",
                "/v1/issuing/cards",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        card: str,
        params: Optional["CardRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Card":
        """
        Retrieves an Issuing Card object.
        """
        return cast(
            "Card",
            self._request(
                "get",
                "/v1/issuing/cards/{card}".format(card=sanitize_id(card)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        card: str,
        params: Optional["CardRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Card":
        """
        Retrieves an Issuing Card object.
        """
        return cast(
            "Card",
            await self._request_async(
                "get",
                "/v1/issuing/cards/{card}".format(card=sanitize_id(card)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        card: str,
        params: Optional["CardUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Card":
        """
        Updates the specified Issuing Card object by setting the values of the parameters passed. Any parameters not provided will be left unchanged.
        """
        return cast(
            "Card",
            self._request(
                "post",
                "/v1/issuing/cards/{card}".format(card=sanitize_id(card)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        card: str,
        params: Optional["CardUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Card":
        """
        Updates the specified Issuing Card object by setting the values of the parameters passed. Any parameters not provided will be left unchanged.
        """
        return cast(
            "Card",
            await self._request_async(
                "post",
                "/v1/issuing/cards/{card}".format(card=sanitize_id(card)),
                base_address="api",
                params=params,
                options=options,
            ),
        )
