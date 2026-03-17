# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.params.radar._value_list_item_create_params import (
        ValueListItemCreateParams,
    )
    from stripe.params.radar._value_list_item_delete_params import (
        ValueListItemDeleteParams,
    )
    from stripe.params.radar._value_list_item_list_params import (
        ValueListItemListParams,
    )
    from stripe.params.radar._value_list_item_retrieve_params import (
        ValueListItemRetrieveParams,
    )
    from stripe.radar._value_list_item import ValueListItem


class ValueListItemService(StripeService):
    def delete(
        self,
        item: str,
        params: Optional["ValueListItemDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ValueListItem":
        """
        Deletes a ValueListItem object, removing it from its parent value list.
        """
        return cast(
            "ValueListItem",
            self._request(
                "delete",
                "/v1/radar/value_list_items/{item}".format(
                    item=sanitize_id(item),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def delete_async(
        self,
        item: str,
        params: Optional["ValueListItemDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ValueListItem":
        """
        Deletes a ValueListItem object, removing it from its parent value list.
        """
        return cast(
            "ValueListItem",
            await self._request_async(
                "delete",
                "/v1/radar/value_list_items/{item}".format(
                    item=sanitize_id(item),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        item: str,
        params: Optional["ValueListItemRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ValueListItem":
        """
        Retrieves a ValueListItem object.
        """
        return cast(
            "ValueListItem",
            self._request(
                "get",
                "/v1/radar/value_list_items/{item}".format(
                    item=sanitize_id(item),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        item: str,
        params: Optional["ValueListItemRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ValueListItem":
        """
        Retrieves a ValueListItem object.
        """
        return cast(
            "ValueListItem",
            await self._request_async(
                "get",
                "/v1/radar/value_list_items/{item}".format(
                    item=sanitize_id(item),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def list(
        self,
        params: "ValueListItemListParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[ValueListItem]":
        """
        Returns a list of ValueListItem objects. The objects are sorted in descending order by creation date, with the most recently created object appearing first.
        """
        return cast(
            "ListObject[ValueListItem]",
            self._request(
                "get",
                "/v1/radar/value_list_items",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: "ValueListItemListParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[ValueListItem]":
        """
        Returns a list of ValueListItem objects. The objects are sorted in descending order by creation date, with the most recently created object appearing first.
        """
        return cast(
            "ListObject[ValueListItem]",
            await self._request_async(
                "get",
                "/v1/radar/value_list_items",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: "ValueListItemCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ValueListItem":
        """
        Creates a new ValueListItem object, which is added to the specified parent value list.
        """
        return cast(
            "ValueListItem",
            self._request(
                "post",
                "/v1/radar/value_list_items",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "ValueListItemCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ValueListItem":
        """
        Creates a new ValueListItem object, which is added to the specified parent value list.
        """
        return cast(
            "ValueListItem",
            await self._request_async(
                "post",
                "/v1/radar/value_list_items",
                base_address="api",
                params=params,
                options=options,
            ),
        )
