# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe._subscription_item import SubscriptionItem
    from stripe.params._subscription_item_create_params import (
        SubscriptionItemCreateParams,
    )
    from stripe.params._subscription_item_delete_params import (
        SubscriptionItemDeleteParams,
    )
    from stripe.params._subscription_item_list_params import (
        SubscriptionItemListParams,
    )
    from stripe.params._subscription_item_retrieve_params import (
        SubscriptionItemRetrieveParams,
    )
    from stripe.params._subscription_item_update_params import (
        SubscriptionItemUpdateParams,
    )


class SubscriptionItemService(StripeService):
    def delete(
        self,
        item: str,
        params: Optional["SubscriptionItemDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "SubscriptionItem":
        """
        Deletes an item from the subscription. Removing a subscription item from a subscription will not cancel the subscription.
        """
        return cast(
            "SubscriptionItem",
            self._request(
                "delete",
                "/v1/subscription_items/{item}".format(item=sanitize_id(item)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def delete_async(
        self,
        item: str,
        params: Optional["SubscriptionItemDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "SubscriptionItem":
        """
        Deletes an item from the subscription. Removing a subscription item from a subscription will not cancel the subscription.
        """
        return cast(
            "SubscriptionItem",
            await self._request_async(
                "delete",
                "/v1/subscription_items/{item}".format(item=sanitize_id(item)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        item: str,
        params: Optional["SubscriptionItemRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "SubscriptionItem":
        """
        Retrieves the subscription item with the given ID.
        """
        return cast(
            "SubscriptionItem",
            self._request(
                "get",
                "/v1/subscription_items/{item}".format(item=sanitize_id(item)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        item: str,
        params: Optional["SubscriptionItemRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "SubscriptionItem":
        """
        Retrieves the subscription item with the given ID.
        """
        return cast(
            "SubscriptionItem",
            await self._request_async(
                "get",
                "/v1/subscription_items/{item}".format(item=sanitize_id(item)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        item: str,
        params: Optional["SubscriptionItemUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "SubscriptionItem":
        """
        Updates the plan or quantity of an item on a current subscription.
        """
        return cast(
            "SubscriptionItem",
            self._request(
                "post",
                "/v1/subscription_items/{item}".format(item=sanitize_id(item)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        item: str,
        params: Optional["SubscriptionItemUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "SubscriptionItem":
        """
        Updates the plan or quantity of an item on a current subscription.
        """
        return cast(
            "SubscriptionItem",
            await self._request_async(
                "post",
                "/v1/subscription_items/{item}".format(item=sanitize_id(item)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def list(
        self,
        params: "SubscriptionItemListParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[SubscriptionItem]":
        """
        Returns a list of your subscription items for a given subscription.
        """
        return cast(
            "ListObject[SubscriptionItem]",
            self._request(
                "get",
                "/v1/subscription_items",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: "SubscriptionItemListParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[SubscriptionItem]":
        """
        Returns a list of your subscription items for a given subscription.
        """
        return cast(
            "ListObject[SubscriptionItem]",
            await self._request_async(
                "get",
                "/v1/subscription_items",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: "SubscriptionItemCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "SubscriptionItem":
        """
        Adds a new item to an existing subscription. No existing items will be changed or replaced.
        """
        return cast(
            "SubscriptionItem",
            self._request(
                "post",
                "/v1/subscription_items",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "SubscriptionItemCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "SubscriptionItem":
        """
        Adds a new item to an existing subscription. No existing items will be changed or replaced.
        """
        return cast(
            "SubscriptionItem",
            await self._request_async(
                "post",
                "/v1/subscription_items",
                base_address="api",
                params=params,
                options=options,
            ),
        )
