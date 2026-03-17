# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.params.radar._value_list_create_params import (
        ValueListCreateParams,
    )
    from stripe.params.radar._value_list_delete_params import (
        ValueListDeleteParams,
    )
    from stripe.params.radar._value_list_list_params import ValueListListParams
    from stripe.params.radar._value_list_retrieve_params import (
        ValueListRetrieveParams,
    )
    from stripe.params.radar._value_list_update_params import (
        ValueListUpdateParams,
    )
    from stripe.radar._value_list import ValueList


class ValueListService(StripeService):
    def delete(
        self,
        value_list: str,
        params: Optional["ValueListDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ValueList":
        """
        Deletes a ValueList object, also deleting any items contained within the value list. To be deleted, a value list must not be referenced in any rules.
        """
        return cast(
            "ValueList",
            self._request(
                "delete",
                "/v1/radar/value_lists/{value_list}".format(
                    value_list=sanitize_id(value_list),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def delete_async(
        self,
        value_list: str,
        params: Optional["ValueListDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ValueList":
        """
        Deletes a ValueList object, also deleting any items contained within the value list. To be deleted, a value list must not be referenced in any rules.
        """
        return cast(
            "ValueList",
            await self._request_async(
                "delete",
                "/v1/radar/value_lists/{value_list}".format(
                    value_list=sanitize_id(value_list),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        value_list: str,
        params: Optional["ValueListRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ValueList":
        """
        Retrieves a ValueList object.
        """
        return cast(
            "ValueList",
            self._request(
                "get",
                "/v1/radar/value_lists/{value_list}".format(
                    value_list=sanitize_id(value_list),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        value_list: str,
        params: Optional["ValueListRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ValueList":
        """
        Retrieves a ValueList object.
        """
        return cast(
            "ValueList",
            await self._request_async(
                "get",
                "/v1/radar/value_lists/{value_list}".format(
                    value_list=sanitize_id(value_list),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        value_list: str,
        params: Optional["ValueListUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ValueList":
        """
        Updates a ValueList object by setting the values of the parameters passed. Any parameters not provided will be left unchanged. Note that item_type is immutable.
        """
        return cast(
            "ValueList",
            self._request(
                "post",
                "/v1/radar/value_lists/{value_list}".format(
                    value_list=sanitize_id(value_list),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        value_list: str,
        params: Optional["ValueListUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ValueList":
        """
        Updates a ValueList object by setting the values of the parameters passed. Any parameters not provided will be left unchanged. Note that item_type is immutable.
        """
        return cast(
            "ValueList",
            await self._request_async(
                "post",
                "/v1/radar/value_lists/{value_list}".format(
                    value_list=sanitize_id(value_list),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def list(
        self,
        params: Optional["ValueListListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[ValueList]":
        """
        Returns a list of ValueList objects. The objects are sorted in descending order by creation date, with the most recently created object appearing first.
        """
        return cast(
            "ListObject[ValueList]",
            self._request(
                "get",
                "/v1/radar/value_lists",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["ValueListListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[ValueList]":
        """
        Returns a list of ValueList objects. The objects are sorted in descending order by creation date, with the most recently created object appearing first.
        """
        return cast(
            "ListObject[ValueList]",
            await self._request_async(
                "get",
                "/v1/radar/value_lists",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: "ValueListCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ValueList":
        """
        Creates a new ValueList object, which can then be referenced in rules.
        """
        return cast(
            "ValueList",
            self._request(
                "post",
                "/v1/radar/value_lists",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "ValueListCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ValueList":
        """
        Creates a new ValueList object, which can then be referenced in rules.
        """
        return cast(
            "ValueList",
            await self._request_async(
                "post",
                "/v1/radar/value_lists",
                base_address="api",
                params=params,
                options=options,
            ),
        )
