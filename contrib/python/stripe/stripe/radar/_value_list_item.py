# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._deletable_api_resource import DeletableAPIResource
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._util import class_method_variant, sanitize_id
from typing import ClassVar, Optional, cast, overload
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
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


class ValueListItem(
    CreateableAPIResource["ValueListItem"],
    DeletableAPIResource["ValueListItem"],
    ListableAPIResource["ValueListItem"],
):
    """
    Value list items allow you to add specific values to a given Radar value list, which can then be used in rules.

    Related guide: [Managing list items](https://docs.stripe.com/radar/lists#managing-list-items)
    """

    OBJECT_NAME: ClassVar[Literal["radar.value_list_item"]] = (
        "radar.value_list_item"
    )
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    created_by: str
    """
    The name or email address of the user who added this item to the value list.
    """
    deleted: Optional[Literal[True]]
    """
    Always true for a deleted object
    """
    id: str
    """
    Unique identifier for the object.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    object: Literal["radar.value_list_item"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    value: str
    """
    The value of the item.
    """
    value_list: str
    """
    The identifier of the value list this item belongs to.
    """

    @classmethod
    def create(
        cls, **params: Unpack["ValueListItemCreateParams"]
    ) -> "ValueListItem":
        """
        Creates a new ValueListItem object, which is added to the specified parent value list.
        """
        return cast(
            "ValueListItem",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["ValueListItemCreateParams"]
    ) -> "ValueListItem":
        """
        Creates a new ValueListItem object, which is added to the specified parent value list.
        """
        return cast(
            "ValueListItem",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def _cls_delete(
        cls, sid: str, **params: Unpack["ValueListItemDeleteParams"]
    ) -> "ValueListItem":
        """
        Deletes a ValueListItem object, removing it from its parent value list.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(sid))
        return cast(
            "ValueListItem",
            cls._static_request(
                "delete",
                url,
                params=params,
            ),
        )

    @overload
    @staticmethod
    def delete(
        sid: str, **params: Unpack["ValueListItemDeleteParams"]
    ) -> "ValueListItem":
        """
        Deletes a ValueListItem object, removing it from its parent value list.
        """
        ...

    @overload
    def delete(
        self, **params: Unpack["ValueListItemDeleteParams"]
    ) -> "ValueListItem":
        """
        Deletes a ValueListItem object, removing it from its parent value list.
        """
        ...

    @class_method_variant("_cls_delete")
    def delete(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["ValueListItemDeleteParams"]
    ) -> "ValueListItem":
        """
        Deletes a ValueListItem object, removing it from its parent value list.
        """
        return self._request_and_refresh(
            "delete",
            self.instance_url(),
            params=params,
        )

    @classmethod
    async def _cls_delete_async(
        cls, sid: str, **params: Unpack["ValueListItemDeleteParams"]
    ) -> "ValueListItem":
        """
        Deletes a ValueListItem object, removing it from its parent value list.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(sid))
        return cast(
            "ValueListItem",
            await cls._static_request_async(
                "delete",
                url,
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def delete_async(
        sid: str, **params: Unpack["ValueListItemDeleteParams"]
    ) -> "ValueListItem":
        """
        Deletes a ValueListItem object, removing it from its parent value list.
        """
        ...

    @overload
    async def delete_async(
        self, **params: Unpack["ValueListItemDeleteParams"]
    ) -> "ValueListItem":
        """
        Deletes a ValueListItem object, removing it from its parent value list.
        """
        ...

    @class_method_variant("_cls_delete_async")
    async def delete_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["ValueListItemDeleteParams"]
    ) -> "ValueListItem":
        """
        Deletes a ValueListItem object, removing it from its parent value list.
        """
        return await self._request_and_refresh_async(
            "delete",
            self.instance_url(),
            params=params,
        )

    @classmethod
    def list(
        cls, **params: Unpack["ValueListItemListParams"]
    ) -> ListObject["ValueListItem"]:
        """
        Returns a list of ValueListItem objects. The objects are sorted in descending order by creation date, with the most recently created object appearing first.
        """
        result = cls._static_request(
            "get",
            cls.class_url(),
            params=params,
        )
        if not isinstance(result, ListObject):
            raise TypeError(
                "Expected list object from API, got %s"
                % (type(result).__name__)
            )

        return result

    @classmethod
    async def list_async(
        cls, **params: Unpack["ValueListItemListParams"]
    ) -> ListObject["ValueListItem"]:
        """
        Returns a list of ValueListItem objects. The objects are sorted in descending order by creation date, with the most recently created object appearing first.
        """
        result = await cls._static_request_async(
            "get",
            cls.class_url(),
            params=params,
        )
        if not isinstance(result, ListObject):
            raise TypeError(
                "Expected list object from API, got %s"
                % (type(result).__name__)
            )

        return result

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["ValueListItemRetrieveParams"]
    ) -> "ValueListItem":
        """
        Retrieves a ValueListItem object.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["ValueListItemRetrieveParams"]
    ) -> "ValueListItem":
        """
        Retrieves a ValueListItem object.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance
