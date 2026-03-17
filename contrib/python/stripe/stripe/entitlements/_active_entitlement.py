# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._expandable_field import ExpandableField
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from typing import ClassVar
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.entitlements._feature import Feature
    from stripe.params.entitlements._active_entitlement_list_params import (
        ActiveEntitlementListParams,
    )
    from stripe.params.entitlements._active_entitlement_retrieve_params import (
        ActiveEntitlementRetrieveParams,
    )


class ActiveEntitlement(ListableAPIResource["ActiveEntitlement"]):
    """
    An active entitlement describes access to a feature for a customer.
    """

    OBJECT_NAME: ClassVar[Literal["entitlements.active_entitlement"]] = (
        "entitlements.active_entitlement"
    )
    feature: ExpandableField["Feature"]
    """
    The [Feature](https://docs.stripe.com/api/entitlements/feature) that the customer is entitled to.
    """
    id: str
    """
    Unique identifier for the object.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    lookup_key: str
    """
    A unique key you provide as your own system identifier. This may be up to 80 characters.
    """
    object: Literal["entitlements.active_entitlement"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """

    @classmethod
    def list(
        cls, **params: Unpack["ActiveEntitlementListParams"]
    ) -> ListObject["ActiveEntitlement"]:
        """
        Retrieve a list of active entitlements for a customer
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
        cls, **params: Unpack["ActiveEntitlementListParams"]
    ) -> ListObject["ActiveEntitlement"]:
        """
        Retrieve a list of active entitlements for a customer
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
        cls, id: str, **params: Unpack["ActiveEntitlementRetrieveParams"]
    ) -> "ActiveEntitlement":
        """
        Retrieve an active entitlement
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["ActiveEntitlementRetrieveParams"]
    ) -> "ActiveEntitlement":
        """
        Retrieve an active entitlement
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance
