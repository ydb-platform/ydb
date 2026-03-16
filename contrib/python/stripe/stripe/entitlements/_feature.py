# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._updateable_api_resource import UpdateableAPIResource
from stripe._util import sanitize_id
from typing import ClassVar, Dict, cast
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params.entitlements._feature_create_params import (
        FeatureCreateParams,
    )
    from stripe.params.entitlements._feature_list_params import (
        FeatureListParams,
    )
    from stripe.params.entitlements._feature_modify_params import (
        FeatureModifyParams,
    )
    from stripe.params.entitlements._feature_retrieve_params import (
        FeatureRetrieveParams,
    )


class Feature(
    CreateableAPIResource["Feature"],
    ListableAPIResource["Feature"],
    UpdateableAPIResource["Feature"],
):
    """
    A feature represents a monetizable ability or functionality in your system.
    Features can be assigned to products, and when those products are purchased, Stripe will create an entitlement to the feature for the purchasing customer.
    """

    OBJECT_NAME: ClassVar[Literal["entitlements.feature"]] = (
        "entitlements.feature"
    )
    active: bool
    """
    Inactive features cannot be attached to new products and will not be returned from the features list endpoint.
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
    metadata: Dict[str, str]
    """
    Set of key-value pairs that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    name: str
    """
    The feature's name, for your own purpose, not meant to be displayable to the customer.
    """
    object: Literal["entitlements.feature"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """

    @classmethod
    def create(cls, **params: Unpack["FeatureCreateParams"]) -> "Feature":
        """
        Creates a feature
        """
        return cast(
            "Feature",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["FeatureCreateParams"]
    ) -> "Feature":
        """
        Creates a feature
        """
        return cast(
            "Feature",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def list(
        cls, **params: Unpack["FeatureListParams"]
    ) -> ListObject["Feature"]:
        """
        Retrieve a list of features
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
        cls, **params: Unpack["FeatureListParams"]
    ) -> ListObject["Feature"]:
        """
        Retrieve a list of features
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
    def modify(
        cls, id: str, **params: Unpack["FeatureModifyParams"]
    ) -> "Feature":
        """
        Update a feature's metadata or permanently deactivate it.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Feature",
            cls._static_request(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    async def modify_async(
        cls, id: str, **params: Unpack["FeatureModifyParams"]
    ) -> "Feature":
        """
        Update a feature's metadata or permanently deactivate it.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Feature",
            await cls._static_request_async(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["FeatureRetrieveParams"]
    ) -> "Feature":
        """
        Retrieves a feature
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["FeatureRetrieveParams"]
    ) -> "Feature":
        """
        Retrieves a feature
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance
