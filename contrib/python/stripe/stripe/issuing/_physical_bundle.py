# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._stripe_object import StripeObject
from typing import ClassVar
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params.issuing._physical_bundle_list_params import (
        PhysicalBundleListParams,
    )
    from stripe.params.issuing._physical_bundle_retrieve_params import (
        PhysicalBundleRetrieveParams,
    )


class PhysicalBundle(ListableAPIResource["PhysicalBundle"]):
    """
    A Physical Bundle represents the bundle of physical items - card stock, carrier letter, and envelope - that is shipped to a cardholder when you create a physical card.
    """

    OBJECT_NAME: ClassVar[Literal["issuing.physical_bundle"]] = (
        "issuing.physical_bundle"
    )

    class Features(StripeObject):
        card_logo: Literal["optional", "required", "unsupported"]
        """
        The policy for how to use card logo images in a card design with this physical bundle.
        """
        carrier_text: Literal["optional", "required", "unsupported"]
        """
        The policy for how to use carrier letter text in a card design with this physical bundle.
        """
        second_line: Literal["optional", "required", "unsupported"]
        """
        The policy for how to use a second line on a card with this physical bundle.
        """

    features: Features
    id: str
    """
    Unique identifier for the object.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    name: str
    """
    Friendly display name.
    """
    object: Literal["issuing.physical_bundle"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    status: Literal["active", "inactive", "review"]
    """
    Whether this physical bundle can be used to create cards.
    """
    type: Literal["custom", "standard"]
    """
    Whether this physical bundle is a standard Stripe offering or custom-made for you.
    """

    @classmethod
    def list(
        cls, **params: Unpack["PhysicalBundleListParams"]
    ) -> ListObject["PhysicalBundle"]:
        """
        Returns a list of physical bundle objects. The objects are sorted in descending order by creation date, with the most recently created object appearing first.
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
        cls, **params: Unpack["PhysicalBundleListParams"]
    ) -> ListObject["PhysicalBundle"]:
        """
        Returns a list of physical bundle objects. The objects are sorted in descending order by creation date, with the most recently created object appearing first.
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
        cls, id: str, **params: Unpack["PhysicalBundleRetrieveParams"]
    ) -> "PhysicalBundle":
        """
        Retrieves a physical bundle object.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["PhysicalBundleRetrieveParams"]
    ) -> "PhysicalBundle":
        """
        Retrieves a physical bundle object.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    _inner_class_types = {"features": Features}
