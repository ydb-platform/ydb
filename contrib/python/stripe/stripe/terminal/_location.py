# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._deletable_api_resource import DeletableAPIResource
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._stripe_object import StripeObject
from stripe._updateable_api_resource import UpdateableAPIResource
from stripe._util import class_method_variant, sanitize_id
from typing import ClassVar, Dict, Optional, cast, overload
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params.terminal._location_create_params import (
        LocationCreateParams,
    )
    from stripe.params.terminal._location_delete_params import (
        LocationDeleteParams,
    )
    from stripe.params.terminal._location_list_params import LocationListParams
    from stripe.params.terminal._location_modify_params import (
        LocationModifyParams,
    )
    from stripe.params.terminal._location_retrieve_params import (
        LocationRetrieveParams,
    )


class Location(
    CreateableAPIResource["Location"],
    DeletableAPIResource["Location"],
    ListableAPIResource["Location"],
    UpdateableAPIResource["Location"],
):
    """
    A Location represents a grouping of readers.

    Related guide: [Fleet management](https://docs.stripe.com/terminal/fleet/locations)
    """

    OBJECT_NAME: ClassVar[Literal["terminal.location"]] = "terminal.location"

    class Address(StripeObject):
        city: Optional[str]
        """
        City, district, suburb, town, or village.
        """
        country: Optional[str]
        """
        Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
        """
        line1: Optional[str]
        """
        Address line 1, such as the street, PO Box, or company name.
        """
        line2: Optional[str]
        """
        Address line 2, such as the apartment, suite, unit, or building.
        """
        postal_code: Optional[str]
        """
        ZIP or postal code.
        """
        state: Optional[str]
        """
        State, county, province, or region ([ISO 3166-2](https://en.wikipedia.org/wiki/ISO_3166-2)).
        """

    class AddressKana(StripeObject):
        city: Optional[str]
        """
        City/Ward.
        """
        country: Optional[str]
        """
        Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
        """
        line1: Optional[str]
        """
        Block/Building number.
        """
        line2: Optional[str]
        """
        Building details.
        """
        postal_code: Optional[str]
        """
        ZIP or postal code.
        """
        state: Optional[str]
        """
        Prefecture.
        """
        town: Optional[str]
        """
        Town/cho-me.
        """

    class AddressKanji(StripeObject):
        city: Optional[str]
        """
        City/Ward.
        """
        country: Optional[str]
        """
        Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
        """
        line1: Optional[str]
        """
        Block/Building number.
        """
        line2: Optional[str]
        """
        Building details.
        """
        postal_code: Optional[str]
        """
        ZIP or postal code.
        """
        state: Optional[str]
        """
        Prefecture.
        """
        town: Optional[str]
        """
        Town/cho-me.
        """

    address: Address
    address_kana: Optional[AddressKana]
    address_kanji: Optional[AddressKanji]
    configuration_overrides: Optional[str]
    """
    The ID of a configuration that will be used to customize all readers in this location.
    """
    deleted: Optional[Literal[True]]
    """
    Always true for a deleted object
    """
    display_name: str
    """
    The display name of the location.
    """
    display_name_kana: Optional[str]
    """
    The Kana variation of the display name of the location.
    """
    display_name_kanji: Optional[str]
    """
    The Kanji variation of the display name of the location.
    """
    id: str
    """
    Unique identifier for the object.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    metadata: Dict[str, str]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    object: Literal["terminal.location"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    phone: Optional[str]
    """
    The phone number of the location.
    """

    @classmethod
    def create(cls, **params: Unpack["LocationCreateParams"]) -> "Location":
        """
        Creates a new Location object.
        For further details, including which address fields are required in each country, see the [Manage locations](https://docs.stripe.com/docs/terminal/fleet/locations) guide.
        """
        return cast(
            "Location",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["LocationCreateParams"]
    ) -> "Location":
        """
        Creates a new Location object.
        For further details, including which address fields are required in each country, see the [Manage locations](https://docs.stripe.com/docs/terminal/fleet/locations) guide.
        """
        return cast(
            "Location",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def _cls_delete(
        cls, sid: str, **params: Unpack["LocationDeleteParams"]
    ) -> "Location":
        """
        Deletes a Location object.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(sid))
        return cast(
            "Location",
            cls._static_request(
                "delete",
                url,
                params=params,
            ),
        )

    @overload
    @staticmethod
    def delete(
        sid: str, **params: Unpack["LocationDeleteParams"]
    ) -> "Location":
        """
        Deletes a Location object.
        """
        ...

    @overload
    def delete(self, **params: Unpack["LocationDeleteParams"]) -> "Location":
        """
        Deletes a Location object.
        """
        ...

    @class_method_variant("_cls_delete")
    def delete(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["LocationDeleteParams"]
    ) -> "Location":
        """
        Deletes a Location object.
        """
        return self._request_and_refresh(
            "delete",
            self.instance_url(),
            params=params,
        )

    @classmethod
    async def _cls_delete_async(
        cls, sid: str, **params: Unpack["LocationDeleteParams"]
    ) -> "Location":
        """
        Deletes a Location object.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(sid))
        return cast(
            "Location",
            await cls._static_request_async(
                "delete",
                url,
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def delete_async(
        sid: str, **params: Unpack["LocationDeleteParams"]
    ) -> "Location":
        """
        Deletes a Location object.
        """
        ...

    @overload
    async def delete_async(
        self, **params: Unpack["LocationDeleteParams"]
    ) -> "Location":
        """
        Deletes a Location object.
        """
        ...

    @class_method_variant("_cls_delete_async")
    async def delete_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["LocationDeleteParams"]
    ) -> "Location":
        """
        Deletes a Location object.
        """
        return await self._request_and_refresh_async(
            "delete",
            self.instance_url(),
            params=params,
        )

    @classmethod
    def list(
        cls, **params: Unpack["LocationListParams"]
    ) -> ListObject["Location"]:
        """
        Returns a list of Location objects.
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
        cls, **params: Unpack["LocationListParams"]
    ) -> ListObject["Location"]:
        """
        Returns a list of Location objects.
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
        cls, id: str, **params: Unpack["LocationModifyParams"]
    ) -> "Location":
        """
        Updates a Location object by setting the values of the parameters passed. Any parameters not provided will be left unchanged.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Location",
            cls._static_request(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    async def modify_async(
        cls, id: str, **params: Unpack["LocationModifyParams"]
    ) -> "Location":
        """
        Updates a Location object by setting the values of the parameters passed. Any parameters not provided will be left unchanged.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Location",
            await cls._static_request_async(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["LocationRetrieveParams"]
    ) -> "Location":
        """
        Retrieves a Location object.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["LocationRetrieveParams"]
    ) -> "Location":
        """
        Retrieves a Location object.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    _inner_class_types = {
        "address": Address,
        "address_kana": AddressKana,
        "address_kanji": AddressKanji,
    }
