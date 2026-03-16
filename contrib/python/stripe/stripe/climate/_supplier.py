# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._stripe_object import StripeObject
from typing import ClassVar, List, Optional
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params.climate._supplier_list_params import SupplierListParams
    from stripe.params.climate._supplier_retrieve_params import (
        SupplierRetrieveParams,
    )


class Supplier(ListableAPIResource["Supplier"]):
    """
    A supplier of carbon removal.
    """

    OBJECT_NAME: ClassVar[Literal["climate.supplier"]] = "climate.supplier"

    class Location(StripeObject):
        city: Optional[str]
        """
        The city where the supplier is located.
        """
        country: str
        """
        Two-letter ISO code representing the country where the supplier is located.
        """
        latitude: Optional[float]
        """
        The geographic latitude where the supplier is located.
        """
        longitude: Optional[float]
        """
        The geographic longitude where the supplier is located.
        """
        region: Optional[str]
        """
        The state/county/province/region where the supplier is located.
        """

    id: str
    """
    Unique identifier for the object.
    """
    info_url: str
    """
    Link to a webpage to learn more about the supplier.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    locations: List[Location]
    """
    The locations in which this supplier operates.
    """
    name: str
    """
    Name of this carbon removal supplier.
    """
    object: Literal["climate.supplier"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    removal_pathway: Literal[
        "biomass_carbon_removal_and_storage",
        "direct_air_capture",
        "enhanced_weathering",
    ]
    """
    The scientific pathway used for carbon removal.
    """

    @classmethod
    def list(
        cls, **params: Unpack["SupplierListParams"]
    ) -> ListObject["Supplier"]:
        """
        Lists all available Climate supplier objects.
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
        cls, **params: Unpack["SupplierListParams"]
    ) -> ListObject["Supplier"]:
        """
        Lists all available Climate supplier objects.
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
        cls, id: str, **params: Unpack["SupplierRetrieveParams"]
    ) -> "Supplier":
        """
        Retrieves a Climate supplier object.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["SupplierRetrieveParams"]
    ) -> "Supplier":
        """
        Retrieves a Climate supplier object.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    _inner_class_types = {"locations": Location}
