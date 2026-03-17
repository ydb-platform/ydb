# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._stripe_object import StripeObject
from typing import ClassVar, Dict, List, Optional
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.climate._supplier import Supplier
    from stripe.params.climate._product_list_params import ProductListParams
    from stripe.params.climate._product_retrieve_params import (
        ProductRetrieveParams,
    )


class Product(ListableAPIResource["Product"]):
    """
    A Climate product represents a type of carbon removal unit available for reservation.
    You can retrieve it to see the current price and availability.
    """

    OBJECT_NAME: ClassVar[Literal["climate.product"]] = "climate.product"

    class CurrentPricesPerMetricTon(StripeObject):
        amount_fees: int
        """
        Fees for one metric ton of carbon removal in the currency's smallest unit.
        """
        amount_subtotal: int
        """
        Subtotal for one metric ton of carbon removal (excluding fees) in the currency's smallest unit.
        """
        amount_total: int
        """
        Total for one metric ton of carbon removal (including fees) in the currency's smallest unit.
        """

    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    current_prices_per_metric_ton: Dict[str, CurrentPricesPerMetricTon]
    """
    Current prices for a metric ton of carbon removal in a currency's smallest unit.
    """
    delivery_year: Optional[int]
    """
    The year in which the carbon removal is expected to be delivered.
    """
    id: str
    """
    Unique identifier for the object. For convenience, Climate product IDs are human-readable strings
    that start with `climsku_`. See [carbon removal inventory](https://stripe.com/docs/climate/orders/carbon-removal-inventory)
    for a list of available carbon removal products.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    metric_tons_available: str
    """
    The quantity of metric tons available for reservation.
    """
    name: str
    """
    The Climate product's name.
    """
    object: Literal["climate.product"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    suppliers: List["Supplier"]
    """
    The carbon removal suppliers that fulfill orders for this Climate product.
    """

    @classmethod
    def list(
        cls, **params: Unpack["ProductListParams"]
    ) -> ListObject["Product"]:
        """
        Lists all available Climate product objects.
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
        cls, **params: Unpack["ProductListParams"]
    ) -> ListObject["Product"]:
        """
        Lists all available Climate product objects.
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
        cls, id: str, **params: Unpack["ProductRetrieveParams"]
    ) -> "Product":
        """
        Retrieves the details of a Climate product with the given ID.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["ProductRetrieveParams"]
    ) -> "Product":
        """
        Retrieves the details of a Climate product with the given ID.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    _inner_class_types = {
        "current_prices_per_metric_ton": CurrentPricesPerMetricTon,
    }
