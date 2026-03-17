# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._expandable_field import ExpandableField
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._stripe_object import StripeObject
from stripe._updateable_api_resource import UpdateableAPIResource
from stripe._util import class_method_variant, sanitize_id
from typing import ClassVar, Dict, List, Optional, cast, overload
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.climate._product import Product
    from stripe.climate._supplier import Supplier
    from stripe.params.climate._order_cancel_params import OrderCancelParams
    from stripe.params.climate._order_create_params import OrderCreateParams
    from stripe.params.climate._order_list_params import OrderListParams
    from stripe.params.climate._order_modify_params import OrderModifyParams
    from stripe.params.climate._order_retrieve_params import (
        OrderRetrieveParams,
    )


class Order(
    CreateableAPIResource["Order"],
    ListableAPIResource["Order"],
    UpdateableAPIResource["Order"],
):
    """
    Orders represent your intent to purchase a particular Climate product. When you create an order, the
    payment is deducted from your merchant balance.
    """

    OBJECT_NAME: ClassVar[Literal["climate.order"]] = "climate.order"

    class Beneficiary(StripeObject):
        public_name: str
        """
        Publicly displayable name for the end beneficiary of carbon removal.
        """

    class DeliveryDetail(StripeObject):
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

        delivered_at: int
        """
        Time at which the delivery occurred. Measured in seconds since the Unix epoch.
        """
        location: Optional[Location]
        """
        Specific location of this delivery.
        """
        metric_tons: str
        """
        Quantity of carbon removal supplied by this delivery.
        """
        registry_url: Optional[str]
        """
        Once retired, a URL to the registry entry for the tons from this delivery.
        """
        supplier: "Supplier"
        """
        A supplier of carbon removal.
        """
        _inner_class_types = {"location": Location}

    amount_fees: int
    """
    Total amount of [Frontier](https://frontierclimate.com/)'s service fees in the currency's smallest unit.
    """
    amount_subtotal: int
    """
    Total amount of the carbon removal in the currency's smallest unit.
    """
    amount_total: int
    """
    Total amount of the order including fees in the currency's smallest unit.
    """
    beneficiary: Optional[Beneficiary]
    canceled_at: Optional[int]
    """
    Time at which the order was canceled. Measured in seconds since the Unix epoch.
    """
    cancellation_reason: Optional[
        Literal["expired", "product_unavailable", "requested"]
    ]
    """
    Reason for the cancellation of this order.
    """
    certificate: Optional[str]
    """
    For delivered orders, a URL to a delivery certificate for the order.
    """
    confirmed_at: Optional[int]
    """
    Time at which the order was confirmed. Measured in seconds since the Unix epoch.
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase, representing the currency for this order.
    """
    delayed_at: Optional[int]
    """
    Time at which the order's expected_delivery_year was delayed. Measured in seconds since the Unix epoch.
    """
    delivered_at: Optional[int]
    """
    Time at which the order was delivered. Measured in seconds since the Unix epoch.
    """
    delivery_details: List[DeliveryDetail]
    """
    Details about the delivery of carbon removal for this order.
    """
    expected_delivery_year: int
    """
    The year this order is expected to be delivered.
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
    metric_tons: str
    """
    Quantity of carbon removal that is included in this order.
    """
    object: Literal["climate.order"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    product: ExpandableField["Product"]
    """
    Unique ID for the Climate `Product` this order is purchasing.
    """
    product_substituted_at: Optional[int]
    """
    Time at which the order's product was substituted for a different product. Measured in seconds since the Unix epoch.
    """
    status: Literal[
        "awaiting_funds", "canceled", "confirmed", "delivered", "open"
    ]
    """
    The current status of this order.
    """

    @classmethod
    def _cls_cancel(
        cls, order: str, **params: Unpack["OrderCancelParams"]
    ) -> "Order":
        """
        Cancels a Climate order. You can cancel an order within 24 hours of creation. Stripe refunds the
        reservation amount_subtotal, but not the amount_fees for user-triggered cancellations. Frontier
        might cancel reservations if suppliers fail to deliver. If Frontier cancels the reservation, Stripe
        provides 90 days advance notice and refunds the amount_total.
        """
        return cast(
            "Order",
            cls._static_request(
                "post",
                "/v1/climate/orders/{order}/cancel".format(
                    order=sanitize_id(order)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def cancel(order: str, **params: Unpack["OrderCancelParams"]) -> "Order":
        """
        Cancels a Climate order. You can cancel an order within 24 hours of creation. Stripe refunds the
        reservation amount_subtotal, but not the amount_fees for user-triggered cancellations. Frontier
        might cancel reservations if suppliers fail to deliver. If Frontier cancels the reservation, Stripe
        provides 90 days advance notice and refunds the amount_total.
        """
        ...

    @overload
    def cancel(self, **params: Unpack["OrderCancelParams"]) -> "Order":
        """
        Cancels a Climate order. You can cancel an order within 24 hours of creation. Stripe refunds the
        reservation amount_subtotal, but not the amount_fees for user-triggered cancellations. Frontier
        might cancel reservations if suppliers fail to deliver. If Frontier cancels the reservation, Stripe
        provides 90 days advance notice and refunds the amount_total.
        """
        ...

    @class_method_variant("_cls_cancel")
    def cancel(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["OrderCancelParams"]
    ) -> "Order":
        """
        Cancels a Climate order. You can cancel an order within 24 hours of creation. Stripe refunds the
        reservation amount_subtotal, but not the amount_fees for user-triggered cancellations. Frontier
        might cancel reservations if suppliers fail to deliver. If Frontier cancels the reservation, Stripe
        provides 90 days advance notice and refunds the amount_total.
        """
        return cast(
            "Order",
            self._request(
                "post",
                "/v1/climate/orders/{order}/cancel".format(
                    order=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_cancel_async(
        cls, order: str, **params: Unpack["OrderCancelParams"]
    ) -> "Order":
        """
        Cancels a Climate order. You can cancel an order within 24 hours of creation. Stripe refunds the
        reservation amount_subtotal, but not the amount_fees for user-triggered cancellations. Frontier
        might cancel reservations if suppliers fail to deliver. If Frontier cancels the reservation, Stripe
        provides 90 days advance notice and refunds the amount_total.
        """
        return cast(
            "Order",
            await cls._static_request_async(
                "post",
                "/v1/climate/orders/{order}/cancel".format(
                    order=sanitize_id(order)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def cancel_async(
        order: str, **params: Unpack["OrderCancelParams"]
    ) -> "Order":
        """
        Cancels a Climate order. You can cancel an order within 24 hours of creation. Stripe refunds the
        reservation amount_subtotal, but not the amount_fees for user-triggered cancellations. Frontier
        might cancel reservations if suppliers fail to deliver. If Frontier cancels the reservation, Stripe
        provides 90 days advance notice and refunds the amount_total.
        """
        ...

    @overload
    async def cancel_async(
        self, **params: Unpack["OrderCancelParams"]
    ) -> "Order":
        """
        Cancels a Climate order. You can cancel an order within 24 hours of creation. Stripe refunds the
        reservation amount_subtotal, but not the amount_fees for user-triggered cancellations. Frontier
        might cancel reservations if suppliers fail to deliver. If Frontier cancels the reservation, Stripe
        provides 90 days advance notice and refunds the amount_total.
        """
        ...

    @class_method_variant("_cls_cancel_async")
    async def cancel_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["OrderCancelParams"]
    ) -> "Order":
        """
        Cancels a Climate order. You can cancel an order within 24 hours of creation. Stripe refunds the
        reservation amount_subtotal, but not the amount_fees for user-triggered cancellations. Frontier
        might cancel reservations if suppliers fail to deliver. If Frontier cancels the reservation, Stripe
        provides 90 days advance notice and refunds the amount_total.
        """
        return cast(
            "Order",
            await self._request_async(
                "post",
                "/v1/climate/orders/{order}/cancel".format(
                    order=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def create(cls, **params: Unpack["OrderCreateParams"]) -> "Order":
        """
        Creates a Climate order object for a given Climate product. The order will be processed immediately
        after creation and payment will be deducted your Stripe balance.
        """
        return cast(
            "Order",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["OrderCreateParams"]
    ) -> "Order":
        """
        Creates a Climate order object for a given Climate product. The order will be processed immediately
        after creation and payment will be deducted your Stripe balance.
        """
        return cast(
            "Order",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def list(cls, **params: Unpack["OrderListParams"]) -> ListObject["Order"]:
        """
        Lists all Climate order objects. The orders are returned sorted by creation date, with the
        most recently created orders appearing first.
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
        cls, **params: Unpack["OrderListParams"]
    ) -> ListObject["Order"]:
        """
        Lists all Climate order objects. The orders are returned sorted by creation date, with the
        most recently created orders appearing first.
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
    def modify(cls, id: str, **params: Unpack["OrderModifyParams"]) -> "Order":
        """
        Updates the specified order by setting the values of the parameters passed.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Order",
            cls._static_request(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    async def modify_async(
        cls, id: str, **params: Unpack["OrderModifyParams"]
    ) -> "Order":
        """
        Updates the specified order by setting the values of the parameters passed.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Order",
            await cls._static_request_async(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["OrderRetrieveParams"]
    ) -> "Order":
        """
        Retrieves the details of a Climate order object with the given ID.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["OrderRetrieveParams"]
    ) -> "Order":
        """
        Retrieves the details of a Climate order object with the given ID.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    _inner_class_types = {
        "beneficiary": Beneficiary,
        "delivery_details": DeliveryDetail,
    }
