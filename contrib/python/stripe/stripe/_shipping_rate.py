# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._expandable_field import ExpandableField
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._stripe_object import StripeObject
from stripe._updateable_api_resource import UpdateableAPIResource
from stripe._util import sanitize_id
from typing import ClassVar, Dict, Optional, cast
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._tax_code import TaxCode
    from stripe.params._shipping_rate_create_params import (
        ShippingRateCreateParams,
    )
    from stripe.params._shipping_rate_list_params import ShippingRateListParams
    from stripe.params._shipping_rate_modify_params import (
        ShippingRateModifyParams,
    )
    from stripe.params._shipping_rate_retrieve_params import (
        ShippingRateRetrieveParams,
    )


class ShippingRate(
    CreateableAPIResource["ShippingRate"],
    ListableAPIResource["ShippingRate"],
    UpdateableAPIResource["ShippingRate"],
):
    """
    Shipping rates describe the price of shipping presented to your customers and
    applied to a purchase. For more information, see [Charge for shipping](https://docs.stripe.com/payments/during-payment/charge-shipping).
    """

    OBJECT_NAME: ClassVar[Literal["shipping_rate"]] = "shipping_rate"

    class DeliveryEstimate(StripeObject):
        class Maximum(StripeObject):
            unit: Literal["business_day", "day", "hour", "month", "week"]
            """
            A unit of time.
            """
            value: int
            """
            Must be greater than 0.
            """

        class Minimum(StripeObject):
            unit: Literal["business_day", "day", "hour", "month", "week"]
            """
            A unit of time.
            """
            value: int
            """
            Must be greater than 0.
            """

        maximum: Optional[Maximum]
        """
        The upper bound of the estimated range. If empty, represents no upper bound i.e., infinite.
        """
        minimum: Optional[Minimum]
        """
        The lower bound of the estimated range. If empty, represents no lower bound.
        """
        _inner_class_types = {"maximum": Maximum, "minimum": Minimum}

    class FixedAmount(StripeObject):
        class CurrencyOptions(StripeObject):
            amount: int
            """
            A non-negative integer in cents representing how much to charge.
            """
            tax_behavior: Literal["exclusive", "inclusive", "unspecified"]
            """
            Specifies whether the rate is considered inclusive of taxes or exclusive of taxes. One of `inclusive`, `exclusive`, or `unspecified`.
            """

        amount: int
        """
        A non-negative integer in cents representing how much to charge.
        """
        currency: str
        """
        Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
        """
        currency_options: Optional[Dict[str, CurrencyOptions]]
        """
        Shipping rates defined in each available currency option. Each key must be a three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html) and a [supported currency](https://stripe.com/docs/currencies).
        """
        _inner_class_types = {"currency_options": CurrencyOptions}
        _inner_class_dicts = ["currency_options"]

    active: bool
    """
    Whether the shipping rate can be used for new purchases. Defaults to `true`.
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    delivery_estimate: Optional[DeliveryEstimate]
    """
    The estimated range for how long shipping will take, meant to be displayable to the customer. This will appear on CheckoutSessions.
    """
    display_name: Optional[str]
    """
    The name of the shipping rate, meant to be displayable to the customer. This will appear on CheckoutSessions.
    """
    fixed_amount: Optional[FixedAmount]
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
    object: Literal["shipping_rate"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    tax_behavior: Optional[Literal["exclusive", "inclusive", "unspecified"]]
    """
    Specifies whether the rate is considered inclusive of taxes or exclusive of taxes. One of `inclusive`, `exclusive`, or `unspecified`.
    """
    tax_code: Optional[ExpandableField["TaxCode"]]
    """
    A [tax code](https://docs.stripe.com/tax/tax-categories) ID. The Shipping tax code is `txcd_92010001`.
    """
    type: Literal["fixed_amount"]
    """
    The type of calculation to use on the shipping rate.
    """

    @classmethod
    def create(
        cls, **params: Unpack["ShippingRateCreateParams"]
    ) -> "ShippingRate":
        """
        Creates a new shipping rate object.
        """
        return cast(
            "ShippingRate",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["ShippingRateCreateParams"]
    ) -> "ShippingRate":
        """
        Creates a new shipping rate object.
        """
        return cast(
            "ShippingRate",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def list(
        cls, **params: Unpack["ShippingRateListParams"]
    ) -> ListObject["ShippingRate"]:
        """
        Returns a list of your shipping rates.
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
        cls, **params: Unpack["ShippingRateListParams"]
    ) -> ListObject["ShippingRate"]:
        """
        Returns a list of your shipping rates.
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
        cls, id: str, **params: Unpack["ShippingRateModifyParams"]
    ) -> "ShippingRate":
        """
        Updates an existing shipping rate object.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "ShippingRate",
            cls._static_request(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    async def modify_async(
        cls, id: str, **params: Unpack["ShippingRateModifyParams"]
    ) -> "ShippingRate":
        """
        Updates an existing shipping rate object.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "ShippingRate",
            await cls._static_request_async(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["ShippingRateRetrieveParams"]
    ) -> "ShippingRate":
        """
        Returns the shipping rate object with the given ID.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["ShippingRateRetrieveParams"]
    ) -> "ShippingRate":
        """
        Returns the shipping rate object with the given ID.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    _inner_class_types = {
        "delivery_estimate": DeliveryEstimate,
        "fixed_amount": FixedAmount,
    }
