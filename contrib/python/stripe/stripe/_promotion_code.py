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
    from stripe._coupon import Coupon
    from stripe._customer import Customer
    from stripe.params._promotion_code_create_params import (
        PromotionCodeCreateParams,
    )
    from stripe.params._promotion_code_list_params import (
        PromotionCodeListParams,
    )
    from stripe.params._promotion_code_modify_params import (
        PromotionCodeModifyParams,
    )
    from stripe.params._promotion_code_retrieve_params import (
        PromotionCodeRetrieveParams,
    )


class PromotionCode(
    CreateableAPIResource["PromotionCode"],
    ListableAPIResource["PromotionCode"],
    UpdateableAPIResource["PromotionCode"],
):
    """
    A Promotion Code represents a customer-redeemable code for an underlying promotion.
    You can create multiple codes for a single promotion.

    If you enable promotion codes in your [customer portal configuration](https://docs.stripe.com/customer-management/configure-portal), then customers can redeem a code themselves when updating a subscription in the portal.
    Customers can also view the currently active promotion codes and coupons on each of their subscriptions in the portal.
    """

    OBJECT_NAME: ClassVar[Literal["promotion_code"]] = "promotion_code"

    class Promotion(StripeObject):
        coupon: Optional[ExpandableField["Coupon"]]
        """
        If promotion `type` is `coupon`, the coupon for this promotion.
        """
        type: Literal["coupon"]
        """
        The type of promotion.
        """

    class Restrictions(StripeObject):
        class CurrencyOptions(StripeObject):
            minimum_amount: int
            """
            Minimum amount required to redeem this Promotion Code into a Coupon (e.g., a purchase must be $100 or more to work).
            """

        currency_options: Optional[Dict[str, CurrencyOptions]]
        """
        Promotion code restrictions defined in each available currency option. Each key must be a three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html) and a [supported currency](https://stripe.com/docs/currencies).
        """
        first_time_transaction: bool
        """
        A Boolean indicating if the Promotion Code should only be redeemed for Customers without any successful payments or invoices
        """
        minimum_amount: Optional[int]
        """
        Minimum amount required to redeem this Promotion Code into a Coupon (e.g., a purchase must be $100 or more to work).
        """
        minimum_amount_currency: Optional[str]
        """
        Three-letter [ISO code](https://stripe.com/docs/currencies) for minimum_amount
        """
        _inner_class_types = {"currency_options": CurrencyOptions}
        _inner_class_dicts = ["currency_options"]

    active: bool
    """
    Whether the promotion code is currently active. A promotion code is only active if the coupon is also valid.
    """
    code: str
    """
    The customer-facing code. Regardless of case, this code must be unique across all active promotion codes for each customer. Valid characters are lower case letters (a-z), upper case letters (A-Z), digits (0-9), and dashes (-).
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    customer: Optional[ExpandableField["Customer"]]
    """
    The customer who can use this promotion code.
    """
    customer_account: Optional[str]
    """
    The account representing the customer who can use this promotion code.
    """
    expires_at: Optional[int]
    """
    Date at which the promotion code can no longer be redeemed.
    """
    id: str
    """
    Unique identifier for the object.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    max_redemptions: Optional[int]
    """
    Maximum number of times this promotion code can be redeemed.
    """
    metadata: Optional[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    object: Literal["promotion_code"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    promotion: Promotion
    restrictions: Restrictions
    times_redeemed: int
    """
    Number of times this promotion code has been used.
    """

    @classmethod
    def create(
        cls, **params: Unpack["PromotionCodeCreateParams"]
    ) -> "PromotionCode":
        """
        A promotion code points to an underlying promotion. You can optionally restrict the code to a specific customer, redemption limit, and expiration date.
        """
        return cast(
            "PromotionCode",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["PromotionCodeCreateParams"]
    ) -> "PromotionCode":
        """
        A promotion code points to an underlying promotion. You can optionally restrict the code to a specific customer, redemption limit, and expiration date.
        """
        return cast(
            "PromotionCode",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def list(
        cls, **params: Unpack["PromotionCodeListParams"]
    ) -> ListObject["PromotionCode"]:
        """
        Returns a list of your promotion codes.
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
        cls, **params: Unpack["PromotionCodeListParams"]
    ) -> ListObject["PromotionCode"]:
        """
        Returns a list of your promotion codes.
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
        cls, id: str, **params: Unpack["PromotionCodeModifyParams"]
    ) -> "PromotionCode":
        """
        Updates the specified promotion code by setting the values of the parameters passed. Most fields are, by design, not editable.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "PromotionCode",
            cls._static_request(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    async def modify_async(
        cls, id: str, **params: Unpack["PromotionCodeModifyParams"]
    ) -> "PromotionCode":
        """
        Updates the specified promotion code by setting the values of the parameters passed. Most fields are, by design, not editable.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "PromotionCode",
            await cls._static_request_async(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["PromotionCodeRetrieveParams"]
    ) -> "PromotionCode":
        """
        Retrieves the promotion code with the given ID. In order to retrieve a promotion code by the customer-facing code use [list](https://docs.stripe.com/docs/api/promotion_codes/list) with the desired code.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["PromotionCodeRetrieveParams"]
    ) -> "PromotionCode":
        """
        Retrieves the promotion code with the given ID. In order to retrieve a promotion code by the customer-facing code use [list](https://docs.stripe.com/docs/api/promotion_codes/list) with the desired code.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    _inner_class_types = {"promotion": Promotion, "restrictions": Restrictions}
