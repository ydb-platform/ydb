# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._expandable_field import ExpandableField
from stripe._stripe_object import StripeObject
from typing import ClassVar, Optional
from typing_extensions import Literal, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._coupon import Coupon
    from stripe._customer import Customer
    from stripe._promotion_code import PromotionCode


class Discount(StripeObject):
    """
    A discount represents the actual application of a [coupon](https://api.stripe.com#coupons) or [promotion code](https://api.stripe.com#promotion_codes).
    It contains information about when the discount began, when it will end, and what it is applied to.

    Related guide: [Applying discounts to subscriptions](https://docs.stripe.com/billing/subscriptions/discounts)
    """

    OBJECT_NAME: ClassVar[Literal["discount"]] = "discount"

    class Source(StripeObject):
        coupon: Optional[ExpandableField["Coupon"]]
        """
        The coupon that was redeemed to create this discount.
        """
        type: Literal["coupon"]
        """
        The source type of the discount.
        """

    checkout_session: Optional[str]
    """
    The Checkout session that this coupon is applied to, if it is applied to a particular session in payment mode. Will not be present for subscription mode.
    """
    customer: Optional[ExpandableField["Customer"]]
    """
    The ID of the customer associated with this discount.
    """
    customer_account: Optional[str]
    """
    The ID of the account representing the customer associated with this discount.
    """
    deleted: Optional[Literal[True]]
    """
    Always true for a deleted object
    """
    end: Optional[int]
    """
    If the coupon has a duration of `repeating`, the date that this discount will end. If the coupon has a duration of `once` or `forever`, this attribute will be null.
    """
    id: str
    """
    The ID of the discount object. Discounts cannot be fetched by ID. Use `expand[]=discounts` in API calls to expand discount IDs in an array.
    """
    invoice: Optional[str]
    """
    The invoice that the discount's coupon was applied to, if it was applied directly to a particular invoice.
    """
    invoice_item: Optional[str]
    """
    The invoice item `id` (or invoice line item `id` for invoice line items of type='subscription') that the discount's coupon was applied to, if it was applied directly to a particular invoice item or invoice line item.
    """
    object: Literal["discount"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    promotion_code: Optional[ExpandableField["PromotionCode"]]
    """
    The promotion code applied to create this discount.
    """
    source: Source
    start: int
    """
    Date that the coupon was applied.
    """
    subscription: Optional[str]
    """
    The subscription that this coupon is applied to, if it is applied to a particular subscription.
    """
    subscription_item: Optional[str]
    """
    The subscription item that this coupon is applied to, if it is applied to a particular subscription item.
    """
    _inner_class_types = {"source": Source}
