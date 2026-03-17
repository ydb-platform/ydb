# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_object import StripeObject
from typing import ClassVar, Optional
from typing_extensions import Literal, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.entitlements._feature import Feature


class ProductFeature(StripeObject):
    """
    A product_feature represents an attachment between a feature and a product.
    When a product is purchased that has a feature attached, Stripe will create an entitlement to the feature for the purchasing customer.
    """

    OBJECT_NAME: ClassVar[Literal["product_feature"]] = "product_feature"
    deleted: Optional[Literal[True]]
    """
    Always true for a deleted object
    """
    entitlement_feature: "Feature"
    """
    A feature represents a monetizable ability or functionality in your system.
    Features can be assigned to products, and when those products are purchased, Stripe will create an entitlement to the feature for the purchasing customer.
    """
    id: str
    """
    Unique identifier for the object.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    object: Literal["product_feature"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
