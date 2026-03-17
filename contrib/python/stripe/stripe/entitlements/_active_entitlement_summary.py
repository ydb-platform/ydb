# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._list_object import ListObject
from stripe._stripe_object import StripeObject
from typing import ClassVar
from typing_extensions import Literal, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.entitlements._active_entitlement import ActiveEntitlement


class ActiveEntitlementSummary(StripeObject):
    """
    A summary of a customer's active entitlements.
    """

    OBJECT_NAME: ClassVar[
        Literal["entitlements.active_entitlement_summary"]
    ] = "entitlements.active_entitlement_summary"
    customer: str
    """
    The customer that is entitled to this feature.
    """
    entitlements: ListObject["ActiveEntitlement"]
    """
    The list of entitlements this customer has.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    object: Literal["entitlements.active_entitlement_summary"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
