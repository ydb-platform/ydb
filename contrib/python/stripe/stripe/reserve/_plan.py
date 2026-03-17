# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_object import StripeObject
from typing import ClassVar, Dict, Optional
from typing_extensions import Literal


class Plan(StripeObject):
    """
    ReservePlans are used to automatically place holds on a merchant's funds until the plan expires. It takes a portion of each incoming Charge (including those resulting from a Transfer from a platform account).
    """

    OBJECT_NAME: ClassVar[Literal["reserve.plan"]] = "reserve.plan"

    class FixedRelease(StripeObject):
        release_after: int
        """
        The time after which all reserved funds are requested for release.
        """
        scheduled_release: int
        """
        The time at which reserved funds are scheduled for release, automatically set to midnight UTC of the day after `release_after`.
        """

    class RollingRelease(StripeObject):
        days_after_charge: int
        """
        The number of days to reserve funds before releasing.
        """
        expires_on: Optional[int]
        """
        The time at which the ReservePlan expires.
        """

    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    created_by: Literal["application", "stripe"]
    """
    Indicates which party created this ReservePlan.
    """
    currency: Optional[str]
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies). An unset currency indicates that the plan applies to all currencies.
    """
    disabled_at: Optional[int]
    """
    Time at which the ReservePlan was disabled.
    """
    fixed_release: Optional[FixedRelease]
    id: str
    """
    Unique identifier for the object.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    metadata: Optional[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    object: Literal["reserve.plan"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    percent: int
    """
    The percent of each Charge to reserve.
    """
    rolling_release: Optional[RollingRelease]
    status: Literal["active", "disabled", "expired"]
    """
    The current status of the ReservePlan. The ReservePlan only affects charges if it is `active`.
    """
    type: Literal["fixed_release", "rolling_release"]
    """
    The type of the ReservePlan.
    """
    _inner_class_types = {
        "fixed_release": FixedRelease,
        "rolling_release": RollingRelease,
    }
