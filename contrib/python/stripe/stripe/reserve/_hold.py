# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._expandable_field import ExpandableField
from stripe._stripe_object import StripeObject
from typing import ClassVar, Dict, Optional
from typing_extensions import Literal, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._charge import Charge
    from stripe.reserve._plan import Plan


class Hold(StripeObject):
    """
    ReserveHolds are used to place a temporary ReserveHold on a merchant's funds.
    """

    OBJECT_NAME: ClassVar[Literal["reserve.hold"]] = "reserve.hold"

    class ReleaseSchedule(StripeObject):
        release_after: Optional[int]
        """
        The time after which the ReserveHold is requested to be released.
        """
        scheduled_release: Optional[int]
        """
        The time at which the ReserveHold is scheduled to be released, automatically set to midnight UTC of the day after `release_after`.
        """

    amount: int
    """
    Amount reserved. A positive integer representing how much is reserved in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal).
    """
    amount_releasable: Optional[int]
    """
    Amount in cents that can be released from this ReserveHold
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    created_by: Literal["application", "stripe"]
    """
    Indicates which party created this ReserveHold.
    """
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    id: str
    """
    Unique identifier for the object.
    """
    is_releasable: Optional[bool]
    """
    Whether there are any funds available to release on this ReserveHold. Note that if the ReserveHold is in the process of being released, this could be false, even though the funds haven't been fully released yet.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    metadata: Optional[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    object: Literal["reserve.hold"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    reason: Literal["charge", "standalone"]
    """
    The reason for the ReserveHold.
    """
    release_schedule: ReleaseSchedule
    reserve_plan: Optional[ExpandableField["Plan"]]
    """
    The ReservePlan which produced this ReserveHold (i.e., resplan_123)
    """
    source_charge: Optional[ExpandableField["Charge"]]
    """
    The Charge which funded this ReserveHold (e.g., ch_123)
    """
    source_type: Literal["bank_account", "card", "fpx"]
    """
    Which source balance type this ReserveHold reserves funds from. One of `bank_account`, `card`, or `fpx`.
    """
    _inner_class_types = {"release_schedule": ReleaseSchedule}
