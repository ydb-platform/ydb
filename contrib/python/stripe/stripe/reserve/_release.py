# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._expandable_field import ExpandableField
from stripe._stripe_object import StripeObject
from typing import ClassVar, Dict, Optional
from typing_extensions import Literal, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._dispute import Dispute
    from stripe._refund import Refund
    from stripe.reserve._hold import Hold
    from stripe.reserve._plan import Plan


class Release(StripeObject):
    """
    ReserveReleases represent the release of funds from a ReserveHold.
    """

    OBJECT_NAME: ClassVar[Literal["reserve.release"]] = "reserve.release"

    class SourceTransaction(StripeObject):
        dispute: Optional[ExpandableField["Dispute"]]
        """
        The ID of the dispute.
        """
        refund: Optional[ExpandableField["Refund"]]
        """
        The ID of the refund.
        """
        type: Literal["dispute", "refund"]
        """
        The type of source transaction.
        """

    amount: int
    """
    Amount released. A positive integer representing how much is released in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal).
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    created_by: Literal["application", "stripe"]
    """
    Indicates which party created this ReserveRelease.
    """
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
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
    object: Literal["reserve.release"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    reason: Literal[
        "bulk_hold_expiry",
        "hold_released_early",
        "hold_reversed",
        "plan_disabled",
    ]
    """
    The reason for the ReserveRelease, indicating why the funds were released.
    """
    released_at: int
    """
    The release timestamp of the funds.
    """
    reserve_hold: Optional[ExpandableField["Hold"]]
    """
    The ReserveHold this ReserveRelease is associated with.
    """
    reserve_plan: Optional[ExpandableField["Plan"]]
    """
    The ReservePlan ID this ReserveRelease is associated with. This field is only populated if a ReserveRelease is created by a ReservePlan disable operation, or from a scheduled ReservedHold expiry.
    """
    source_transaction: Optional[SourceTransaction]
    _inner_class_types = {"source_transaction": SourceTransaction}
