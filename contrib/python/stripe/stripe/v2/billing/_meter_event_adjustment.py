# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_object import StripeObject
from typing import ClassVar
from typing_extensions import Literal


class MeterEventAdjustment(StripeObject):
    """
    A Meter Event Adjustment is used to cancel or modify previously recorded meter events. Meter Event Adjustments allow you to correct billing data by canceling individual events or event ranges, with tracking of adjustment status and creation time.
    """

    OBJECT_NAME: ClassVar[Literal["v2.billing.meter_event_adjustment"]] = (
        "v2.billing.meter_event_adjustment"
    )

    class Cancel(StripeObject):
        identifier: str
        """
        Unique identifier for the event. You can only cancel events within 24 hours of Stripe receiving them.
        """

    cancel: Cancel
    """
    Specifies which event to cancel.
    """
    created: str
    """
    The time the adjustment was created.
    """
    event_name: str
    """
    The name of the meter event. Corresponds with the `event_name` field on a meter.
    """
    id: str
    """
    The unique id of this meter event adjustment.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    object: Literal["v2.billing.meter_event_adjustment"]
    """
    String representing the object's type. Objects of the same type share the same value of the object field.
    """
    status: Literal["complete", "pending"]
    """
    Open Enum. The meter event adjustment's status.
    """
    type: Literal["cancel"]
    """
    Open Enum. Specifies whether to cancel a single event or a range of events for a time period. Time period cancellation is not supported yet.
    """
    _inner_class_types = {"cancel": Cancel}
