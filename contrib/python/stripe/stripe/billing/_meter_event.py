# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from typing import ClassVar, Dict, cast
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params.billing._meter_event_create_params import (
        MeterEventCreateParams,
    )


class MeterEvent(CreateableAPIResource["MeterEvent"]):
    """
    Meter events represent actions that customers take in your system. You can use meter events to bill a customer based on their usage. Meter events are associated with billing meters, which define both the contents of the event's payload and how to aggregate those events.
    """

    OBJECT_NAME: ClassVar[Literal["billing.meter_event"]] = (
        "billing.meter_event"
    )
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    event_name: str
    """
    The name of the meter event. Corresponds with the `event_name` field on a meter.
    """
    identifier: str
    """
    A unique identifier for the event.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    object: Literal["billing.meter_event"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    payload: Dict[str, str]
    """
    The payload of the event. This contains the fields corresponding to a meter's `customer_mapping.event_payload_key` (default is `stripe_customer_id`) and `value_settings.event_payload_key` (default is `value`). Read more about the [payload](https://docs.stripe.com/billing/subscriptions/usage-based/meters/configure#meter-configuration-attributes).
    """
    timestamp: int
    """
    The timestamp passed in when creating the event. Measured in seconds since the Unix epoch.
    """

    @classmethod
    def create(
        cls, **params: Unpack["MeterEventCreateParams"]
    ) -> "MeterEvent":
        """
        Creates a billing meter event.
        """
        return cast(
            "MeterEvent",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["MeterEventCreateParams"]
    ) -> "MeterEvent":
        """
        Creates a billing meter event.
        """
        return cast(
            "MeterEvent",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )
