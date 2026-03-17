# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from typing import Dict
from typing_extensions import NotRequired, TypedDict


class MeterEventCreateParams(TypedDict):
    event_name: str
    """
    The name of the meter event. Corresponds with the `event_name` field on a meter.
    """
    identifier: NotRequired[str]
    """
    A unique identifier for the event. If not provided, one will be generated.
    We recommend using a globally unique identifier for this. We'll enforce
    uniqueness within a rolling 24 hour period.
    """
    payload: Dict[str, str]
    """
    The payload of the event. This must contain the fields corresponding to a meter's
    `customer_mapping.event_payload_key` (default is `stripe_customer_id`) and
    `value_settings.event_payload_key` (default is `value`). Read more about
    the
    [payload](https://docs.stripe.com/billing/subscriptions/usage-based/recording-usage#payload-key-overrides).
    """
    timestamp: NotRequired[str]
    """
    The time of the event. Must be within the past 35 calendar days or up to
    5 minutes in the future. Defaults to current timestamp if not specified.
    """
