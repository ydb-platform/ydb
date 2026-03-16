# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_object import StripeObject
from typing import ClassVar
from typing_extensions import Literal


class MeterEventSession(StripeObject):
    """
    A Meter Event Session is an authentication session for the high-throughput meter event API. Meter Event Sessions provide temporary authentication tokens with expiration times, enabling secure and efficient bulk submission of usage events.
    """

    OBJECT_NAME: ClassVar[Literal["v2.billing.meter_event_session"]] = (
        "v2.billing.meter_event_session"
    )
    authentication_token: str
    """
    The authentication token for this session.  Use this token when calling the
    high-throughput meter event API.
    """
    created: str
    """
    The creation time of this session.
    """
    expires_at: str
    """
    The time at which this session will expire.
    """
    id: str
    """
    The unique id of this auth session.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    object: Literal["v2.billing.meter_event_session"]
    """
    String representing the object's type. Objects of the same type share the same value of the object field.
    """
