# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class OutboundPaymentUpdateParams(RequestOptions):
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    tracking_details: "OutboundPaymentUpdateParamsTrackingDetails"
    """
    Details about network-specific tracking information.
    """


class OutboundPaymentUpdateParamsTrackingDetails(TypedDict):
    ach: NotRequired["OutboundPaymentUpdateParamsTrackingDetailsAch"]
    """
    ACH network tracking details.
    """
    type: Literal["ach", "us_domestic_wire"]
    """
    The US bank account network used to send funds.
    """
    us_domestic_wire: NotRequired[
        "OutboundPaymentUpdateParamsTrackingDetailsUsDomesticWire"
    ]
    """
    US domestic wire network tracking details.
    """


class OutboundPaymentUpdateParamsTrackingDetailsAch(TypedDict):
    trace_id: str
    """
    ACH trace ID for funds sent over the `ach` network.
    """


class OutboundPaymentUpdateParamsTrackingDetailsUsDomesticWire(TypedDict):
    chips: NotRequired[str]
    """
    CHIPS System Sequence Number (SSN) for funds sent over the `us_domestic_wire` network.
    """
    imad: NotRequired[str]
    """
    IMAD for funds sent over the `us_domestic_wire` network.
    """
    omad: NotRequired[str]
    """
    OMAD for funds sent over the `us_domestic_wire` network.
    """
