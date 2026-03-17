# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class OutboundTransferUpdateParams(TypedDict):
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    tracking_details: "OutboundTransferUpdateParamsTrackingDetails"
    """
    Details about network-specific tracking information.
    """


class OutboundTransferUpdateParamsTrackingDetails(TypedDict):
    ach: NotRequired["OutboundTransferUpdateParamsTrackingDetailsAch"]
    """
    ACH network tracking details.
    """
    type: Literal["ach", "us_domestic_wire"]
    """
    The US bank account network used to send funds.
    """
    us_domestic_wire: NotRequired[
        "OutboundTransferUpdateParamsTrackingDetailsUsDomesticWire"
    ]
    """
    US domestic wire network tracking details.
    """


class OutboundTransferUpdateParamsTrackingDetailsAch(TypedDict):
    trace_id: str
    """
    ACH trace ID for funds sent over the `ach` network.
    """


class OutboundTransferUpdateParamsTrackingDetailsUsDomesticWire(TypedDict):
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
