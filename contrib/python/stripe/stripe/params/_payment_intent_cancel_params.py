# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import Literal, NotRequired


class PaymentIntentCancelParams(RequestOptions):
    cancellation_reason: NotRequired[
        Literal[
            "abandoned", "duplicate", "fraudulent", "requested_by_customer"
        ]
    ]
    """
    Reason for canceling this PaymentIntent. Possible values are: `duplicate`, `fraudulent`, `requested_by_customer`, or `abandoned`
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
