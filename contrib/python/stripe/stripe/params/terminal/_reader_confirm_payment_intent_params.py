# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import NotRequired, TypedDict


class ReaderConfirmPaymentIntentParams(RequestOptions):
    confirm_config: NotRequired[
        "ReaderConfirmPaymentIntentParamsConfirmConfig"
    ]
    """
    Configuration overrides for this confirmation, such as surcharge settings and return URL.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    payment_intent: str
    """
    The ID of the PaymentIntent to confirm.
    """


class ReaderConfirmPaymentIntentParamsConfirmConfig(TypedDict):
    return_url: NotRequired[str]
    """
    The URL to redirect your customer back to after they authenticate or cancel their payment on the payment method's app or site. If you'd prefer to redirect to a mobile application, you can alternatively supply an application URI scheme.
    """
