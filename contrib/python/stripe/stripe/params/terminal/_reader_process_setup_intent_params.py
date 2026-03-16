# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class ReaderProcessSetupIntentParams(RequestOptions):
    allow_redisplay: Literal["always", "limited", "unspecified"]
    """
    This field indicates whether this payment method can be shown again to its customer in a checkout flow. Stripe products such as Checkout and Elements use this field to determine whether a payment method can be shown as a saved payment method in a checkout flow.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    process_config: NotRequired["ReaderProcessSetupIntentParamsProcessConfig"]
    """
    Configuration overrides for this setup, such as MOTO and customer cancellation settings.
    """
    setup_intent: str
    """
    The ID of the SetupIntent to process on the reader.
    """


class ReaderProcessSetupIntentParamsProcessConfig(TypedDict):
    enable_customer_cancellation: NotRequired[bool]
    """
    Enables cancel button on transaction screens.
    """
