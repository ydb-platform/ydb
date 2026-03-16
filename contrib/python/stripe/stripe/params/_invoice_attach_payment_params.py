# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import NotRequired


class InvoiceAttachPaymentParams(RequestOptions):
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    payment_intent: NotRequired[str]
    """
    The ID of the PaymentIntent to attach to the invoice.
    """
    payment_record: NotRequired[str]
    """
    The ID of the PaymentRecord to attach to the invoice.
    """
