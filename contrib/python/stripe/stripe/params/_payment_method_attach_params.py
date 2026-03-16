# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import NotRequired


class PaymentMethodAttachParams(RequestOptions):
    customer: NotRequired[str]
    """
    The ID of the customer to which to attach the PaymentMethod.
    """
    customer_account: NotRequired[str]
    """
    The ID of the Account representing the customer to which to attach the PaymentMethod.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
