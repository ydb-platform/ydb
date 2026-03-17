# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class RequestCreateParams(RequestOptions):
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    payment_method: str
    """
    The PaymentMethod to insert into the forwarded request. Forwarding previously consumed PaymentMethods is allowed.
    """
    replacements: List[
        Literal[
            "card_cvc",
            "card_expiry",
            "card_number",
            "cardholder_name",
            "request_signature",
        ]
    ]
    """
    The field kinds to be replaced in the forwarded request.
    """
    request: "RequestCreateParamsRequest"
    """
    The request body and headers to be sent to the destination endpoint.
    """
    url: str
    """
    The destination URL for the forwarded request. Must be supported by the config.
    """


class RequestCreateParamsRequest(TypedDict):
    body: NotRequired[str]
    """
    The body payload to send to the destination endpoint.
    """
    headers: NotRequired[List["RequestCreateParamsRequestHeader"]]
    """
    The headers to include in the forwarded request. Can be omitted if no additional headers (excluding Stripe-generated ones such as the Content-Type header) should be included.
    """


class RequestCreateParamsRequestHeader(TypedDict):
    name: str
    """
    The header name.
    """
    value: str
    """
    The header value.
    """
