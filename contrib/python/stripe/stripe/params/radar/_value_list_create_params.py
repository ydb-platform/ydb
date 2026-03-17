# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List
from typing_extensions import Literal, NotRequired


class ValueListCreateParams(RequestOptions):
    alias: str
    """
    The name of the value list for use in rules.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    item_type: NotRequired[
        Literal[
            "card_bin",
            "card_fingerprint",
            "case_sensitive_string",
            "country",
            "customer_id",
            "email",
            "ip_address",
            "sepa_debit_fingerprint",
            "string",
            "us_bank_account_fingerprint",
        ]
    ]
    """
    Type of the items in the value list. One of `card_fingerprint`, `card_bin`, `email`, `ip_address`, `country`, `string`, `case_sensitive_string`, `customer_id`, `sepa_debit_fingerprint`, or `us_bank_account_fingerprint`. Use `string` if the item type is unknown or mixed.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    name: str
    """
    The human-readable name of the value list.
    """
