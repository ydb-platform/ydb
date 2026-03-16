# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List
from typing_extensions import Literal, NotRequired


class TaxRateModifyParams(RequestOptions):
    active: NotRequired[bool]
    """
    Flag determining whether the tax rate is active or inactive (archived). Inactive tax rates cannot be used with new applications or Checkout Sessions, but will still work for subscriptions and invoices that already have it set.
    """
    country: NotRequired[str]
    """
    Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
    """
    description: NotRequired[str]
    """
    An arbitrary string attached to the tax rate for your internal use only. It will not be visible to your customers.
    """
    display_name: NotRequired[str]
    """
    The display name of the tax rate, which will be shown to users.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    jurisdiction: NotRequired[str]
    """
    The jurisdiction for the tax rate. You can use this label field for tax reporting purposes. It also appears on your customer's invoice.
    """
    metadata: NotRequired["Literal['']|Dict[str, str]"]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    state: NotRequired[str]
    """
    [ISO 3166-2 subdivision code](https://en.wikipedia.org/wiki/ISO_3166-2), without country prefix. For example, "NY" for New York, United States.
    """
    tax_type: NotRequired[
        Literal[
            "amusement_tax",
            "communications_tax",
            "gst",
            "hst",
            "igst",
            "jct",
            "lease_tax",
            "pst",
            "qst",
            "retail_delivery_fee",
            "rst",
            "sales_tax",
            "service_tax",
            "vat",
        ]
    ]
    """
    The high-level tax type, such as `vat` or `sales_tax`.
    """
