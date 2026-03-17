# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class ProductUpdateParams(TypedDict):
    active: NotRequired[bool]
    """
    Whether the product is available for purchase.
    """
    default_price: NotRequired[str]
    """
    The ID of the [Price](https://docs.stripe.com/api/prices) object that is the default price for this product.
    """
    description: NotRequired["Literal['']|str"]
    """
    The product's description, meant to be displayable to the customer. Use this field to optionally store a long form explanation of the product being sold for your own rendering purposes.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    images: NotRequired["Literal['']|List[str]"]
    """
    A list of up to 8 URLs of images for this product, meant to be displayable to the customer.
    """
    marketing_features: NotRequired[
        "Literal['']|List[ProductUpdateParamsMarketingFeature]"
    ]
    """
    A list of up to 15 marketing features for this product. These are displayed in [pricing tables](https://docs.stripe.com/payments/checkout/pricing-table).
    """
    metadata: NotRequired["Literal['']|Dict[str, str]"]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    name: NotRequired[str]
    """
    The product's name, meant to be displayable to the customer.
    """
    package_dimensions: NotRequired[
        "Literal['']|ProductUpdateParamsPackageDimensions"
    ]
    """
    The dimensions of this product for shipping purposes.
    """
    shippable: NotRequired[bool]
    """
    Whether this product is shipped (i.e., physical goods).
    """
    statement_descriptor: NotRequired[str]
    """
    An arbitrary string to be displayed on your customer's credit card or bank statement. While most banks display this information consistently, some may display it incorrectly or not at all.

    This may be up to 22 characters. The statement description may not include `<`, `>`, `\\`, `"`, `'` characters, and will appear on your customer's statement in capital letters. Non-ASCII characters are automatically stripped.
     It must contain at least one letter. May only be set if `type=service`. Only used for subscription payments.
    """
    tax_code: NotRequired["Literal['']|str"]
    """
    A [tax code](https://docs.stripe.com/tax/tax-categories) ID.
    """
    unit_label: NotRequired["Literal['']|str"]
    """
    A label that represents units of this product. When set, this will be included in customers' receipts, invoices, Checkout, and the customer portal. May only be set if `type=service`.
    """
    url: NotRequired["Literal['']|str"]
    """
    A URL of a publicly-accessible webpage for this product.
    """


class ProductUpdateParamsMarketingFeature(TypedDict):
    name: str
    """
    The marketing feature name. Up to 80 characters long.
    """


class ProductUpdateParamsPackageDimensions(TypedDict):
    height: float
    """
    Height, in inches. Maximum precision is 2 decimal places.
    """
    length: float
    """
    Length, in inches. Maximum precision is 2 decimal places.
    """
    weight: float
    """
    Weight, in ounces. Maximum precision is 2 decimal places.
    """
    width: float
    """
    Width, in inches. Maximum precision is 2 decimal places.
    """
