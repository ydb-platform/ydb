# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class InvoiceAddLinesParams(RequestOptions):
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    invoice_metadata: NotRequired["Literal['']|Dict[str, str]"]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    lines: List["InvoiceAddLinesParamsLine"]
    """
    The line items to add.
    """


class InvoiceAddLinesParamsLine(TypedDict):
    amount: NotRequired[int]
    """
    The integer amount in cents (or local equivalent) of the charge to be applied to the upcoming invoice. If you want to apply a credit to the customer's account, pass a negative amount.
    """
    description: NotRequired[str]
    """
    An arbitrary string which you can attach to the invoice item. The description is displayed in the invoice for easy tracking.
    """
    discountable: NotRequired[bool]
    """
    Controls whether discounts apply to this line item. Defaults to false for prorations or negative line items, and true for all other line items. Cannot be set to true for prorations.
    """
    discounts: NotRequired[
        "Literal['']|List[InvoiceAddLinesParamsLineDiscount]"
    ]
    """
    The coupons, promotion codes & existing discounts which apply to the line item. Item discounts are applied before invoice discounts. Pass an empty string to remove previously-defined discounts.
    """
    invoice_item: NotRequired[str]
    """
    ID of an unassigned invoice item to assign to this invoice. If not provided, a new item will be created.
    """
    metadata: NotRequired["Literal['']|Dict[str, str]"]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    period: NotRequired["InvoiceAddLinesParamsLinePeriod"]
    """
    The period associated with this invoice item. When set to different values, the period will be rendered on the invoice. If you have [Stripe Revenue Recognition](https://docs.stripe.com/revenue-recognition) enabled, the period will be used to recognize and defer revenue. See the [Revenue Recognition documentation](https://docs.stripe.com/revenue-recognition/methodology/subscriptions-and-invoicing) for details.
    """
    price_data: NotRequired["InvoiceAddLinesParamsLinePriceData"]
    """
    Data used to generate a new [Price](https://docs.stripe.com/api/prices) object inline.
    """
    pricing: NotRequired["InvoiceAddLinesParamsLinePricing"]
    """
    The pricing information for the invoice item.
    """
    quantity: NotRequired[int]
    """
    Non-negative integer. The quantity of units for the line item.
    """
    tax_amounts: NotRequired[
        "Literal['']|List[InvoiceAddLinesParamsLineTaxAmount]"
    ]
    """
    A list of up to 10 tax amounts for this line item. This can be useful if you calculate taxes on your own or use a third-party to calculate them. You cannot set tax amounts if any line item has [tax_rates](https://docs.stripe.com/api/invoices/line_item#invoice_line_item_object-tax_rates) or if the invoice has [default_tax_rates](https://docs.stripe.com/api/invoices/object#invoice_object-default_tax_rates) or uses [automatic tax](https://docs.stripe.com/tax/invoicing). Pass an empty string to remove previously defined tax amounts.
    """
    tax_rates: NotRequired["Literal['']|List[str]"]
    """
    The tax rates which apply to the line item. When set, the `default_tax_rates` on the invoice do not apply to this line item. Pass an empty string to remove previously-defined tax rates.
    """


class InvoiceAddLinesParamsLineDiscount(TypedDict):
    coupon: NotRequired[str]
    """
    ID of the coupon to create a new discount for.
    """
    discount: NotRequired[str]
    """
    ID of an existing discount on the object (or one of its ancestors) to reuse.
    """
    promotion_code: NotRequired[str]
    """
    ID of the promotion code to create a new discount for.
    """


class InvoiceAddLinesParamsLinePeriod(TypedDict):
    end: int
    """
    The end of the period, which must be greater than or equal to the start. This value is inclusive.
    """
    start: int
    """
    The start of the period. This value is inclusive.
    """


class InvoiceAddLinesParamsLinePriceData(TypedDict):
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    product: NotRequired[str]
    """
    The ID of the [Product](https://docs.stripe.com/api/products) that this [Price](https://docs.stripe.com/api/prices) will belong to. One of `product` or `product_data` is required.
    """
    product_data: NotRequired["InvoiceAddLinesParamsLinePriceDataProductData"]
    """
    Data used to generate a new [Product](https://docs.stripe.com/api/products) object inline. One of `product` or `product_data` is required.
    """
    tax_behavior: NotRequired[Literal["exclusive", "inclusive", "unspecified"]]
    """
    Only required if a [default tax behavior](https://docs.stripe.com/tax/products-prices-tax-categories-tax-behavior#setting-a-default-tax-behavior-(recommended)) was not provided in the Stripe Tax settings. Specifies whether the price is considered inclusive of taxes or exclusive of taxes. One of `inclusive`, `exclusive`, or `unspecified`. Once specified as either `inclusive` or `exclusive`, it cannot be changed.
    """
    unit_amount: NotRequired[int]
    """
    A non-negative integer in cents (or local equivalent) representing how much to charge. One of `unit_amount` or `unit_amount_decimal` is required.
    """
    unit_amount_decimal: NotRequired[str]
    """
    Same as `unit_amount`, but accepts a decimal value in cents (or local equivalent) with at most 12 decimal places. Only one of `unit_amount` and `unit_amount_decimal` can be set.
    """


class InvoiceAddLinesParamsLinePriceDataProductData(TypedDict):
    description: NotRequired[str]
    """
    The product's description, meant to be displayable to the customer. Use this field to optionally store a long form explanation of the product being sold for your own rendering purposes.
    """
    images: NotRequired[List[str]]
    """
    A list of up to 8 URLs of images for this product, meant to be displayable to the customer.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    name: str
    """
    The product's name, meant to be displayable to the customer.
    """
    tax_code: NotRequired[str]
    """
    A [tax code](https://docs.stripe.com/tax/tax-categories) ID.
    """
    unit_label: NotRequired[str]
    """
    A label that represents units of this product. When set, this will be included in customers' receipts, invoices, Checkout, and the customer portal.
    """


class InvoiceAddLinesParamsLinePricing(TypedDict):
    price: NotRequired[str]
    """
    The ID of the price object.
    """


class InvoiceAddLinesParamsLineTaxAmount(TypedDict):
    amount: int
    """
    The amount, in cents (or local equivalent), of the tax.
    """
    tax_rate_data: "InvoiceAddLinesParamsLineTaxAmountTaxRateData"
    """
    Data to find or create a TaxRate object.

    Stripe automatically creates or reuses a TaxRate object for each tax amount. If the `tax_rate_data` exactly matches a previous value, Stripe will reuse the TaxRate object. TaxRate objects created automatically by Stripe are immediately archived, do not appear in the line item's `tax_rates`, and cannot be directly added to invoices, payments, or line items.
    """
    taxability_reason: NotRequired[
        Literal[
            "customer_exempt",
            "not_collecting",
            "not_subject_to_tax",
            "not_supported",
            "portion_product_exempt",
            "portion_reduced_rated",
            "portion_standard_rated",
            "product_exempt",
            "product_exempt_holiday",
            "proportionally_rated",
            "reduced_rated",
            "reverse_charge",
            "standard_rated",
            "taxable_basis_reduced",
            "zero_rated",
        ]
    ]
    """
    The reasoning behind this tax, for example, if the product is tax exempt.
    """
    taxable_amount: int
    """
    The amount on which tax is calculated, in cents (or local equivalent).
    """


class InvoiceAddLinesParamsLineTaxAmountTaxRateData(TypedDict):
    country: NotRequired[str]
    """
    Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
    """
    description: NotRequired[str]
    """
    An arbitrary string attached to the tax rate for your internal use only. It will not be visible to your customers.
    """
    display_name: str
    """
    The display name of the tax rate, which will be shown to users.
    """
    inclusive: bool
    """
    This specifies if the tax rate is inclusive or exclusive.
    """
    jurisdiction: NotRequired[str]
    """
    The jurisdiction for the tax rate. You can use this label field for tax reporting purposes. It also appears on your customer's invoice.
    """
    jurisdiction_level: NotRequired[
        Literal["city", "country", "county", "district", "multiple", "state"]
    ]
    """
    The level of the jurisdiction that imposes this tax rate.
    """
    percentage: float
    """
    The statutory tax rate percent. This field accepts decimal values between 0 and 100 inclusive with at most 4 decimal places. To accommodate fixed-amount taxes, set the percentage to zero. Stripe will not display zero percentages on the invoice unless the `amount` of the tax is also zero.
    """
    state: NotRequired[str]
    """
    [ISO 3166-2 subdivision code](https://en.wikipedia.org/wiki/ISO_3166-2:US), without country prefix. For example, "NY" for New York, United States.
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
