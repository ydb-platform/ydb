# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_object import StripeObject
from typing import ClassVar, Dict, List, Optional
from typing_extensions import Literal


class CalculationLineItem(StripeObject):
    OBJECT_NAME: ClassVar[Literal["tax.calculation_line_item"]] = (
        "tax.calculation_line_item"
    )

    class TaxBreakdown(StripeObject):
        class Jurisdiction(StripeObject):
            country: str
            """
            Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
            """
            display_name: str
            """
            A human-readable name for the jurisdiction imposing the tax.
            """
            level: Literal["city", "country", "county", "district", "state"]
            """
            Indicates the level of the jurisdiction imposing the tax.
            """
            state: Optional[str]
            """
            [ISO 3166-2 subdivision code](https://en.wikipedia.org/wiki/ISO_3166-2), without country prefix. For example, "NY" for New York, United States.
            """

        class TaxRateDetails(StripeObject):
            display_name: str
            """
            A localized display name for tax type, intended to be human-readable. For example, "Local Sales and Use Tax", "Value-added tax (VAT)", or "Umsatzsteuer (USt.)".
            """
            percentage_decimal: str
            """
            The tax rate percentage as a string. For example, 8.5% is represented as "8.5".
            """
            tax_type: Literal[
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
            """
            The tax type, such as `vat` or `sales_tax`.
            """

        amount: int
        """
        The amount of tax, in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal).
        """
        jurisdiction: Jurisdiction
        sourcing: Literal["destination", "origin"]
        """
        Indicates whether the jurisdiction was determined by the origin (merchant's address) or destination (customer's address).
        """
        tax_rate_details: Optional[TaxRateDetails]
        """
        Details regarding the rate for this tax. This field will be `null` when the tax is not imposed, for example if the product is exempt from tax.
        """
        taxability_reason: Literal[
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
        """
        The reasoning behind this tax, for example, if the product is tax exempt. The possible values for this field may be extended as new tax rules are supported.
        """
        taxable_amount: int
        """
        The amount on which tax is calculated, in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal).
        """
        _inner_class_types = {
            "jurisdiction": Jurisdiction,
            "tax_rate_details": TaxRateDetails,
        }

    amount: int
    """
    The line item amount in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal). If `tax_behavior=inclusive`, then this amount includes taxes. Otherwise, taxes were calculated on top of this amount.
    """
    amount_tax: int
    """
    The amount of tax calculated for this line item, in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal).
    """
    id: str
    """
    Unique identifier for the object.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    metadata: Optional[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    object: Literal["tax.calculation_line_item"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    product: Optional[str]
    """
    The ID of an existing [Product](https://docs.stripe.com/api/products/object).
    """
    quantity: int
    """
    The number of units of the item being purchased. For reversals, this is the quantity reversed.
    """
    reference: str
    """
    A custom identifier for this line item.
    """
    tax_behavior: Literal["exclusive", "inclusive"]
    """
    Specifies whether the `amount` includes taxes. If `tax_behavior=inclusive`, then the amount includes taxes.
    """
    tax_breakdown: Optional[List[TaxBreakdown]]
    """
    Detailed account of taxes relevant to this line item.
    """
    tax_code: str
    """
    The [tax code](https://docs.stripe.com/tax/tax-categories) ID used for this resource.
    """
    _inner_class_types = {"tax_breakdown": TaxBreakdown}
