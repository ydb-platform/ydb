# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._expandable_field import ExpandableField
from stripe._stripe_object import StripeObject
from stripe._updateable_api_resource import UpdateableAPIResource
from stripe._util import sanitize_id
from typing import ClassVar, Dict, List, Optional, cast
from typing_extensions import Literal, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._discount import Discount
    from stripe._price import Price
    from stripe._subscription import Subscription
    from stripe.billing._credit_balance_transaction import (
        CreditBalanceTransaction,
    )


class InvoiceLineItem(UpdateableAPIResource["InvoiceLineItem"]):
    """
    Invoice Line Items represent the individual lines within an [invoice](https://docs.stripe.com/api/invoices) and only exist within the context of an invoice.

    Each line item is backed by either an [invoice item](https://docs.stripe.com/api/invoiceitems) or a [subscription item](https://docs.stripe.com/api/subscription_items).
    """

    OBJECT_NAME: ClassVar[Literal["line_item"]] = "line_item"

    class DiscountAmount(StripeObject):
        amount: int
        """
        The amount, in cents (or local equivalent), of the discount.
        """
        discount: ExpandableField["Discount"]
        """
        The discount that was applied to get this discount amount.
        """

    class Parent(StripeObject):
        class InvoiceItemDetails(StripeObject):
            class ProrationDetails(StripeObject):
                class CreditedItems(StripeObject):
                    invoice: str
                    """
                    Invoice containing the credited invoice line items
                    """
                    invoice_line_items: List[str]
                    """
                    Credited invoice line items
                    """

                credited_items: Optional[CreditedItems]
                """
                For a credit proration `line_item`, the original debit line_items to which the credit proration applies.
                """
                _inner_class_types = {"credited_items": CreditedItems}

            invoice_item: str
            """
            The invoice item that generated this line item
            """
            proration: bool
            """
            Whether this is a proration
            """
            proration_details: Optional[ProrationDetails]
            """
            Additional details for proration line items
            """
            subscription: Optional[str]
            """
            The subscription that the invoice item belongs to
            """
            _inner_class_types = {"proration_details": ProrationDetails}

        class SubscriptionItemDetails(StripeObject):
            class ProrationDetails(StripeObject):
                class CreditedItems(StripeObject):
                    invoice: str
                    """
                    Invoice containing the credited invoice line items
                    """
                    invoice_line_items: List[str]
                    """
                    Credited invoice line items
                    """

                credited_items: Optional[CreditedItems]
                """
                For a credit proration `line_item`, the original debit line_items to which the credit proration applies.
                """
                _inner_class_types = {"credited_items": CreditedItems}

            invoice_item: Optional[str]
            """
            The invoice item that generated this line item
            """
            proration: bool
            """
            Whether this is a proration
            """
            proration_details: Optional[ProrationDetails]
            """
            Additional details for proration line items
            """
            subscription: Optional[str]
            """
            The subscription that the subscription item belongs to
            """
            subscription_item: str
            """
            The subscription item that generated this line item
            """
            _inner_class_types = {"proration_details": ProrationDetails}

        invoice_item_details: Optional[InvoiceItemDetails]
        """
        Details about the invoice item that generated this line item
        """
        subscription_item_details: Optional[SubscriptionItemDetails]
        """
        Details about the subscription item that generated this line item
        """
        type: Literal["invoice_item_details", "subscription_item_details"]
        """
        The type of parent that generated this line item
        """
        _inner_class_types = {
            "invoice_item_details": InvoiceItemDetails,
            "subscription_item_details": SubscriptionItemDetails,
        }

    class Period(StripeObject):
        end: int
        """
        The end of the period, which must be greater than or equal to the start. This value is inclusive.
        """
        start: int
        """
        The start of the period. This value is inclusive.
        """

    class PretaxCreditAmount(StripeObject):
        amount: int
        """
        The amount, in cents (or local equivalent), of the pretax credit amount.
        """
        credit_balance_transaction: Optional[
            ExpandableField["CreditBalanceTransaction"]
        ]
        """
        The credit balance transaction that was applied to get this pretax credit amount.
        """
        discount: Optional[ExpandableField["Discount"]]
        """
        The discount that was applied to get this pretax credit amount.
        """
        type: Literal["credit_balance_transaction", "discount"]
        """
        Type of the pretax credit amount referenced.
        """

    class Pricing(StripeObject):
        class PriceDetails(StripeObject):
            price: ExpandableField["Price"]
            """
            The ID of the price this item is associated with.
            """
            product: str
            """
            The ID of the product this item is associated with.
            """

        price_details: Optional[PriceDetails]
        type: Literal["price_details"]
        """
        The type of the pricing details.
        """
        unit_amount_decimal: Optional[str]
        """
        The unit amount (in the `currency` specified) of the item which contains a decimal value with at most 12 decimal places.
        """
        _inner_class_types = {"price_details": PriceDetails}

    class Tax(StripeObject):
        class TaxRateDetails(StripeObject):
            tax_rate: str
            """
            ID of the tax rate
            """

        amount: int
        """
        The amount of the tax, in cents (or local equivalent).
        """
        tax_behavior: Literal["exclusive", "inclusive"]
        """
        Whether this tax is inclusive or exclusive.
        """
        tax_rate_details: Optional[TaxRateDetails]
        """
        Additional details about the tax rate. Only present when `type` is `tax_rate_details`.
        """
        taxability_reason: Literal[
            "customer_exempt",
            "not_available",
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
        taxable_amount: Optional[int]
        """
        The amount on which tax is calculated, in cents (or local equivalent).
        """
        type: Literal["tax_rate_details"]
        """
        The type of tax information.
        """
        _inner_class_types = {"tax_rate_details": TaxRateDetails}

    amount: int
    """
    The amount, in cents (or local equivalent).
    """
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    description: Optional[str]
    """
    An arbitrary string attached to the object. Often useful for displaying to users.
    """
    discount_amounts: Optional[List[DiscountAmount]]
    """
    The amount of discount calculated per discount for this line item.
    """
    discountable: bool
    """
    If true, discounts will apply to this line item. Always false for prorations.
    """
    discounts: List[ExpandableField["Discount"]]
    """
    The discounts applied to the invoice line item. Line item discounts are applied before invoice discounts. Use `expand[]=discounts` to expand each discount.
    """
    id: str
    """
    Unique identifier for the object.
    """
    invoice: Optional[str]
    """
    The ID of the invoice that contains this line item.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    metadata: Dict[str, str]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Note that for line items with `type=subscription`, `metadata` reflects the current metadata from the subscription associated with the line item, unless the invoice line was directly updated with different metadata after creation.
    """
    object: Literal["line_item"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    parent: Optional[Parent]
    """
    The parent that generated this line item.
    """
    period: Period
    pretax_credit_amounts: Optional[List[PretaxCreditAmount]]
    """
    Contains pretax credit amounts (ex: discount, credit grants, etc) that apply to this line item.
    """
    pricing: Optional[Pricing]
    """
    The pricing information of the line item.
    """
    quantity: Optional[int]
    """
    The quantity of the subscription, if the line item is a subscription or a proration.
    """
    subscription: Optional[ExpandableField["Subscription"]]
    subtotal: int
    """
    The subtotal of the line item, in cents (or local equivalent), before any discounts or taxes.
    """
    taxes: Optional[List[Tax]]
    """
    The tax information of the line item.
    """

    @classmethod
    def modify(
        cls, invoice: str, line_item_id: str, **params
    ) -> "InvoiceLineItem":
        """
        Updates an invoice's line item. Some fields, such as tax_amounts, only live on the invoice line item,
        so they can only be updated through this endpoint. Other fields, such as amount, live on both the invoice
        item and the invoice line item, so updates on this endpoint will propagate to the invoice item as well.
        Updating an invoice's line item is only possible before the invoice is finalized.
        """
        url = "/v1/invoices/%s/lines/%s" % (
            sanitize_id(invoice),
            sanitize_id(line_item_id),
        )
        return cast(
            "InvoiceLineItem",
            cls._static_request(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    async def modify_async(
        cls, invoice: str, line_item_id: str, **params
    ) -> "InvoiceLineItem":
        """
        Updates an invoice's line item. Some fields, such as tax_amounts, only live on the invoice line item,
        so they can only be updated through this endpoint. Other fields, such as amount, live on both the invoice
        item and the invoice line item, so updates on this endpoint will propagate to the invoice item as well.
        Updating an invoice's line item is only possible before the invoice is finalized.
        """
        url = "/v1/invoices/%s/lines/%s" % (
            sanitize_id(invoice),
            sanitize_id(line_item_id),
        )
        return cast(
            "InvoiceLineItem",
            await cls._static_request_async(
                "post",
                url,
                params=params,
            ),
        )

    _inner_class_types = {
        "discount_amounts": DiscountAmount,
        "parent": Parent,
        "period": Period,
        "pretax_credit_amounts": PretaxCreditAmount,
        "pricing": Pricing,
        "taxes": Tax,
    }
