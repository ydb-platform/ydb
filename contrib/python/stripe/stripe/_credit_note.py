# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._expandable_field import ExpandableField
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._nested_resource_class_methods import nested_resource_class_methods
from stripe._stripe_object import StripeObject
from stripe._updateable_api_resource import UpdateableAPIResource
from stripe._util import class_method_variant, sanitize_id
from typing import ClassVar, Dict, List, Optional, cast, overload
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._credit_note_line_item import CreditNoteLineItem
    from stripe._customer import Customer
    from stripe._customer_balance_transaction import CustomerBalanceTransaction
    from stripe._discount import Discount
    from stripe._invoice import Invoice
    from stripe._refund import Refund as RefundResource
    from stripe._shipping_rate import ShippingRate
    from stripe._tax_rate import TaxRate
    from stripe.billing._credit_balance_transaction import (
        CreditBalanceTransaction,
    )
    from stripe.params._credit_note_create_params import CreditNoteCreateParams
    from stripe.params._credit_note_list_lines_params import (
        CreditNoteListLinesParams,
    )
    from stripe.params._credit_note_list_params import CreditNoteListParams
    from stripe.params._credit_note_modify_params import CreditNoteModifyParams
    from stripe.params._credit_note_preview_lines_params import (
        CreditNotePreviewLinesParams,
    )
    from stripe.params._credit_note_preview_params import (
        CreditNotePreviewParams,
    )
    from stripe.params._credit_note_retrieve_params import (
        CreditNoteRetrieveParams,
    )
    from stripe.params._credit_note_void_credit_note_params import (
        CreditNoteVoidCreditNoteParams,
    )


@nested_resource_class_methods("line")
class CreditNote(
    CreateableAPIResource["CreditNote"],
    ListableAPIResource["CreditNote"],
    UpdateableAPIResource["CreditNote"],
):
    """
    Issue a credit note to adjust an invoice's amount after the invoice is finalized.

    Related guide: [Credit notes](https://docs.stripe.com/billing/invoices/credit-notes)
    """

    OBJECT_NAME: ClassVar[Literal["credit_note"]] = "credit_note"

    class DiscountAmount(StripeObject):
        amount: int
        """
        The amount, in cents (or local equivalent), of the discount.
        """
        discount: ExpandableField["Discount"]
        """
        The discount that was applied to get this discount amount.
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

    class Refund(StripeObject):
        class PaymentRecordRefund(StripeObject):
            payment_record: str
            """
            ID of the payment record.
            """
            refund_group: str
            """
            ID of the refund group.
            """

        amount_refunded: int
        """
        Amount of the refund that applies to this credit note, in cents (or local equivalent).
        """
        payment_record_refund: Optional[PaymentRecordRefund]
        """
        The PaymentRecord refund details associated with this credit note refund.
        """
        refund: ExpandableField["RefundResource"]
        """
        ID of the refund.
        """
        type: Optional[Literal["payment_record_refund", "refund"]]
        """
        Type of the refund, one of `refund` or `payment_record_refund`.
        """
        _inner_class_types = {"payment_record_refund": PaymentRecordRefund}

    class ShippingCost(StripeObject):
        class Tax(StripeObject):
            amount: int
            """
            Amount of tax applied for this rate.
            """
            rate: "TaxRate"
            """
            Tax rates can be applied to [invoices](https://docs.stripe.com/invoicing/taxes/tax-rates), [subscriptions](https://docs.stripe.com/billing/taxes/tax-rates) and [Checkout Sessions](https://docs.stripe.com/payments/checkout/use-manual-tax-rates) to collect tax.

            Related guide: [Tax rates](https://docs.stripe.com/billing/taxes/tax-rates)
            """
            taxability_reason: Optional[
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
            The reasoning behind this tax, for example, if the product is tax exempt. The possible values for this field may be extended as new tax rules are supported.
            """
            taxable_amount: Optional[int]
            """
            The amount on which tax is calculated, in cents (or local equivalent).
            """

        amount_subtotal: int
        """
        Total shipping cost before any taxes are applied.
        """
        amount_tax: int
        """
        Total tax amount applied due to shipping costs. If no tax was applied, defaults to 0.
        """
        amount_total: int
        """
        Total shipping cost after taxes are applied.
        """
        shipping_rate: Optional[ExpandableField["ShippingRate"]]
        """
        The ID of the ShippingRate for this invoice.
        """
        taxes: Optional[List[Tax]]
        """
        The taxes applied to the shipping rate.
        """
        _inner_class_types = {"taxes": Tax}

    class TotalTax(StripeObject):
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
    The integer amount in cents (or local equivalent) representing the total amount of the credit note, including tax.
    """
    amount_shipping: int
    """
    This is the sum of all the shipping amounts.
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    customer: ExpandableField["Customer"]
    """
    ID of the customer.
    """
    customer_account: Optional[str]
    """
    ID of the account representing the customer.
    """
    customer_balance_transaction: Optional[
        ExpandableField["CustomerBalanceTransaction"]
    ]
    """
    Customer balance transaction related to this credit note.
    """
    discount_amount: int
    """
    The integer amount in cents (or local equivalent) representing the total amount of discount that was credited.
    """
    discount_amounts: List[DiscountAmount]
    """
    The aggregate amounts calculated per discount for all line items.
    """
    effective_at: Optional[int]
    """
    The date when this credit note is in effect. Same as `created` unless overwritten. When defined, this value replaces the system-generated 'Date of issue' printed on the credit note PDF.
    """
    id: str
    """
    Unique identifier for the object.
    """
    invoice: ExpandableField["Invoice"]
    """
    ID of the invoice.
    """
    lines: ListObject["CreditNoteLineItem"]
    """
    Line items that make up the credit note
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    memo: Optional[str]
    """
    Customer-facing text that appears on the credit note PDF.
    """
    metadata: Optional[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    number: str
    """
    A unique number that identifies this particular credit note and appears on the PDF of the credit note and its associated invoice.
    """
    object: Literal["credit_note"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    out_of_band_amount: Optional[int]
    """
    Amount that was credited outside of Stripe.
    """
    pdf: str
    """
    The link to download the PDF of the credit note.
    """
    post_payment_amount: int
    """
    The amount of the credit note that was refunded to the customer, credited to the customer's balance, credited outside of Stripe, or any combination thereof.
    """
    pre_payment_amount: int
    """
    The amount of the credit note by which the invoice's `amount_remaining` and `amount_due` were reduced.
    """
    pretax_credit_amounts: List[PretaxCreditAmount]
    """
    The pretax credit amounts (ex: discount, credit grants, etc) for all line items.
    """
    reason: Optional[
        Literal[
            "duplicate", "fraudulent", "order_change", "product_unsatisfactory"
        ]
    ]
    """
    Reason for issuing this credit note, one of `duplicate`, `fraudulent`, `order_change`, or `product_unsatisfactory`
    """
    refunds: List[Refund]
    """
    Refunds related to this credit note.
    """
    shipping_cost: Optional[ShippingCost]
    """
    The details of the cost of shipping, including the ShippingRate applied to the invoice.
    """
    status: Literal["issued", "void"]
    """
    Status of this credit note, one of `issued` or `void`. Learn more about [voiding credit notes](https://docs.stripe.com/billing/invoices/credit-notes#voiding).
    """
    subtotal: int
    """
    The integer amount in cents (or local equivalent) representing the amount of the credit note, excluding exclusive tax and invoice level discounts.
    """
    subtotal_excluding_tax: Optional[int]
    """
    The integer amount in cents (or local equivalent) representing the amount of the credit note, excluding all tax and invoice level discounts.
    """
    total: int
    """
    The integer amount in cents (or local equivalent) representing the total amount of the credit note, including tax and all discount.
    """
    total_excluding_tax: Optional[int]
    """
    The integer amount in cents (or local equivalent) representing the total amount of the credit note, excluding tax, but including discounts.
    """
    total_taxes: Optional[List[TotalTax]]
    """
    The aggregate tax information for all line items.
    """
    type: Literal["mixed", "post_payment", "pre_payment"]
    """
    Type of this credit note, one of `pre_payment` or `post_payment`. A `pre_payment` credit note means it was issued when the invoice was open. A `post_payment` credit note means it was issued when the invoice was paid.
    """
    voided_at: Optional[int]
    """
    The time that the credit note was voided.
    """

    @classmethod
    def create(
        cls, **params: Unpack["CreditNoteCreateParams"]
    ) -> "CreditNote":
        """
        Issue a credit note to adjust the amount of a finalized invoice. A credit note will first reduce the invoice's amount_remaining (and amount_due), but not below zero.
        This amount is indicated by the credit note's pre_payment_amount. The excess amount is indicated by post_payment_amount, and it can result in any combination of the following:


        Refunds: create a new refund (using refund_amount) or link existing refunds (using refunds).
        Customer balance credit: credit the customer's balance (using credit_amount) which will be automatically applied to their next invoice when it's finalized.
        Outside of Stripe credit: record the amount that is or will be credited outside of Stripe (using out_of_band_amount).


        The sum of refunds, customer balance credits, and outside of Stripe credits must equal the post_payment_amount.

        You may issue multiple credit notes for an invoice. Each credit note may increment the invoice's pre_payment_credit_notes_amount,
        post_payment_credit_notes_amount, or both, depending on the invoice's amount_remaining at the time of credit note creation.
        """
        return cast(
            "CreditNote",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["CreditNoteCreateParams"]
    ) -> "CreditNote":
        """
        Issue a credit note to adjust the amount of a finalized invoice. A credit note will first reduce the invoice's amount_remaining (and amount_due), but not below zero.
        This amount is indicated by the credit note's pre_payment_amount. The excess amount is indicated by post_payment_amount, and it can result in any combination of the following:


        Refunds: create a new refund (using refund_amount) or link existing refunds (using refunds).
        Customer balance credit: credit the customer's balance (using credit_amount) which will be automatically applied to their next invoice when it's finalized.
        Outside of Stripe credit: record the amount that is or will be credited outside of Stripe (using out_of_band_amount).


        The sum of refunds, customer balance credits, and outside of Stripe credits must equal the post_payment_amount.

        You may issue multiple credit notes for an invoice. Each credit note may increment the invoice's pre_payment_credit_notes_amount,
        post_payment_credit_notes_amount, or both, depending on the invoice's amount_remaining at the time of credit note creation.
        """
        return cast(
            "CreditNote",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def list(
        cls, **params: Unpack["CreditNoteListParams"]
    ) -> ListObject["CreditNote"]:
        """
        Returns a list of credit notes.
        """
        result = cls._static_request(
            "get",
            cls.class_url(),
            params=params,
        )
        if not isinstance(result, ListObject):
            raise TypeError(
                "Expected list object from API, got %s"
                % (type(result).__name__)
            )

        return result

    @classmethod
    async def list_async(
        cls, **params: Unpack["CreditNoteListParams"]
    ) -> ListObject["CreditNote"]:
        """
        Returns a list of credit notes.
        """
        result = await cls._static_request_async(
            "get",
            cls.class_url(),
            params=params,
        )
        if not isinstance(result, ListObject):
            raise TypeError(
                "Expected list object from API, got %s"
                % (type(result).__name__)
            )

        return result

    @classmethod
    def modify(
        cls, id: str, **params: Unpack["CreditNoteModifyParams"]
    ) -> "CreditNote":
        """
        Updates an existing credit note.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "CreditNote",
            cls._static_request(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    async def modify_async(
        cls, id: str, **params: Unpack["CreditNoteModifyParams"]
    ) -> "CreditNote":
        """
        Updates an existing credit note.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "CreditNote",
            await cls._static_request_async(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    def preview(
        cls, **params: Unpack["CreditNotePreviewParams"]
    ) -> "CreditNote":
        """
        Get a preview of a credit note without creating it.
        """
        return cast(
            "CreditNote",
            cls._static_request(
                "get",
                "/v1/credit_notes/preview",
                params=params,
            ),
        )

    @classmethod
    async def preview_async(
        cls, **params: Unpack["CreditNotePreviewParams"]
    ) -> "CreditNote":
        """
        Get a preview of a credit note without creating it.
        """
        return cast(
            "CreditNote",
            await cls._static_request_async(
                "get",
                "/v1/credit_notes/preview",
                params=params,
            ),
        )

    @classmethod
    def preview_lines(
        cls, **params: Unpack["CreditNotePreviewLinesParams"]
    ) -> ListObject["CreditNoteLineItem"]:
        """
        When retrieving a credit note preview, you'll get a lines property containing the first handful of those items. This URL you can retrieve the full (paginated) list of line items.
        """
        return cast(
            ListObject["CreditNoteLineItem"],
            cls._static_request(
                "get",
                "/v1/credit_notes/preview/lines",
                params=params,
            ),
        )

    @classmethod
    async def preview_lines_async(
        cls, **params: Unpack["CreditNotePreviewLinesParams"]
    ) -> ListObject["CreditNoteLineItem"]:
        """
        When retrieving a credit note preview, you'll get a lines property containing the first handful of those items. This URL you can retrieve the full (paginated) list of line items.
        """
        return cast(
            ListObject["CreditNoteLineItem"],
            await cls._static_request_async(
                "get",
                "/v1/credit_notes/preview/lines",
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["CreditNoteRetrieveParams"]
    ) -> "CreditNote":
        """
        Retrieves the credit note object with the given identifier.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["CreditNoteRetrieveParams"]
    ) -> "CreditNote":
        """
        Retrieves the credit note object with the given identifier.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    @classmethod
    def _cls_void_credit_note(
        cls, id: str, **params: Unpack["CreditNoteVoidCreditNoteParams"]
    ) -> "CreditNote":
        """
        Marks a credit note as void. Learn more about [voiding credit notes](https://docs.stripe.com/docs/billing/invoices/credit-notes#voiding).
        """
        return cast(
            "CreditNote",
            cls._static_request(
                "post",
                "/v1/credit_notes/{id}/void".format(id=sanitize_id(id)),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def void_credit_note(
        id: str, **params: Unpack["CreditNoteVoidCreditNoteParams"]
    ) -> "CreditNote":
        """
        Marks a credit note as void. Learn more about [voiding credit notes](https://docs.stripe.com/docs/billing/invoices/credit-notes#voiding).
        """
        ...

    @overload
    def void_credit_note(
        self, **params: Unpack["CreditNoteVoidCreditNoteParams"]
    ) -> "CreditNote":
        """
        Marks a credit note as void. Learn more about [voiding credit notes](https://docs.stripe.com/docs/billing/invoices/credit-notes#voiding).
        """
        ...

    @class_method_variant("_cls_void_credit_note")
    def void_credit_note(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["CreditNoteVoidCreditNoteParams"]
    ) -> "CreditNote":
        """
        Marks a credit note as void. Learn more about [voiding credit notes](https://docs.stripe.com/docs/billing/invoices/credit-notes#voiding).
        """
        return cast(
            "CreditNote",
            self._request(
                "post",
                "/v1/credit_notes/{id}/void".format(
                    id=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_void_credit_note_async(
        cls, id: str, **params: Unpack["CreditNoteVoidCreditNoteParams"]
    ) -> "CreditNote":
        """
        Marks a credit note as void. Learn more about [voiding credit notes](https://docs.stripe.com/docs/billing/invoices/credit-notes#voiding).
        """
        return cast(
            "CreditNote",
            await cls._static_request_async(
                "post",
                "/v1/credit_notes/{id}/void".format(id=sanitize_id(id)),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def void_credit_note_async(
        id: str, **params: Unpack["CreditNoteVoidCreditNoteParams"]
    ) -> "CreditNote":
        """
        Marks a credit note as void. Learn more about [voiding credit notes](https://docs.stripe.com/docs/billing/invoices/credit-notes#voiding).
        """
        ...

    @overload
    async def void_credit_note_async(
        self, **params: Unpack["CreditNoteVoidCreditNoteParams"]
    ) -> "CreditNote":
        """
        Marks a credit note as void. Learn more about [voiding credit notes](https://docs.stripe.com/docs/billing/invoices/credit-notes#voiding).
        """
        ...

    @class_method_variant("_cls_void_credit_note_async")
    async def void_credit_note_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["CreditNoteVoidCreditNoteParams"]
    ) -> "CreditNote":
        """
        Marks a credit note as void. Learn more about [voiding credit notes](https://docs.stripe.com/docs/billing/invoices/credit-notes#voiding).
        """
        return cast(
            "CreditNote",
            await self._request_async(
                "post",
                "/v1/credit_notes/{id}/void".format(
                    id=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def list_lines(
        cls, credit_note: str, **params: Unpack["CreditNoteListLinesParams"]
    ) -> ListObject["CreditNoteLineItem"]:
        """
        When retrieving a credit note, you'll get a lines property containing the first handful of those items. There is also a URL where you can retrieve the full (paginated) list of line items.
        """
        return cast(
            ListObject["CreditNoteLineItem"],
            cls._static_request(
                "get",
                "/v1/credit_notes/{credit_note}/lines".format(
                    credit_note=sanitize_id(credit_note)
                ),
                params=params,
            ),
        )

    @classmethod
    async def list_lines_async(
        cls, credit_note: str, **params: Unpack["CreditNoteListLinesParams"]
    ) -> ListObject["CreditNoteLineItem"]:
        """
        When retrieving a credit note, you'll get a lines property containing the first handful of those items. There is also a URL where you can retrieve the full (paginated) list of line items.
        """
        return cast(
            ListObject["CreditNoteLineItem"],
            await cls._static_request_async(
                "get",
                "/v1/credit_notes/{credit_note}/lines".format(
                    credit_note=sanitize_id(credit_note)
                ),
                params=params,
            ),
        )

    _inner_class_types = {
        "discount_amounts": DiscountAmount,
        "pretax_credit_amounts": PretaxCreditAmount,
        "refunds": Refund,
        "shipping_cost": ShippingCost,
        "total_taxes": TotalTax,
    }
