# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class QuoteModifyParams(RequestOptions):
    application_fee_amount: NotRequired["Literal['']|int"]
    """
    The amount of the application fee (if any) that will be requested to be applied to the payment and transferred to the application owner's Stripe account. There cannot be any line items with recurring prices when using this field.
    """
    application_fee_percent: NotRequired["Literal['']|float"]
    """
    A non-negative decimal between 0 and 100, with at most two decimal places. This represents the percentage of the subscription invoice total that will be transferred to the application owner's Stripe account. There must be at least 1 line item with a recurring price to use this field.
    """
    automatic_tax: NotRequired["QuoteModifyParamsAutomaticTax"]
    """
    Settings for automatic tax lookup for this quote and resulting invoices and subscriptions.
    """
    collection_method: NotRequired[
        Literal["charge_automatically", "send_invoice"]
    ]
    """
    Either `charge_automatically`, or `send_invoice`. When charging automatically, Stripe will attempt to pay invoices at the end of the subscription cycle or at invoice finalization using the default payment method attached to the subscription or customer. When sending an invoice, Stripe will email your customer an invoice with payment instructions and mark the subscription as `active`. Defaults to `charge_automatically`.
    """
    customer: NotRequired[str]
    """
    The customer for which this quote belongs to. A customer is required before finalizing the quote. Once specified, it cannot be changed.
    """
    customer_account: NotRequired[str]
    """
    The account for which this quote belongs to. A customer or account is required before finalizing the quote. Once specified, it cannot be changed.
    """
    default_tax_rates: NotRequired["Literal['']|List[str]"]
    """
    The tax rates that will apply to any line item that does not have `tax_rates` set.
    """
    description: NotRequired["Literal['']|str"]
    """
    A description that will be displayed on the quote PDF.
    """
    discounts: NotRequired["Literal['']|List[QuoteModifyParamsDiscount]"]
    """
    The discounts applied to the quote.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    expires_at: NotRequired[int]
    """
    A future timestamp on which the quote will be canceled if in `open` or `draft` status. Measured in seconds since the Unix epoch.
    """
    footer: NotRequired["Literal['']|str"]
    """
    A footer that will be displayed on the quote PDF.
    """
    header: NotRequired["Literal['']|str"]
    """
    A header that will be displayed on the quote PDF.
    """
    invoice_settings: NotRequired["QuoteModifyParamsInvoiceSettings"]
    """
    All invoices will be billed using the specified settings.
    """
    line_items: NotRequired[List["QuoteModifyParamsLineItem"]]
    """
    A list of line items the customer is being quoted for. Each line item includes information about the product, the quantity, and the resulting cost.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    on_behalf_of: NotRequired["Literal['']|str"]
    """
    The account on behalf of which to charge.
    """
    subscription_data: NotRequired["QuoteModifyParamsSubscriptionData"]
    """
    When creating a subscription or subscription schedule, the specified configuration data will be used. There must be at least one line item with a recurring price for a subscription or subscription schedule to be created. A subscription schedule is created if `subscription_data[effective_date]` is present and in the future, otherwise a subscription is created.
    """
    transfer_data: NotRequired["Literal['']|QuoteModifyParamsTransferData"]
    """
    The data with which to automatically create a Transfer for each of the invoices.
    """


class QuoteModifyParamsAutomaticTax(TypedDict):
    enabled: bool
    """
    Controls whether Stripe will automatically compute tax on the resulting invoices or subscriptions as well as the quote itself.
    """
    liability: NotRequired["QuoteModifyParamsAutomaticTaxLiability"]
    """
    The account that's liable for tax. If set, the business address and tax registrations required to perform the tax calculation are loaded from this account. The tax transaction is returned in the report of the connected account.
    """


class QuoteModifyParamsAutomaticTaxLiability(TypedDict):
    account: NotRequired[str]
    """
    The connected account being referenced when `type` is `account`.
    """
    type: Literal["account", "self"]
    """
    Type of the account referenced in the request.
    """


class QuoteModifyParamsDiscount(TypedDict):
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


class QuoteModifyParamsInvoiceSettings(TypedDict):
    days_until_due: NotRequired[int]
    """
    Number of days within which a customer must pay the invoice generated by this quote. This value will be `null` for quotes where `collection_method=charge_automatically`.
    """
    issuer: NotRequired["QuoteModifyParamsInvoiceSettingsIssuer"]
    """
    The connected account that issues the invoice. The invoice is presented with the branding and support information of the specified account.
    """


class QuoteModifyParamsInvoiceSettingsIssuer(TypedDict):
    account: NotRequired[str]
    """
    The connected account being referenced when `type` is `account`.
    """
    type: Literal["account", "self"]
    """
    Type of the account referenced in the request.
    """


class QuoteModifyParamsLineItem(TypedDict):
    discounts: NotRequired[
        "Literal['']|List[QuoteModifyParamsLineItemDiscount]"
    ]
    """
    The discounts applied to this line item.
    """
    id: NotRequired[str]
    """
    The ID of an existing line item on the quote.
    """
    price: NotRequired[str]
    """
    The ID of the price object. One of `price` or `price_data` is required.
    """
    price_data: NotRequired["QuoteModifyParamsLineItemPriceData"]
    """
    Data used to generate a new [Price](https://docs.stripe.com/api/prices) object inline. One of `price` or `price_data` is required.
    """
    quantity: NotRequired[int]
    """
    The quantity of the line item.
    """
    tax_rates: NotRequired["Literal['']|List[str]"]
    """
    The tax rates which apply to the line item. When set, the `default_tax_rates` on the quote do not apply to this line item.
    """


class QuoteModifyParamsLineItemDiscount(TypedDict):
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


class QuoteModifyParamsLineItemPriceData(TypedDict):
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    product: str
    """
    The ID of the [Product](https://docs.stripe.com/api/products) that this [Price](https://docs.stripe.com/api/prices) will belong to.
    """
    recurring: NotRequired["QuoteModifyParamsLineItemPriceDataRecurring"]
    """
    The recurring components of a price such as `interval` and `interval_count`.
    """
    tax_behavior: NotRequired[Literal["exclusive", "inclusive", "unspecified"]]
    """
    Only required if a [default tax behavior](https://docs.stripe.com/tax/products-prices-tax-categories-tax-behavior#setting-a-default-tax-behavior-(recommended)) was not provided in the Stripe Tax settings. Specifies whether the price is considered inclusive of taxes or exclusive of taxes. One of `inclusive`, `exclusive`, or `unspecified`. Once specified as either `inclusive` or `exclusive`, it cannot be changed.
    """
    unit_amount: NotRequired[int]
    """
    A positive integer in cents (or local equivalent) (or 0 for a free price) representing how much to charge.
    """
    unit_amount_decimal: NotRequired[str]
    """
    Same as `unit_amount`, but accepts a decimal value in cents (or local equivalent) with at most 12 decimal places. Only one of `unit_amount` and `unit_amount_decimal` can be set.
    """


class QuoteModifyParamsLineItemPriceDataRecurring(TypedDict):
    interval: Literal["day", "month", "week", "year"]
    """
    Specifies billing frequency. Either `day`, `week`, `month` or `year`.
    """
    interval_count: NotRequired[int]
    """
    The number of intervals between subscription billings. For example, `interval=month` and `interval_count=3` bills every 3 months. Maximum of three years interval allowed (3 years, 36 months, or 156 weeks).
    """


class QuoteModifyParamsSubscriptionData(TypedDict):
    description: NotRequired["Literal['']|str"]
    """
    The subscription's description, meant to be displayable to the customer. Use this field to optionally store an explanation of the subscription for rendering in Stripe surfaces and certain local payment methods UIs.
    """
    effective_date: NotRequired[
        "Literal['']|Literal['current_period_end']|int"
    ]
    """
    When creating a new subscription, the date of which the subscription schedule will start after the quote is accepted. The `effective_date` is ignored if it is in the past when the quote is accepted.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that will set metadata on the subscription or subscription schedule when the quote is accepted. If a recurring price is included in `line_items`, this field will be passed to the resulting subscription's `metadata` field. If `subscription_data.effective_date` is used, this field will be passed to the resulting subscription schedule's `phases.metadata` field. Unlike object-level metadata, this field is declarative. Updates will clear prior values.
    """
    trial_period_days: NotRequired["Literal['']|int"]
    """
    Integer representing the number of trial period days before the customer is charged for the first time.
    """


class QuoteModifyParamsTransferData(TypedDict):
    amount: NotRequired[int]
    """
    The amount that will be transferred automatically when the invoice is paid. If no amount is set, the full amount is transferred. There cannot be any line items with recurring prices when using this field.
    """
    amount_percent: NotRequired[float]
    """
    A non-negative decimal between 0 and 100, with at most two decimal places. This represents the percentage of the subscription invoice total that will be transferred to the destination account. By default, the entire amount is transferred to the destination. There must be at least 1 line item with a recurring price to use this field.
    """
    destination: str
    """
    ID of an existing, connected Stripe account.
    """
