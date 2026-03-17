# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class PaymentLinkCreateParams(RequestOptions):
    after_completion: NotRequired["PaymentLinkCreateParamsAfterCompletion"]
    """
    Behavior after the purchase is complete.
    """
    allow_promotion_codes: NotRequired[bool]
    """
    Enables user redeemable promotion codes.
    """
    application_fee_amount: NotRequired[int]
    """
    The amount of the application fee (if any) that will be requested to be applied to the payment and transferred to the application owner's Stripe account. Can only be applied when there are no line items with recurring prices.
    """
    application_fee_percent: NotRequired[float]
    """
    A non-negative decimal between 0 and 100, with at most two decimal places. This represents the percentage of the subscription invoice total that will be transferred to the application owner's Stripe account. There must be at least 1 line item with a recurring price to use this field.
    """
    automatic_tax: NotRequired["PaymentLinkCreateParamsAutomaticTax"]
    """
    Configuration for automatic tax collection.
    """
    billing_address_collection: NotRequired[Literal["auto", "required"]]
    """
    Configuration for collecting the customer's billing address. Defaults to `auto`.
    """
    consent_collection: NotRequired["PaymentLinkCreateParamsConsentCollection"]
    """
    Configure fields to gather active consent from customers.
    """
    currency: NotRequired[str]
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies) and supported by each line item's price.
    """
    custom_fields: NotRequired[List["PaymentLinkCreateParamsCustomField"]]
    """
    Collect additional information from your customer using custom fields. Up to 3 fields are supported. You can't set this parameter if `ui_mode` is `custom`.
    """
    custom_text: NotRequired["PaymentLinkCreateParamsCustomText"]
    """
    Display additional text for your customers using custom text. You can't set this parameter if `ui_mode` is `custom`.
    """
    customer_creation: NotRequired[Literal["always", "if_required"]]
    """
    Configures whether [checkout sessions](https://docs.stripe.com/api/checkout/sessions) created by this payment link create a [Customer](https://docs.stripe.com/api/customers).
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    inactive_message: NotRequired[str]
    """
    The custom message to be displayed to a customer when a payment link is no longer active.
    """
    invoice_creation: NotRequired["PaymentLinkCreateParamsInvoiceCreation"]
    """
    Generate a post-purchase Invoice for one-time payments.
    """
    line_items: List["PaymentLinkCreateParamsLineItem"]
    """
    The line items representing what is being sold. Each line item represents an item being sold. Up to 20 line items are supported.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`. Metadata associated with this Payment Link will automatically be copied to [checkout sessions](https://docs.stripe.com/api/checkout/sessions) created by this payment link.
    """
    name_collection: NotRequired["PaymentLinkCreateParamsNameCollection"]
    """
    Controls settings applied for collecting the customer's name.
    """
    on_behalf_of: NotRequired[str]
    """
    The account on behalf of which to charge.
    """
    optional_items: NotRequired[List["PaymentLinkCreateParamsOptionalItem"]]
    """
    A list of optional items the customer can add to their order at checkout. Use this parameter to pass one-time or recurring [Prices](https://docs.stripe.com/api/prices).
    There is a maximum of 10 optional items allowed on a payment link, and the existing limits on the number of line items allowed on a payment link apply to the combined number of line items and optional items.
    There is a maximum of 20 combined line items and optional items.
    """
    payment_intent_data: NotRequired[
        "PaymentLinkCreateParamsPaymentIntentData"
    ]
    """
    A subset of parameters to be passed to PaymentIntent creation for Checkout Sessions in `payment` mode.
    """
    payment_method_collection: NotRequired[Literal["always", "if_required"]]
    """
    Specify whether Checkout should collect a payment method. When set to `if_required`, Checkout will not collect a payment method when the total due for the session is 0.This may occur if the Checkout Session includes a free trial or a discount.

    Can only be set in `subscription` mode. Defaults to `always`.

    If you'd like information on how to collect a payment method outside of Checkout, read the guide on [configuring subscriptions with a free trial](https://docs.stripe.com/payments/checkout/free-trials).
    """
    payment_method_types: NotRequired[
        List[
            Literal[
                "affirm",
                "afterpay_clearpay",
                "alipay",
                "alma",
                "au_becs_debit",
                "bacs_debit",
                "bancontact",
                "billie",
                "blik",
                "boleto",
                "card",
                "cashapp",
                "eps",
                "fpx",
                "giropay",
                "grabpay",
                "ideal",
                "klarna",
                "konbini",
                "link",
                "mb_way",
                "mobilepay",
                "multibanco",
                "oxxo",
                "p24",
                "pay_by_bank",
                "paynow",
                "paypal",
                "payto",
                "pix",
                "promptpay",
                "satispay",
                "sepa_debit",
                "sofort",
                "swish",
                "twint",
                "us_bank_account",
                "wechat_pay",
                "zip",
            ]
        ]
    ]
    """
    The list of payment method types that customers can use. If no value is passed, Stripe will dynamically show relevant payment methods from your [payment method settings](https://dashboard.stripe.com/settings/payment_methods) (20+ payment methods [supported](https://docs.stripe.com/payments/payment-methods/integration-options#payment-method-product-support)).
    """
    phone_number_collection: NotRequired[
        "PaymentLinkCreateParamsPhoneNumberCollection"
    ]
    """
    Controls phone number collection settings during checkout.

    We recommend that you review your privacy policy and check with your legal contacts.
    """
    restrictions: NotRequired["PaymentLinkCreateParamsRestrictions"]
    """
    Settings that restrict the usage of a payment link.
    """
    shipping_address_collection: NotRequired[
        "PaymentLinkCreateParamsShippingAddressCollection"
    ]
    """
    Configuration for collecting the customer's shipping address.
    """
    shipping_options: NotRequired[
        List["PaymentLinkCreateParamsShippingOption"]
    ]
    """
    The shipping rate options to apply to [checkout sessions](https://docs.stripe.com/api/checkout/sessions) created by this payment link.
    """
    submit_type: NotRequired[
        Literal["auto", "book", "donate", "pay", "subscribe"]
    ]
    """
    Describes the type of transaction being performed in order to customize relevant text on the page, such as the submit button. Changing this value will also affect the hostname in the [url](https://docs.stripe.com/api/payment_links/payment_links/object#url) property (example: `donate.stripe.com`).
    """
    subscription_data: NotRequired["PaymentLinkCreateParamsSubscriptionData"]
    """
    When creating a subscription, the specified configuration data will be used. There must be at least one line item with a recurring price to use `subscription_data`.
    """
    tax_id_collection: NotRequired["PaymentLinkCreateParamsTaxIdCollection"]
    """
    Controls tax ID collection during checkout.
    """
    transfer_data: NotRequired["PaymentLinkCreateParamsTransferData"]
    """
    The account (if any) the payments will be attributed to for tax reporting, and where funds from each payment will be transferred to.
    """


class PaymentLinkCreateParamsAfterCompletion(TypedDict):
    hosted_confirmation: NotRequired[
        "PaymentLinkCreateParamsAfterCompletionHostedConfirmation"
    ]
    """
    Configuration when `type=hosted_confirmation`.
    """
    redirect: NotRequired["PaymentLinkCreateParamsAfterCompletionRedirect"]
    """
    Configuration when `type=redirect`.
    """
    type: Literal["hosted_confirmation", "redirect"]
    """
    The specified behavior after the purchase is complete. Either `redirect` or `hosted_confirmation`.
    """


class PaymentLinkCreateParamsAfterCompletionHostedConfirmation(TypedDict):
    custom_message: NotRequired[str]
    """
    A custom message to display to the customer after the purchase is complete.
    """


class PaymentLinkCreateParamsAfterCompletionRedirect(TypedDict):
    url: str
    """
    The URL the customer will be redirected to after the purchase is complete. You can embed `{CHECKOUT_SESSION_ID}` into the URL to have the `id` of the completed [checkout session](https://docs.stripe.com/api/checkout/sessions/object#checkout_session_object-id) included.
    """


class PaymentLinkCreateParamsAutomaticTax(TypedDict):
    enabled: bool
    """
    Set to `true` to [calculate tax automatically](https://docs.stripe.com/tax) using the customer's location.

    Enabling this parameter causes the payment link to collect any billing address information necessary for tax calculation.
    """
    liability: NotRequired["PaymentLinkCreateParamsAutomaticTaxLiability"]
    """
    The account that's liable for tax. If set, the business address and tax registrations required to perform the tax calculation are loaded from this account. The tax transaction is returned in the report of the connected account.
    """


class PaymentLinkCreateParamsAutomaticTaxLiability(TypedDict):
    account: NotRequired[str]
    """
    The connected account being referenced when `type` is `account`.
    """
    type: Literal["account", "self"]
    """
    Type of the account referenced in the request.
    """


class PaymentLinkCreateParamsConsentCollection(TypedDict):
    payment_method_reuse_agreement: NotRequired[
        "PaymentLinkCreateParamsConsentCollectionPaymentMethodReuseAgreement"
    ]
    """
    Determines the display of payment method reuse agreement text in the UI. If set to `hidden`, it will hide legal text related to the reuse of a payment method.
    """
    promotions: NotRequired[Literal["auto", "none"]]
    """
    If set to `auto`, enables the collection of customer consent for promotional communications. The Checkout
    Session will determine whether to display an option to opt into promotional communication
    from the merchant depending on the customer's locale. Only available to US merchants.
    """
    terms_of_service: NotRequired[Literal["none", "required"]]
    """
    If set to `required`, it requires customers to check a terms of service checkbox before being able to pay.
    There must be a valid terms of service URL set in your [Dashboard settings](https://dashboard.stripe.com/settings/public).
    """


class PaymentLinkCreateParamsConsentCollectionPaymentMethodReuseAgreement(
    TypedDict,
):
    position: Literal["auto", "hidden"]
    """
    Determines the position and visibility of the payment method reuse agreement in the UI. When set to `auto`, Stripe's
    defaults will be used. When set to `hidden`, the payment method reuse agreement text will always be hidden in the UI.
    """


class PaymentLinkCreateParamsCustomField(TypedDict):
    dropdown: NotRequired["PaymentLinkCreateParamsCustomFieldDropdown"]
    """
    Configuration for `type=dropdown` fields.
    """
    key: str
    """
    String of your choice that your integration can use to reconcile this field. Must be unique to this field, alphanumeric, and up to 200 characters.
    """
    label: "PaymentLinkCreateParamsCustomFieldLabel"
    """
    The label for the field, displayed to the customer.
    """
    numeric: NotRequired["PaymentLinkCreateParamsCustomFieldNumeric"]
    """
    Configuration for `type=numeric` fields.
    """
    optional: NotRequired[bool]
    """
    Whether the customer is required to complete the field before completing the Checkout Session. Defaults to `false`.
    """
    text: NotRequired["PaymentLinkCreateParamsCustomFieldText"]
    """
    Configuration for `type=text` fields.
    """
    type: Literal["dropdown", "numeric", "text"]
    """
    The type of the field.
    """


class PaymentLinkCreateParamsCustomFieldDropdown(TypedDict):
    default_value: NotRequired[str]
    """
    The value that pre-fills the field on the payment page.Must match a `value` in the `options` array.
    """
    options: List["PaymentLinkCreateParamsCustomFieldDropdownOption"]
    """
    The options available for the customer to select. Up to 200 options allowed.
    """


class PaymentLinkCreateParamsCustomFieldDropdownOption(TypedDict):
    label: str
    """
    The label for the option, displayed to the customer. Up to 100 characters.
    """
    value: str
    """
    The value for this option, not displayed to the customer, used by your integration to reconcile the option selected by the customer. Must be unique to this option, alphanumeric, and up to 100 characters.
    """


class PaymentLinkCreateParamsCustomFieldLabel(TypedDict):
    custom: str
    """
    Custom text for the label, displayed to the customer. Up to 50 characters.
    """
    type: Literal["custom"]
    """
    The type of the label.
    """


class PaymentLinkCreateParamsCustomFieldNumeric(TypedDict):
    default_value: NotRequired[str]
    """
    The value that pre-fills the field on the payment page.
    """
    maximum_length: NotRequired[int]
    """
    The maximum character length constraint for the customer's input.
    """
    minimum_length: NotRequired[int]
    """
    The minimum character length requirement for the customer's input.
    """


class PaymentLinkCreateParamsCustomFieldText(TypedDict):
    default_value: NotRequired[str]
    """
    The value that pre-fills the field on the payment page.
    """
    maximum_length: NotRequired[int]
    """
    The maximum character length constraint for the customer's input.
    """
    minimum_length: NotRequired[int]
    """
    The minimum character length requirement for the customer's input.
    """


class PaymentLinkCreateParamsCustomText(TypedDict):
    after_submit: NotRequired[
        "Literal['']|PaymentLinkCreateParamsCustomTextAfterSubmit"
    ]
    """
    Custom text that should be displayed after the payment confirmation button.
    """
    shipping_address: NotRequired[
        "Literal['']|PaymentLinkCreateParamsCustomTextShippingAddress"
    ]
    """
    Custom text that should be displayed alongside shipping address collection.
    """
    submit: NotRequired["Literal['']|PaymentLinkCreateParamsCustomTextSubmit"]
    """
    Custom text that should be displayed alongside the payment confirmation button.
    """
    terms_of_service_acceptance: NotRequired[
        "Literal['']|PaymentLinkCreateParamsCustomTextTermsOfServiceAcceptance"
    ]
    """
    Custom text that should be displayed in place of the default terms of service agreement text.
    """


class PaymentLinkCreateParamsCustomTextAfterSubmit(TypedDict):
    message: str
    """
    Text can be up to 1200 characters in length.
    """


class PaymentLinkCreateParamsCustomTextShippingAddress(TypedDict):
    message: str
    """
    Text can be up to 1200 characters in length.
    """


class PaymentLinkCreateParamsCustomTextSubmit(TypedDict):
    message: str
    """
    Text can be up to 1200 characters in length.
    """


class PaymentLinkCreateParamsCustomTextTermsOfServiceAcceptance(TypedDict):
    message: str
    """
    Text can be up to 1200 characters in length.
    """


class PaymentLinkCreateParamsInvoiceCreation(TypedDict):
    enabled: bool
    """
    Whether the feature is enabled
    """
    invoice_data: NotRequired[
        "PaymentLinkCreateParamsInvoiceCreationInvoiceData"
    ]
    """
    Invoice PDF configuration.
    """


class PaymentLinkCreateParamsInvoiceCreationInvoiceData(TypedDict):
    account_tax_ids: NotRequired["Literal['']|List[str]"]
    """
    The account tax IDs associated with the invoice.
    """
    custom_fields: NotRequired[
        "Literal['']|List[PaymentLinkCreateParamsInvoiceCreationInvoiceDataCustomField]"
    ]
    """
    Default custom fields to be displayed on invoices for this customer.
    """
    description: NotRequired[str]
    """
    An arbitrary string attached to the object. Often useful for displaying to users.
    """
    footer: NotRequired[str]
    """
    Default footer to be displayed on invoices for this customer.
    """
    issuer: NotRequired[
        "PaymentLinkCreateParamsInvoiceCreationInvoiceDataIssuer"
    ]
    """
    The connected account that issues the invoice. The invoice is presented with the branding and support information of the specified account.
    """
    metadata: NotRequired["Literal['']|Dict[str, str]"]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    rendering_options: NotRequired[
        "Literal['']|PaymentLinkCreateParamsInvoiceCreationInvoiceDataRenderingOptions"
    ]
    """
    Default options for invoice PDF rendering for this customer.
    """


class PaymentLinkCreateParamsInvoiceCreationInvoiceDataCustomField(TypedDict):
    name: str
    """
    The name of the custom field. This may be up to 40 characters.
    """
    value: str
    """
    The value of the custom field. This may be up to 140 characters.
    """


class PaymentLinkCreateParamsInvoiceCreationInvoiceDataIssuer(TypedDict):
    account: NotRequired[str]
    """
    The connected account being referenced when `type` is `account`.
    """
    type: Literal["account", "self"]
    """
    Type of the account referenced in the request.
    """


class PaymentLinkCreateParamsInvoiceCreationInvoiceDataRenderingOptions(
    TypedDict,
):
    amount_tax_display: NotRequired[
        "Literal['']|Literal['exclude_tax', 'include_inclusive_tax']"
    ]
    """
    How line-item prices and amounts will be displayed with respect to tax on invoice PDFs. One of `exclude_tax` or `include_inclusive_tax`. `include_inclusive_tax` will include inclusive tax (and exclude exclusive tax) in invoice PDF amounts. `exclude_tax` will exclude all tax (inclusive and exclusive alike) from invoice PDF amounts.
    """
    template: NotRequired[str]
    """
    ID of the invoice rendering template to use for this invoice.
    """


class PaymentLinkCreateParamsLineItem(TypedDict):
    adjustable_quantity: NotRequired[
        "PaymentLinkCreateParamsLineItemAdjustableQuantity"
    ]
    """
    When set, provides configuration for this item's quantity to be adjusted by the customer during checkout.
    """
    price: NotRequired[str]
    """
    The ID of the [Price](https://docs.stripe.com/api/prices) or [Plan](https://docs.stripe.com/api/plans) object. One of `price` or `price_data` is required.
    """
    price_data: NotRequired["PaymentLinkCreateParamsLineItemPriceData"]
    """
    Data used to generate a new [Price](https://docs.stripe.com/api/prices) object inline. One of `price` or `price_data` is required.
    """
    quantity: int
    """
    The quantity of the line item being purchased.
    """


class PaymentLinkCreateParamsLineItemAdjustableQuantity(TypedDict):
    enabled: bool
    """
    Set to true if the quantity can be adjusted to any non-negative Integer.
    """
    maximum: NotRequired[int]
    """
    The maximum quantity the customer can purchase. By default this value is 99. You can specify a value up to 999999.
    """
    minimum: NotRequired[int]
    """
    The minimum quantity the customer can purchase. By default this value is 0. If there is only one item in the cart then that item's quantity cannot go down to 0.
    """


class PaymentLinkCreateParamsLineItemPriceData(TypedDict):
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    product: NotRequired[str]
    """
    The ID of the [Product](https://docs.stripe.com/api/products) that this [Price](https://docs.stripe.com/api/prices) will belong to. One of `product` or `product_data` is required.
    """
    product_data: NotRequired[
        "PaymentLinkCreateParamsLineItemPriceDataProductData"
    ]
    """
    Data used to generate a new [Product](https://docs.stripe.com/api/products) object inline. One of `product` or `product_data` is required.
    """
    recurring: NotRequired["PaymentLinkCreateParamsLineItemPriceDataRecurring"]
    """
    The recurring components of a price such as `interval` and `interval_count`.
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


class PaymentLinkCreateParamsLineItemPriceDataProductData(TypedDict):
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


class PaymentLinkCreateParamsLineItemPriceDataRecurring(TypedDict):
    interval: Literal["day", "month", "week", "year"]
    """
    Specifies billing frequency. Either `day`, `week`, `month` or `year`.
    """
    interval_count: NotRequired[int]
    """
    The number of intervals between subscription billings. For example, `interval=month` and `interval_count=3` bills every 3 months. Maximum of three years interval allowed (3 years, 36 months, or 156 weeks).
    """


class PaymentLinkCreateParamsNameCollection(TypedDict):
    business: NotRequired["PaymentLinkCreateParamsNameCollectionBusiness"]
    """
    Controls settings applied for collecting the customer's business name.
    """
    individual: NotRequired["PaymentLinkCreateParamsNameCollectionIndividual"]
    """
    Controls settings applied for collecting the customer's individual name.
    """


class PaymentLinkCreateParamsNameCollectionBusiness(TypedDict):
    enabled: bool
    """
    Enable business name collection on the payment link. Defaults to `false`.
    """
    optional: NotRequired[bool]
    """
    Whether the customer is required to provide their business name before checking out. Defaults to `false`.
    """


class PaymentLinkCreateParamsNameCollectionIndividual(TypedDict):
    enabled: bool
    """
    Enable individual name collection on the payment link. Defaults to `false`.
    """
    optional: NotRequired[bool]
    """
    Whether the customer is required to provide their full name before checking out. Defaults to `false`.
    """


class PaymentLinkCreateParamsOptionalItem(TypedDict):
    adjustable_quantity: NotRequired[
        "PaymentLinkCreateParamsOptionalItemAdjustableQuantity"
    ]
    """
    When set, provides configuration for the customer to adjust the quantity of the line item created when a customer chooses to add this optional item to their order.
    """
    price: str
    """
    The ID of the [Price](https://docs.stripe.com/api/prices) or [Plan](https://docs.stripe.com/api/plans) object.
    """
    quantity: int
    """
    The initial quantity of the line item created when a customer chooses to add this optional item to their order.
    """


class PaymentLinkCreateParamsOptionalItemAdjustableQuantity(TypedDict):
    enabled: bool
    """
    Set to true if the quantity can be adjusted to any non-negative integer.
    """
    maximum: NotRequired[int]
    """
    The maximum quantity of this item the customer can purchase. By default this value is 99.
    """
    minimum: NotRequired[int]
    """
    The minimum quantity of this item the customer must purchase, if they choose to purchase it. Because this item is optional, the customer will always be able to remove it from their order, even if the `minimum` configured here is greater than 0. By default this value is 0.
    """


class PaymentLinkCreateParamsPaymentIntentData(TypedDict):
    capture_method: NotRequired[
        Literal["automatic", "automatic_async", "manual"]
    ]
    """
    Controls when the funds will be captured from the customer's account.
    """
    description: NotRequired[str]
    """
    An arbitrary string attached to the object. Often useful for displaying to users.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that will declaratively set metadata on [Payment Intents](https://docs.stripe.com/api/payment_intents) generated from this payment link. Unlike object-level metadata, this field is declarative. Updates will clear prior values.
    """
    setup_future_usage: NotRequired[Literal["off_session", "on_session"]]
    """
    Indicates that you intend to [make future payments](https://docs.stripe.com/payments/payment-intents#future-usage) with the payment method collected by this Checkout Session.

    When setting this to `on_session`, Checkout will show a notice to the customer that their payment details will be saved.

    When setting this to `off_session`, Checkout will show a notice to the customer that their payment details will be saved and used for future payments.

    If a Customer has been provided or Checkout creates a new Customer,Checkout will attach the payment method to the Customer.

    If Checkout does not create a Customer, the payment method is not attached to a Customer. To reuse the payment method, you can retrieve it from the Checkout Session's PaymentIntent.

    When processing card payments, Checkout also uses `setup_future_usage` to dynamically optimize your payment flow and comply with regional legislation and network rules, such as SCA.
    """
    statement_descriptor: NotRequired[str]
    """
    Text that appears on the customer's statement as the statement descriptor for a non-card charge. This value overrides the account's default statement descriptor. For information about requirements, including the 22-character limit, see [the Statement Descriptor docs](https://docs.stripe.com/get-started/account/statement-descriptors).

    Setting this value for a card charge returns an error. For card charges, set the [statement_descriptor_suffix](https://docs.stripe.com/get-started/account/statement-descriptors#dynamic) instead.
    """
    statement_descriptor_suffix: NotRequired[str]
    """
    Provides information about a card charge. Concatenated to the account's [statement descriptor prefix](https://docs.stripe.com/get-started/account/statement-descriptors#static) to form the complete statement descriptor that appears on the customer's statement.
    """
    transfer_group: NotRequired[str]
    """
    A string that identifies the resulting payment as part of a group. See the PaymentIntents [use case for connected accounts](https://docs.stripe.com/connect/separate-charges-and-transfers) for details.
    """


class PaymentLinkCreateParamsPhoneNumberCollection(TypedDict):
    enabled: bool
    """
    Set to `true` to enable phone number collection.
    """


class PaymentLinkCreateParamsRestrictions(TypedDict):
    completed_sessions: "PaymentLinkCreateParamsRestrictionsCompletedSessions"
    """
    Configuration for the `completed_sessions` restriction type.
    """


class PaymentLinkCreateParamsRestrictionsCompletedSessions(TypedDict):
    limit: int
    """
    The maximum number of checkout sessions that can be completed for the `completed_sessions` restriction to be met.
    """


class PaymentLinkCreateParamsShippingAddressCollection(TypedDict):
    allowed_countries: List[
        Literal[
            "AC",
            "AD",
            "AE",
            "AF",
            "AG",
            "AI",
            "AL",
            "AM",
            "AO",
            "AQ",
            "AR",
            "AT",
            "AU",
            "AW",
            "AX",
            "AZ",
            "BA",
            "BB",
            "BD",
            "BE",
            "BF",
            "BG",
            "BH",
            "BI",
            "BJ",
            "BL",
            "BM",
            "BN",
            "BO",
            "BQ",
            "BR",
            "BS",
            "BT",
            "BV",
            "BW",
            "BY",
            "BZ",
            "CA",
            "CD",
            "CF",
            "CG",
            "CH",
            "CI",
            "CK",
            "CL",
            "CM",
            "CN",
            "CO",
            "CR",
            "CV",
            "CW",
            "CY",
            "CZ",
            "DE",
            "DJ",
            "DK",
            "DM",
            "DO",
            "DZ",
            "EC",
            "EE",
            "EG",
            "EH",
            "ER",
            "ES",
            "ET",
            "FI",
            "FJ",
            "FK",
            "FO",
            "FR",
            "GA",
            "GB",
            "GD",
            "GE",
            "GF",
            "GG",
            "GH",
            "GI",
            "GL",
            "GM",
            "GN",
            "GP",
            "GQ",
            "GR",
            "GS",
            "GT",
            "GU",
            "GW",
            "GY",
            "HK",
            "HN",
            "HR",
            "HT",
            "HU",
            "ID",
            "IE",
            "IL",
            "IM",
            "IN",
            "IO",
            "IQ",
            "IS",
            "IT",
            "JE",
            "JM",
            "JO",
            "JP",
            "KE",
            "KG",
            "KH",
            "KI",
            "KM",
            "KN",
            "KR",
            "KW",
            "KY",
            "KZ",
            "LA",
            "LB",
            "LC",
            "LI",
            "LK",
            "LR",
            "LS",
            "LT",
            "LU",
            "LV",
            "LY",
            "MA",
            "MC",
            "MD",
            "ME",
            "MF",
            "MG",
            "MK",
            "ML",
            "MM",
            "MN",
            "MO",
            "MQ",
            "MR",
            "MS",
            "MT",
            "MU",
            "MV",
            "MW",
            "MX",
            "MY",
            "MZ",
            "NA",
            "NC",
            "NE",
            "NG",
            "NI",
            "NL",
            "NO",
            "NP",
            "NR",
            "NU",
            "NZ",
            "OM",
            "PA",
            "PE",
            "PF",
            "PG",
            "PH",
            "PK",
            "PL",
            "PM",
            "PN",
            "PR",
            "PS",
            "PT",
            "PY",
            "QA",
            "RE",
            "RO",
            "RS",
            "RU",
            "RW",
            "SA",
            "SB",
            "SC",
            "SD",
            "SE",
            "SG",
            "SH",
            "SI",
            "SJ",
            "SK",
            "SL",
            "SM",
            "SN",
            "SO",
            "SR",
            "SS",
            "ST",
            "SV",
            "SX",
            "SZ",
            "TA",
            "TC",
            "TD",
            "TF",
            "TG",
            "TH",
            "TJ",
            "TK",
            "TL",
            "TM",
            "TN",
            "TO",
            "TR",
            "TT",
            "TV",
            "TW",
            "TZ",
            "UA",
            "UG",
            "US",
            "UY",
            "UZ",
            "VA",
            "VC",
            "VE",
            "VG",
            "VN",
            "VU",
            "WF",
            "WS",
            "XK",
            "YE",
            "YT",
            "ZA",
            "ZM",
            "ZW",
            "ZZ",
        ]
    ]
    """
    An array of two-letter ISO country codes representing which countries Checkout should provide as options for
    shipping locations.
    """


class PaymentLinkCreateParamsShippingOption(TypedDict):
    shipping_rate: NotRequired[str]
    """
    The ID of the Shipping Rate to use for this shipping option.
    """


class PaymentLinkCreateParamsSubscriptionData(TypedDict):
    description: NotRequired[str]
    """
    The subscription's description, meant to be displayable to the customer. Use this field to optionally store an explanation of the subscription for rendering in Stripe surfaces and certain local payment methods UIs.
    """
    invoice_settings: NotRequired[
        "PaymentLinkCreateParamsSubscriptionDataInvoiceSettings"
    ]
    """
    All invoices will be billed using the specified settings.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that will declaratively set metadata on [Subscriptions](https://docs.stripe.com/api/subscriptions) generated from this payment link. Unlike object-level metadata, this field is declarative. Updates will clear prior values.
    """
    trial_period_days: NotRequired[int]
    """
    Integer representing the number of trial period days before the customer is charged for the first time. Has to be at least 1.
    """
    trial_settings: NotRequired[
        "PaymentLinkCreateParamsSubscriptionDataTrialSettings"
    ]
    """
    Settings related to subscription trials.
    """


class PaymentLinkCreateParamsSubscriptionDataInvoiceSettings(TypedDict):
    issuer: NotRequired[
        "PaymentLinkCreateParamsSubscriptionDataInvoiceSettingsIssuer"
    ]
    """
    The connected account that issues the invoice. The invoice is presented with the branding and support information of the specified account.
    """


class PaymentLinkCreateParamsSubscriptionDataInvoiceSettingsIssuer(TypedDict):
    account: NotRequired[str]
    """
    The connected account being referenced when `type` is `account`.
    """
    type: Literal["account", "self"]
    """
    Type of the account referenced in the request.
    """


class PaymentLinkCreateParamsSubscriptionDataTrialSettings(TypedDict):
    end_behavior: (
        "PaymentLinkCreateParamsSubscriptionDataTrialSettingsEndBehavior"
    )
    """
    Defines how the subscription should behave when the user's free trial ends.
    """


class PaymentLinkCreateParamsSubscriptionDataTrialSettingsEndBehavior(
    TypedDict,
):
    missing_payment_method: Literal["cancel", "create_invoice", "pause"]
    """
    Indicates how the subscription should change when the trial ends if the user did not provide a payment method.
    """


class PaymentLinkCreateParamsTaxIdCollection(TypedDict):
    enabled: bool
    """
    Enable tax ID collection during checkout. Defaults to `false`.
    """
    required: NotRequired[Literal["if_supported", "never"]]
    """
    Describes whether a tax ID is required during checkout. Defaults to `never`. You can't set this parameter if `ui_mode` is `custom`.
    """


class PaymentLinkCreateParamsTransferData(TypedDict):
    amount: NotRequired[int]
    """
    The amount that will be transferred automatically when a charge succeeds.
    """
    destination: str
    """
    If specified, successful charges will be attributed to the destination
    account for tax reporting, and the funds from charges will be transferred
    to the destination account. The ID of the resulting transfer will be
    returned on the successful charge's `transfer` field.
    """
