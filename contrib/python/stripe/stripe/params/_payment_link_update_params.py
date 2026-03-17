# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class PaymentLinkUpdateParams(TypedDict):
    active: NotRequired[bool]
    """
    Whether the payment link's `url` is active. If `false`, customers visiting the URL will be shown a page saying that the link has been deactivated.
    """
    after_completion: NotRequired["PaymentLinkUpdateParamsAfterCompletion"]
    """
    Behavior after the purchase is complete.
    """
    allow_promotion_codes: NotRequired[bool]
    """
    Enables user redeemable promotion codes.
    """
    automatic_tax: NotRequired["PaymentLinkUpdateParamsAutomaticTax"]
    """
    Configuration for automatic tax collection.
    """
    billing_address_collection: NotRequired[Literal["auto", "required"]]
    """
    Configuration for collecting the customer's billing address. Defaults to `auto`.
    """
    custom_fields: NotRequired[
        "Literal['']|List[PaymentLinkUpdateParamsCustomField]"
    ]
    """
    Collect additional information from your customer using custom fields. Up to 3 fields are supported. You can't set this parameter if `ui_mode` is `custom`.
    """
    custom_text: NotRequired["PaymentLinkUpdateParamsCustomText"]
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
    inactive_message: NotRequired["Literal['']|str"]
    """
    The custom message to be displayed to a customer when a payment link is no longer active.
    """
    invoice_creation: NotRequired["PaymentLinkUpdateParamsInvoiceCreation"]
    """
    Generate a post-purchase Invoice for one-time payments.
    """
    line_items: NotRequired[List["PaymentLinkUpdateParamsLineItem"]]
    """
    The line items representing what is being sold. Each line item represents an item being sold. Up to 20 line items are supported.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`. Metadata associated with this Payment Link will automatically be copied to [checkout sessions](https://docs.stripe.com/api/checkout/sessions) created by this payment link.
    """
    name_collection: NotRequired[
        "Literal['']|PaymentLinkUpdateParamsNameCollection"
    ]
    """
    Controls settings applied for collecting the customer's name.
    """
    optional_items: NotRequired[
        "Literal['']|List[PaymentLinkUpdateParamsOptionalItem]"
    ]
    """
    A list of optional items the customer can add to their order at checkout. Use this parameter to pass one-time or recurring [Prices](https://docs.stripe.com/api/prices).
    There is a maximum of 10 optional items allowed on a payment link, and the existing limits on the number of line items allowed on a payment link apply to the combined number of line items and optional items.
    There is a maximum of 20 combined line items and optional items.
    """
    payment_intent_data: NotRequired[
        "PaymentLinkUpdateParamsPaymentIntentData"
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
        "Literal['']|List[Literal['affirm', 'afterpay_clearpay', 'alipay', 'alma', 'au_becs_debit', 'bacs_debit', 'bancontact', 'billie', 'blik', 'boleto', 'card', 'cashapp', 'eps', 'fpx', 'giropay', 'grabpay', 'ideal', 'klarna', 'konbini', 'link', 'mb_way', 'mobilepay', 'multibanco', 'oxxo', 'p24', 'pay_by_bank', 'paynow', 'paypal', 'payto', 'pix', 'promptpay', 'satispay', 'sepa_debit', 'sofort', 'swish', 'twint', 'us_bank_account', 'wechat_pay', 'zip']]"
    ]
    """
    The list of payment method types that customers can use. Pass an empty string to enable dynamic payment methods that use your [payment method settings](https://dashboard.stripe.com/settings/payment_methods).
    """
    phone_number_collection: NotRequired[
        "PaymentLinkUpdateParamsPhoneNumberCollection"
    ]
    """
    Controls phone number collection settings during checkout.

    We recommend that you review your privacy policy and check with your legal contacts.
    """
    restrictions: NotRequired[
        "Literal['']|PaymentLinkUpdateParamsRestrictions"
    ]
    """
    Settings that restrict the usage of a payment link.
    """
    shipping_address_collection: NotRequired[
        "Literal['']|PaymentLinkUpdateParamsShippingAddressCollection"
    ]
    """
    Configuration for collecting the customer's shipping address.
    """
    submit_type: NotRequired[
        Literal["auto", "book", "donate", "pay", "subscribe"]
    ]
    """
    Describes the type of transaction being performed in order to customize relevant text on the page, such as the submit button. Changing this value will also affect the hostname in the [url](https://docs.stripe.com/api/payment_links/payment_links/object#url) property (example: `donate.stripe.com`).
    """
    subscription_data: NotRequired["PaymentLinkUpdateParamsSubscriptionData"]
    """
    When creating a subscription, the specified configuration data will be used. There must be at least one line item with a recurring price to use `subscription_data`.
    """
    tax_id_collection: NotRequired["PaymentLinkUpdateParamsTaxIdCollection"]
    """
    Controls tax ID collection during checkout.
    """


class PaymentLinkUpdateParamsAfterCompletion(TypedDict):
    hosted_confirmation: NotRequired[
        "PaymentLinkUpdateParamsAfterCompletionHostedConfirmation"
    ]
    """
    Configuration when `type=hosted_confirmation`.
    """
    redirect: NotRequired["PaymentLinkUpdateParamsAfterCompletionRedirect"]
    """
    Configuration when `type=redirect`.
    """
    type: Literal["hosted_confirmation", "redirect"]
    """
    The specified behavior after the purchase is complete. Either `redirect` or `hosted_confirmation`.
    """


class PaymentLinkUpdateParamsAfterCompletionHostedConfirmation(TypedDict):
    custom_message: NotRequired[str]
    """
    A custom message to display to the customer after the purchase is complete.
    """


class PaymentLinkUpdateParamsAfterCompletionRedirect(TypedDict):
    url: str
    """
    The URL the customer will be redirected to after the purchase is complete. You can embed `{CHECKOUT_SESSION_ID}` into the URL to have the `id` of the completed [checkout session](https://docs.stripe.com/api/checkout/sessions/object#checkout_session_object-id) included.
    """


class PaymentLinkUpdateParamsAutomaticTax(TypedDict):
    enabled: bool
    """
    Set to `true` to [calculate tax automatically](https://docs.stripe.com/tax) using the customer's location.

    Enabling this parameter causes the payment link to collect any billing address information necessary for tax calculation.
    """
    liability: NotRequired["PaymentLinkUpdateParamsAutomaticTaxLiability"]
    """
    The account that's liable for tax. If set, the business address and tax registrations required to perform the tax calculation are loaded from this account. The tax transaction is returned in the report of the connected account.
    """


class PaymentLinkUpdateParamsAutomaticTaxLiability(TypedDict):
    account: NotRequired[str]
    """
    The connected account being referenced when `type` is `account`.
    """
    type: Literal["account", "self"]
    """
    Type of the account referenced in the request.
    """


class PaymentLinkUpdateParamsCustomField(TypedDict):
    dropdown: NotRequired["PaymentLinkUpdateParamsCustomFieldDropdown"]
    """
    Configuration for `type=dropdown` fields.
    """
    key: str
    """
    String of your choice that your integration can use to reconcile this field. Must be unique to this field, alphanumeric, and up to 200 characters.
    """
    label: "PaymentLinkUpdateParamsCustomFieldLabel"
    """
    The label for the field, displayed to the customer.
    """
    numeric: NotRequired["PaymentLinkUpdateParamsCustomFieldNumeric"]
    """
    Configuration for `type=numeric` fields.
    """
    optional: NotRequired[bool]
    """
    Whether the customer is required to complete the field before completing the Checkout Session. Defaults to `false`.
    """
    text: NotRequired["PaymentLinkUpdateParamsCustomFieldText"]
    """
    Configuration for `type=text` fields.
    """
    type: Literal["dropdown", "numeric", "text"]
    """
    The type of the field.
    """


class PaymentLinkUpdateParamsCustomFieldDropdown(TypedDict):
    default_value: NotRequired[str]
    """
    The value that pre-fills the field on the payment page.Must match a `value` in the `options` array.
    """
    options: List["PaymentLinkUpdateParamsCustomFieldDropdownOption"]
    """
    The options available for the customer to select. Up to 200 options allowed.
    """


class PaymentLinkUpdateParamsCustomFieldDropdownOption(TypedDict):
    label: str
    """
    The label for the option, displayed to the customer. Up to 100 characters.
    """
    value: str
    """
    The value for this option, not displayed to the customer, used by your integration to reconcile the option selected by the customer. Must be unique to this option, alphanumeric, and up to 100 characters.
    """


class PaymentLinkUpdateParamsCustomFieldLabel(TypedDict):
    custom: str
    """
    Custom text for the label, displayed to the customer. Up to 50 characters.
    """
    type: Literal["custom"]
    """
    The type of the label.
    """


class PaymentLinkUpdateParamsCustomFieldNumeric(TypedDict):
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


class PaymentLinkUpdateParamsCustomFieldText(TypedDict):
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


class PaymentLinkUpdateParamsCustomText(TypedDict):
    after_submit: NotRequired[
        "Literal['']|PaymentLinkUpdateParamsCustomTextAfterSubmit"
    ]
    """
    Custom text that should be displayed after the payment confirmation button.
    """
    shipping_address: NotRequired[
        "Literal['']|PaymentLinkUpdateParamsCustomTextShippingAddress"
    ]
    """
    Custom text that should be displayed alongside shipping address collection.
    """
    submit: NotRequired["Literal['']|PaymentLinkUpdateParamsCustomTextSubmit"]
    """
    Custom text that should be displayed alongside the payment confirmation button.
    """
    terms_of_service_acceptance: NotRequired[
        "Literal['']|PaymentLinkUpdateParamsCustomTextTermsOfServiceAcceptance"
    ]
    """
    Custom text that should be displayed in place of the default terms of service agreement text.
    """


class PaymentLinkUpdateParamsCustomTextAfterSubmit(TypedDict):
    message: str
    """
    Text can be up to 1200 characters in length.
    """


class PaymentLinkUpdateParamsCustomTextShippingAddress(TypedDict):
    message: str
    """
    Text can be up to 1200 characters in length.
    """


class PaymentLinkUpdateParamsCustomTextSubmit(TypedDict):
    message: str
    """
    Text can be up to 1200 characters in length.
    """


class PaymentLinkUpdateParamsCustomTextTermsOfServiceAcceptance(TypedDict):
    message: str
    """
    Text can be up to 1200 characters in length.
    """


class PaymentLinkUpdateParamsInvoiceCreation(TypedDict):
    enabled: bool
    """
    Whether the feature is enabled
    """
    invoice_data: NotRequired[
        "PaymentLinkUpdateParamsInvoiceCreationInvoiceData"
    ]
    """
    Invoice PDF configuration.
    """


class PaymentLinkUpdateParamsInvoiceCreationInvoiceData(TypedDict):
    account_tax_ids: NotRequired["Literal['']|List[str]"]
    """
    The account tax IDs associated with the invoice.
    """
    custom_fields: NotRequired[
        "Literal['']|List[PaymentLinkUpdateParamsInvoiceCreationInvoiceDataCustomField]"
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
        "PaymentLinkUpdateParamsInvoiceCreationInvoiceDataIssuer"
    ]
    """
    The connected account that issues the invoice. The invoice is presented with the branding and support information of the specified account.
    """
    metadata: NotRequired["Literal['']|Dict[str, str]"]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    rendering_options: NotRequired[
        "Literal['']|PaymentLinkUpdateParamsInvoiceCreationInvoiceDataRenderingOptions"
    ]
    """
    Default options for invoice PDF rendering for this customer.
    """


class PaymentLinkUpdateParamsInvoiceCreationInvoiceDataCustomField(TypedDict):
    name: str
    """
    The name of the custom field. This may be up to 40 characters.
    """
    value: str
    """
    The value of the custom field. This may be up to 140 characters.
    """


class PaymentLinkUpdateParamsInvoiceCreationInvoiceDataIssuer(TypedDict):
    account: NotRequired[str]
    """
    The connected account being referenced when `type` is `account`.
    """
    type: Literal["account", "self"]
    """
    Type of the account referenced in the request.
    """


class PaymentLinkUpdateParamsInvoiceCreationInvoiceDataRenderingOptions(
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


class PaymentLinkUpdateParamsLineItem(TypedDict):
    adjustable_quantity: NotRequired[
        "PaymentLinkUpdateParamsLineItemAdjustableQuantity"
    ]
    """
    When set, provides configuration for this item's quantity to be adjusted by the customer during checkout.
    """
    id: str
    """
    The ID of an existing line item on the payment link.
    """
    quantity: NotRequired[int]
    """
    The quantity of the line item being purchased.
    """


class PaymentLinkUpdateParamsLineItemAdjustableQuantity(TypedDict):
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


class PaymentLinkUpdateParamsNameCollection(TypedDict):
    business: NotRequired["PaymentLinkUpdateParamsNameCollectionBusiness"]
    """
    Controls settings applied for collecting the customer's business name.
    """
    individual: NotRequired["PaymentLinkUpdateParamsNameCollectionIndividual"]
    """
    Controls settings applied for collecting the customer's individual name.
    """


class PaymentLinkUpdateParamsNameCollectionBusiness(TypedDict):
    enabled: bool
    """
    Enable business name collection on the payment link. Defaults to `false`.
    """
    optional: NotRequired[bool]
    """
    Whether the customer is required to provide their business name before checking out. Defaults to `false`.
    """


class PaymentLinkUpdateParamsNameCollectionIndividual(TypedDict):
    enabled: bool
    """
    Enable individual name collection on the payment link. Defaults to `false`.
    """
    optional: NotRequired[bool]
    """
    Whether the customer is required to provide their full name before checking out. Defaults to `false`.
    """


class PaymentLinkUpdateParamsOptionalItem(TypedDict):
    adjustable_quantity: NotRequired[
        "PaymentLinkUpdateParamsOptionalItemAdjustableQuantity"
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


class PaymentLinkUpdateParamsOptionalItemAdjustableQuantity(TypedDict):
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


class PaymentLinkUpdateParamsPaymentIntentData(TypedDict):
    description: NotRequired["Literal['']|str"]
    """
    An arbitrary string attached to the object. Often useful for displaying to users.
    """
    metadata: NotRequired["Literal['']|Dict[str, str]"]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that will declaratively set metadata on [Payment Intents](https://docs.stripe.com/api/payment_intents) generated from this payment link. Unlike object-level metadata, this field is declarative. Updates will clear prior values.
    """
    statement_descriptor: NotRequired["Literal['']|str"]
    """
    Text that appears on the customer's statement as the statement descriptor for a non-card charge. This value overrides the account's default statement descriptor. For information about requirements, including the 22-character limit, see [the Statement Descriptor docs](https://docs.stripe.com/get-started/account/statement-descriptors).

    Setting this value for a card charge returns an error. For card charges, set the [statement_descriptor_suffix](https://docs.stripe.com/get-started/account/statement-descriptors#dynamic) instead.
    """
    statement_descriptor_suffix: NotRequired["Literal['']|str"]
    """
    Provides information about a card charge. Concatenated to the account's [statement descriptor prefix](https://docs.stripe.com/get-started/account/statement-descriptors#static) to form the complete statement descriptor that appears on the customer's statement.
    """
    transfer_group: NotRequired["Literal['']|str"]
    """
    A string that identifies the resulting payment as part of a group. See the PaymentIntents [use case for connected accounts](https://docs.stripe.com/connect/separate-charges-and-transfers) for details.
    """


class PaymentLinkUpdateParamsPhoneNumberCollection(TypedDict):
    enabled: bool
    """
    Set to `true` to enable phone number collection.
    """


class PaymentLinkUpdateParamsRestrictions(TypedDict):
    completed_sessions: "PaymentLinkUpdateParamsRestrictionsCompletedSessions"
    """
    Configuration for the `completed_sessions` restriction type.
    """


class PaymentLinkUpdateParamsRestrictionsCompletedSessions(TypedDict):
    limit: int
    """
    The maximum number of checkout sessions that can be completed for the `completed_sessions` restriction to be met.
    """


class PaymentLinkUpdateParamsShippingAddressCollection(TypedDict):
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


class PaymentLinkUpdateParamsSubscriptionData(TypedDict):
    invoice_settings: NotRequired[
        "PaymentLinkUpdateParamsSubscriptionDataInvoiceSettings"
    ]
    """
    All invoices will be billed using the specified settings.
    """
    metadata: NotRequired["Literal['']|Dict[str, str]"]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that will declaratively set metadata on [Subscriptions](https://docs.stripe.com/api/subscriptions) generated from this payment link. Unlike object-level metadata, this field is declarative. Updates will clear prior values.
    """
    trial_period_days: NotRequired["Literal['']|int"]
    """
    Integer representing the number of trial period days before the customer is charged for the first time. Has to be at least 1.
    """
    trial_settings: NotRequired[
        "Literal['']|PaymentLinkUpdateParamsSubscriptionDataTrialSettings"
    ]
    """
    Settings related to subscription trials.
    """


class PaymentLinkUpdateParamsSubscriptionDataInvoiceSettings(TypedDict):
    issuer: NotRequired[
        "PaymentLinkUpdateParamsSubscriptionDataInvoiceSettingsIssuer"
    ]
    """
    The connected account that issues the invoice. The invoice is presented with the branding and support information of the specified account.
    """


class PaymentLinkUpdateParamsSubscriptionDataInvoiceSettingsIssuer(TypedDict):
    account: NotRequired[str]
    """
    The connected account being referenced when `type` is `account`.
    """
    type: Literal["account", "self"]
    """
    Type of the account referenced in the request.
    """


class PaymentLinkUpdateParamsSubscriptionDataTrialSettings(TypedDict):
    end_behavior: (
        "PaymentLinkUpdateParamsSubscriptionDataTrialSettingsEndBehavior"
    )
    """
    Defines how the subscription should behave when the user's free trial ends.
    """


class PaymentLinkUpdateParamsSubscriptionDataTrialSettingsEndBehavior(
    TypedDict,
):
    missing_payment_method: Literal["cancel", "create_invoice", "pause"]
    """
    Indicates how the subscription should change when the trial ends if the user did not provide a payment method.
    """


class PaymentLinkUpdateParamsTaxIdCollection(TypedDict):
    enabled: bool
    """
    Enable tax ID collection during checkout. Defaults to `false`.
    """
    required: NotRequired[Literal["if_supported", "never"]]
    """
    Describes whether a tax ID is required during checkout. Defaults to `never`. You can't set this parameter if `ui_mode` is `custom`.
    """
