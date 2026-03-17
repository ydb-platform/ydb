# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class SessionCreateParams(RequestOptions):
    adaptive_pricing: NotRequired["SessionCreateParamsAdaptivePricing"]
    """
    Settings for price localization with [Adaptive Pricing](https://docs.stripe.com/payments/checkout/adaptive-pricing).
    """
    after_expiration: NotRequired["SessionCreateParamsAfterExpiration"]
    """
    Configure actions after a Checkout Session has expired. You can't set this parameter if `ui_mode` is `custom`.
    """
    allow_promotion_codes: NotRequired[bool]
    """
    Enables user redeemable promotion codes.
    """
    automatic_tax: NotRequired["SessionCreateParamsAutomaticTax"]
    """
    Settings for automatic tax lookup for this session and resulting payments, invoices, and subscriptions.
    """
    billing_address_collection: NotRequired[Literal["auto", "required"]]
    """
    Specify whether Checkout should collect the customer's billing address. Defaults to `auto`.
    """
    branding_settings: NotRequired["SessionCreateParamsBrandingSettings"]
    """
    The branding settings for the Checkout Session. This parameter is not allowed if ui_mode is `custom`.
    """
    cancel_url: NotRequired[str]
    """
    If set, Checkout displays a back button and customers will be directed to this URL if they decide to cancel payment and return to your website. This parameter is not allowed if ui_mode is `embedded` or `custom`.
    """
    client_reference_id: NotRequired[str]
    """
    A unique string to reference the Checkout Session. This can be a
    customer ID, a cart ID, or similar, and can be used to reconcile the
    session with your internal systems.
    """
    consent_collection: NotRequired["SessionCreateParamsConsentCollection"]
    """
    Configure fields for the Checkout Session to gather active consent from customers.
    """
    currency: NotRequired[str]
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies). Required in `setup` mode when `payment_method_types` is not set.
    """
    custom_fields: NotRequired[List["SessionCreateParamsCustomField"]]
    """
    Collect additional information from your customer using custom fields. Up to 3 fields are supported. You can't set this parameter if `ui_mode` is `custom`.
    """
    custom_text: NotRequired["SessionCreateParamsCustomText"]
    """
    Display additional text for your customers using custom text. You can't set this parameter if `ui_mode` is `custom`.
    """
    customer: NotRequired[str]
    """
    ID of an existing Customer, if one exists. In `payment` mode, the customer's most recently saved card
    payment method will be used to prefill the email, name, card details, and billing address
    on the Checkout page. In `subscription` mode, the customer's [default payment method](https://docs.stripe.com/api/customers/update#update_customer-invoice_settings-default_payment_method)
    will be used if it's a card, otherwise the most recently saved card will be used. A valid billing address, billing name and billing email are required on the payment method for Checkout to prefill the customer's card details.

    If the Customer already has a valid [email](https://docs.stripe.com/api/customers/object#customer_object-email) set, the email will be prefilled and not editable in Checkout.
    If the Customer does not have a valid `email`, Checkout will set the email entered during the session on the Customer.

    If blank for Checkout Sessions in `subscription` mode or with `customer_creation` set as `always` in `payment` mode, Checkout will create a new Customer object based on information provided during the payment flow.

    You can set [`payment_intent_data.setup_future_usage`](https://docs.stripe.com/api/checkout/sessions/create#create_checkout_session-payment_intent_data-setup_future_usage) to have Checkout automatically attach the payment method to the Customer you pass in for future reuse.
    """
    customer_account: NotRequired[str]
    """
    ID of an existing Account, if one exists. Has the same behavior as `customer`.
    """
    customer_creation: NotRequired[Literal["always", "if_required"]]
    """
    Configure whether a Checkout Session creates a [Customer](https://docs.stripe.com/api/customers) during Session confirmation.

    When a Customer is not created, you can still retrieve email, address, and other customer data entered in Checkout
    with [customer_details](https://docs.stripe.com/api/checkout/sessions/object#checkout_session_object-customer_details).

    Sessions that don't create Customers instead are grouped by [guest customers](https://docs.stripe.com/payments/checkout/guest-customers)
    in the Dashboard. Promotion codes limited to first time customers will return invalid for these Sessions.

    Can only be set in `payment` and `setup` mode.
    """
    customer_email: NotRequired[str]
    """
    If provided, this value will be used when the Customer object is created.
    If not provided, customers will be asked to enter their email address.
    Use this parameter to prefill customer data if you already have an email
    on file. To access information about the customer once a session is
    complete, use the `customer` field.
    """
    customer_update: NotRequired["SessionCreateParamsCustomerUpdate"]
    """
    Controls what fields on Customer can be updated by the Checkout Session. Can only be provided when `customer` is provided.
    """
    discounts: NotRequired[List["SessionCreateParamsDiscount"]]
    """
    The coupon or promotion code to apply to this Session. Currently, only up to one may be specified.
    """
    excluded_payment_method_types: NotRequired[
        List[
            Literal[
                "acss_debit",
                "affirm",
                "afterpay_clearpay",
                "alipay",
                "alma",
                "amazon_pay",
                "au_becs_debit",
                "bacs_debit",
                "bancontact",
                "billie",
                "blik",
                "boleto",
                "card",
                "cashapp",
                "crypto",
                "customer_balance",
                "eps",
                "fpx",
                "giropay",
                "grabpay",
                "ideal",
                "kakao_pay",
                "klarna",
                "konbini",
                "kr_card",
                "mb_way",
                "mobilepay",
                "multibanco",
                "naver_pay",
                "nz_bank_account",
                "oxxo",
                "p24",
                "pay_by_bank",
                "payco",
                "paynow",
                "paypal",
                "payto",
                "pix",
                "promptpay",
                "revolut_pay",
                "samsung_pay",
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
    A list of the types of payment methods (e.g., `card`) that should be excluded from this Checkout Session. This should only be used when payment methods for this Checkout Session are managed through the [Stripe Dashboard](https://dashboard.stripe.com/settings/payment_methods).
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    expires_at: NotRequired[int]
    """
    The Epoch time in seconds at which the Checkout Session will expire. It can be anywhere from 30 minutes to 24 hours after Checkout Session creation. By default, this value is 24 hours from creation.
    """
    invoice_creation: NotRequired["SessionCreateParamsInvoiceCreation"]
    """
    Generate a post-purchase Invoice for one-time payments.
    """
    line_items: NotRequired[List["SessionCreateParamsLineItem"]]
    """
    A list of items the customer is purchasing. Use this parameter to pass one-time or recurring [Prices](https://docs.stripe.com/api/prices). The parameter is required for `payment` and `subscription` mode.

    For `payment` mode, there is a maximum of 100 line items, however it is recommended to consolidate line items if there are more than a few dozen.

    For `subscription` mode, there is a maximum of 20 line items with recurring Prices and 20 line items with one-time Prices. Line items with one-time Prices will be on the initial invoice only.
    """
    locale: NotRequired[
        Literal[
            "auto",
            "bg",
            "cs",
            "da",
            "de",
            "el",
            "en",
            "en-GB",
            "es",
            "es-419",
            "et",
            "fi",
            "fil",
            "fr",
            "fr-CA",
            "hr",
            "hu",
            "id",
            "it",
            "ja",
            "ko",
            "lt",
            "lv",
            "ms",
            "mt",
            "nb",
            "nl",
            "pl",
            "pt",
            "pt-BR",
            "ro",
            "ru",
            "sk",
            "sl",
            "sv",
            "th",
            "tr",
            "vi",
            "zh",
            "zh-HK",
            "zh-TW",
        ]
    ]
    """
    The IETF language tag of the locale Checkout is displayed in. If blank or `auto`, the browser's locale is used.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    mode: NotRequired[Literal["payment", "setup", "subscription"]]
    """
    The mode of the Checkout Session. Pass `subscription` if the Checkout Session includes at least one recurring item.
    """
    name_collection: NotRequired["SessionCreateParamsNameCollection"]
    """
    Controls name collection settings for the session.

    You can configure Checkout to collect your customers' business names, individual names, or both. Each name field can be either required or optional.

    If a [Customer](https://docs.stripe.com/api/customers) is created or provided, the names can be saved to the Customer object as well.

    You can't set this parameter if `ui_mode` is `custom`.
    """
    optional_items: NotRequired[List["SessionCreateParamsOptionalItem"]]
    """
    A list of optional items the customer can add to their order at checkout. Use this parameter to pass one-time or recurring [Prices](https://docs.stripe.com/api/prices).

    There is a maximum of 10 optional items allowed on a Checkout Session, and the existing limits on the number of line items allowed on a Checkout Session apply to the combined number of line items and optional items.

    For `payment` mode, there is a maximum of 100 combined line items and optional items, however it is recommended to consolidate items if there are more than a few dozen.

    For `subscription` mode, there is a maximum of 20 line items and optional items with recurring Prices and 20 line items and optional items with one-time Prices.

    You can't set this parameter if `ui_mode` is `custom`.
    """
    origin_context: NotRequired[Literal["mobile_app", "web"]]
    """
    Where the user is coming from. This informs the optimizations that are applied to the session. You can't set this parameter if `ui_mode` is `custom`.
    """
    payment_intent_data: NotRequired["SessionCreateParamsPaymentIntentData"]
    """
    A subset of parameters to be passed to PaymentIntent creation for Checkout Sessions in `payment` mode.
    """
    payment_method_collection: NotRequired[Literal["always", "if_required"]]
    """
    Specify whether Checkout should collect a payment method. When set to `if_required`, Checkout will not collect a payment method when the total due for the session is 0.
    This may occur if the Checkout Session includes a free trial or a discount.

    Can only be set in `subscription` mode. Defaults to `always`.

    If you'd like information on how to collect a payment method outside of Checkout, read the guide on configuring [subscriptions with a free trial](https://docs.stripe.com/payments/checkout/free-trials).
    """
    payment_method_configuration: NotRequired[str]
    """
    The ID of the payment method configuration to use with this Checkout session.
    """
    payment_method_data: NotRequired["SessionCreateParamsPaymentMethodData"]
    """
    This parameter allows you to set some attributes on the payment method created during a Checkout session.
    """
    payment_method_options: NotRequired[
        "SessionCreateParamsPaymentMethodOptions"
    ]
    """
    Payment-method-specific configuration.
    """
    payment_method_types: NotRequired[
        List[
            Literal[
                "acss_debit",
                "affirm",
                "afterpay_clearpay",
                "alipay",
                "alma",
                "amazon_pay",
                "au_becs_debit",
                "bacs_debit",
                "bancontact",
                "billie",
                "blik",
                "boleto",
                "card",
                "cashapp",
                "crypto",
                "customer_balance",
                "eps",
                "fpx",
                "giropay",
                "grabpay",
                "ideal",
                "kakao_pay",
                "klarna",
                "konbini",
                "kr_card",
                "link",
                "mb_way",
                "mobilepay",
                "multibanco",
                "naver_pay",
                "nz_bank_account",
                "oxxo",
                "p24",
                "pay_by_bank",
                "payco",
                "paynow",
                "paypal",
                "payto",
                "pix",
                "promptpay",
                "revolut_pay",
                "samsung_pay",
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
    A list of the types of payment methods (e.g., `card`) this Checkout Session can accept.

    You can omit this attribute to manage your payment methods from the [Stripe Dashboard](https://dashboard.stripe.com/settings/payment_methods).
    See [Dynamic Payment Methods](https://docs.stripe.com/payments/payment-methods/integration-options#using-dynamic-payment-methods) for more details.

    Read more about the supported payment methods and their requirements in our [payment
    method details guide](https://docs.stripe.com/docs/payments/checkout/payment-methods).

    If multiple payment methods are passed, Checkout will dynamically reorder them to
    prioritize the most relevant payment methods based on the customer's location and
    other characteristics.
    """
    permissions: NotRequired["SessionCreateParamsPermissions"]
    """
    This property is used to set up permissions for various actions (e.g., update) on the CheckoutSession object. Can only be set when creating `embedded` or `custom` sessions.

    For specific permissions, please refer to their dedicated subsections, such as `permissions.update_shipping_details`.
    """
    phone_number_collection: NotRequired[
        "SessionCreateParamsPhoneNumberCollection"
    ]
    """
    Controls phone number collection settings for the session.

    We recommend that you review your privacy policy and check with your legal contacts
    before using this feature. Learn more about [collecting phone numbers with Checkout](https://docs.stripe.com/payments/checkout/phone-numbers).
    """
    redirect_on_completion: NotRequired[
        Literal["always", "if_required", "never"]
    ]
    """
    This parameter applies to `ui_mode: embedded`. Learn more about the [redirect behavior](https://docs.stripe.com/payments/checkout/custom-success-page?payment-ui=embedded-form) of embedded sessions. Defaults to `always`.
    """
    return_url: NotRequired[str]
    """
    The URL to redirect your customer back to after they authenticate or cancel their payment on the
    payment method's app or site. This parameter is required if `ui_mode` is `embedded` or `custom`
    and redirect-based payment methods are enabled on the session.
    """
    saved_payment_method_options: NotRequired[
        "SessionCreateParamsSavedPaymentMethodOptions"
    ]
    """
    Controls saved payment method settings for the session. Only available in `payment` and `subscription` mode.
    """
    setup_intent_data: NotRequired["SessionCreateParamsSetupIntentData"]
    """
    A subset of parameters to be passed to SetupIntent creation for Checkout Sessions in `setup` mode.
    """
    shipping_address_collection: NotRequired[
        "SessionCreateParamsShippingAddressCollection"
    ]
    """
    When set, provides configuration for Checkout to collect a shipping address from a customer.
    """
    shipping_options: NotRequired[List["SessionCreateParamsShippingOption"]]
    """
    The shipping rate options to apply to this Session. Up to a maximum of 5.
    """
    submit_type: NotRequired[
        Literal["auto", "book", "donate", "pay", "subscribe"]
    ]
    """
    Describes the type of transaction being performed by Checkout in order
    to customize relevant text on the page, such as the submit button.
     `submit_type` can only be specified on Checkout Sessions in
    `payment` or `subscription` mode. If blank or `auto`, `pay` is used.
    You can't set this parameter if `ui_mode` is `custom`.
    """
    subscription_data: NotRequired["SessionCreateParamsSubscriptionData"]
    """
    A subset of parameters to be passed to subscription creation for Checkout Sessions in `subscription` mode.
    """
    success_url: NotRequired[str]
    """
    The URL to which Stripe should send customers when payment or setup
    is complete.
    This parameter is not allowed if ui_mode is `embedded` or `custom`. If you'd like to use
    information from the successful Checkout Session on your page, read the
    guide on [customizing your success page](https://docs.stripe.com/payments/checkout/custom-success-page).
    """
    tax_id_collection: NotRequired["SessionCreateParamsTaxIdCollection"]
    """
    Controls tax ID collection during checkout.
    """
    ui_mode: NotRequired[Literal["custom", "embedded", "hosted"]]
    """
    The UI mode of the Session. Defaults to `hosted`.
    """
    wallet_options: NotRequired["SessionCreateParamsWalletOptions"]
    """
    Wallet-specific configuration.
    """


class SessionCreateParamsAdaptivePricing(TypedDict):
    enabled: NotRequired[bool]
    """
    If set to `true`, Adaptive Pricing is available on [eligible sessions](https://docs.stripe.com/payments/currencies/localize-prices/adaptive-pricing?payment-ui=stripe-hosted#restrictions). Defaults to your [dashboard setting](https://dashboard.stripe.com/settings/adaptive-pricing).
    """


class SessionCreateParamsAfterExpiration(TypedDict):
    recovery: NotRequired["SessionCreateParamsAfterExpirationRecovery"]
    """
    Configure a Checkout Session that can be used to recover an expired session.
    """


class SessionCreateParamsAfterExpirationRecovery(TypedDict):
    allow_promotion_codes: NotRequired[bool]
    """
    Enables user redeemable promotion codes on the recovered Checkout Sessions. Defaults to `false`
    """
    enabled: bool
    """
    If `true`, a recovery URL will be generated to recover this Checkout Session if it
    expires before a successful transaction is completed. It will be attached to the
    Checkout Session object upon expiration.
    """


class SessionCreateParamsAutomaticTax(TypedDict):
    enabled: bool
    """
    Set to `true` to [calculate tax automatically](https://docs.stripe.com/tax) using the customer's location.

    Enabling this parameter causes Checkout to collect any billing address information necessary for tax calculation.
    """
    liability: NotRequired["SessionCreateParamsAutomaticTaxLiability"]
    """
    The account that's liable for tax. If set, the business address and tax registrations required to perform the tax calculation are loaded from this account. The tax transaction is returned in the report of the connected account.
    """


class SessionCreateParamsAutomaticTaxLiability(TypedDict):
    account: NotRequired[str]
    """
    The connected account being referenced when `type` is `account`.
    """
    type: Literal["account", "self"]
    """
    Type of the account referenced in the request.
    """


class SessionCreateParamsBrandingSettings(TypedDict):
    background_color: NotRequired["Literal['']|str"]
    """
    A hex color value starting with `#` representing the background color for the Checkout Session.
    """
    border_style: NotRequired[
        "Literal['']|Literal['pill', 'rectangular', 'rounded']"
    ]
    """
    The border style for the Checkout Session.
    """
    button_color: NotRequired["Literal['']|str"]
    """
    A hex color value starting with `#` representing the button color for the Checkout Session.
    """
    display_name: NotRequired[str]
    """
    A string to override the business name shown on the Checkout Session. This only shows at the top of the Checkout page, and your business name still appears in terms, receipts, and other places.
    """
    font_family: NotRequired[
        "Literal['']|Literal['be_vietnam_pro', 'bitter', 'chakra_petch', 'default', 'hahmlet', 'inconsolata', 'inter', 'lato', 'lora', 'm_plus_1_code', 'montserrat', 'noto_sans', 'noto_sans_jp', 'noto_serif', 'nunito', 'open_sans', 'pridi', 'pt_sans', 'pt_serif', 'raleway', 'roboto', 'roboto_slab', 'source_sans_pro', 'titillium_web', 'ubuntu_mono', 'zen_maru_gothic']"
    ]
    """
    The font family for the Checkout Session corresponding to one of the [supported font families](https://docs.stripe.com/payments/checkout/customization/appearance?payment-ui=stripe-hosted#font-compatibility).
    """
    icon: NotRequired["SessionCreateParamsBrandingSettingsIcon"]
    """
    The icon for the Checkout Session. For best results, use a square image.
    """
    logo: NotRequired["SessionCreateParamsBrandingSettingsLogo"]
    """
    The logo for the Checkout Session.
    """


class SessionCreateParamsBrandingSettingsIcon(TypedDict):
    file: NotRequired[str]
    """
    The ID of a [File upload](https://stripe.com/docs/api/files) representing the icon. Purpose must be `business_icon`. Required if `type` is `file` and disallowed otherwise.
    """
    type: Literal["file", "url"]
    """
    The type of image for the icon. Must be one of `file` or `url`.
    """
    url: NotRequired[str]
    """
    The URL of the image. Required if `type` is `url` and disallowed otherwise.
    """


class SessionCreateParamsBrandingSettingsLogo(TypedDict):
    file: NotRequired[str]
    """
    The ID of a [File upload](https://stripe.com/docs/api/files) representing the logo. Purpose must be `business_logo`. Required if `type` is `file` and disallowed otherwise.
    """
    type: Literal["file", "url"]
    """
    The type of image for the logo. Must be one of `file` or `url`.
    """
    url: NotRequired[str]
    """
    The URL of the image. Required if `type` is `url` and disallowed otherwise.
    """


class SessionCreateParamsConsentCollection(TypedDict):
    payment_method_reuse_agreement: NotRequired[
        "SessionCreateParamsConsentCollectionPaymentMethodReuseAgreement"
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


class SessionCreateParamsConsentCollectionPaymentMethodReuseAgreement(
    TypedDict,
):
    position: Literal["auto", "hidden"]
    """
    Determines the position and visibility of the payment method reuse agreement in the UI. When set to `auto`, Stripe's
    defaults will be used. When set to `hidden`, the payment method reuse agreement text will always be hidden in the UI.
    """


class SessionCreateParamsCustomField(TypedDict):
    dropdown: NotRequired["SessionCreateParamsCustomFieldDropdown"]
    """
    Configuration for `type=dropdown` fields.
    """
    key: str
    """
    String of your choice that your integration can use to reconcile this field. Must be unique to this field, alphanumeric, and up to 200 characters.
    """
    label: "SessionCreateParamsCustomFieldLabel"
    """
    The label for the field, displayed to the customer.
    """
    numeric: NotRequired["SessionCreateParamsCustomFieldNumeric"]
    """
    Configuration for `type=numeric` fields.
    """
    optional: NotRequired[bool]
    """
    Whether the customer is required to complete the field before completing the Checkout Session. Defaults to `false`.
    """
    text: NotRequired["SessionCreateParamsCustomFieldText"]
    """
    Configuration for `type=text` fields.
    """
    type: Literal["dropdown", "numeric", "text"]
    """
    The type of the field.
    """


class SessionCreateParamsCustomFieldDropdown(TypedDict):
    default_value: NotRequired[str]
    """
    The value that pre-fills the field on the payment page.Must match a `value` in the `options` array.
    """
    options: List["SessionCreateParamsCustomFieldDropdownOption"]
    """
    The options available for the customer to select. Up to 200 options allowed.
    """


class SessionCreateParamsCustomFieldDropdownOption(TypedDict):
    label: str
    """
    The label for the option, displayed to the customer. Up to 100 characters.
    """
    value: str
    """
    The value for this option, not displayed to the customer, used by your integration to reconcile the option selected by the customer. Must be unique to this option, alphanumeric, and up to 100 characters.
    """


class SessionCreateParamsCustomFieldLabel(TypedDict):
    custom: str
    """
    Custom text for the label, displayed to the customer. Up to 50 characters.
    """
    type: Literal["custom"]
    """
    The type of the label.
    """


class SessionCreateParamsCustomFieldNumeric(TypedDict):
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


class SessionCreateParamsCustomFieldText(TypedDict):
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


class SessionCreateParamsCustomText(TypedDict):
    after_submit: NotRequired[
        "Literal['']|SessionCreateParamsCustomTextAfterSubmit"
    ]
    """
    Custom text that should be displayed after the payment confirmation button.
    """
    shipping_address: NotRequired[
        "Literal['']|SessionCreateParamsCustomTextShippingAddress"
    ]
    """
    Custom text that should be displayed alongside shipping address collection.
    """
    submit: NotRequired["Literal['']|SessionCreateParamsCustomTextSubmit"]
    """
    Custom text that should be displayed alongside the payment confirmation button.
    """
    terms_of_service_acceptance: NotRequired[
        "Literal['']|SessionCreateParamsCustomTextTermsOfServiceAcceptance"
    ]
    """
    Custom text that should be displayed in place of the default terms of service agreement text.
    """


class SessionCreateParamsCustomTextAfterSubmit(TypedDict):
    message: str
    """
    Text can be up to 1200 characters in length.
    """


class SessionCreateParamsCustomTextShippingAddress(TypedDict):
    message: str
    """
    Text can be up to 1200 characters in length.
    """


class SessionCreateParamsCustomTextSubmit(TypedDict):
    message: str
    """
    Text can be up to 1200 characters in length.
    """


class SessionCreateParamsCustomTextTermsOfServiceAcceptance(TypedDict):
    message: str
    """
    Text can be up to 1200 characters in length.
    """


class SessionCreateParamsCustomerUpdate(TypedDict):
    address: NotRequired[Literal["auto", "never"]]
    """
    Describes whether Checkout saves the billing address onto `customer.address`.
    To always collect a full billing address, use `billing_address_collection`. Defaults to `never`.
    """
    name: NotRequired[Literal["auto", "never"]]
    """
    Describes whether Checkout saves the name onto `customer.name`. Defaults to `never`.
    """
    shipping: NotRequired[Literal["auto", "never"]]
    """
    Describes whether Checkout saves shipping information onto `customer.shipping`.
    To collect shipping information, use `shipping_address_collection`. Defaults to `never`.
    """


class SessionCreateParamsDiscount(TypedDict):
    coupon: NotRequired[str]
    """
    The ID of the coupon to apply to this Session.
    """
    promotion_code: NotRequired[str]
    """
    The ID of a promotion code to apply to this Session.
    """


class SessionCreateParamsInvoiceCreation(TypedDict):
    enabled: bool
    """
    Set to `true` to enable invoice creation.
    """
    invoice_data: NotRequired["SessionCreateParamsInvoiceCreationInvoiceData"]
    """
    Parameters passed when creating invoices for payment-mode Checkout Sessions.
    """


class SessionCreateParamsInvoiceCreationInvoiceData(TypedDict):
    account_tax_ids: NotRequired["Literal['']|List[str]"]
    """
    The account tax IDs associated with the invoice.
    """
    custom_fields: NotRequired[
        "Literal['']|List[SessionCreateParamsInvoiceCreationInvoiceDataCustomField]"
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
    issuer: NotRequired["SessionCreateParamsInvoiceCreationInvoiceDataIssuer"]
    """
    The connected account that issues the invoice. The invoice is presented with the branding and support information of the specified account.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    rendering_options: NotRequired[
        "Literal['']|SessionCreateParamsInvoiceCreationInvoiceDataRenderingOptions"
    ]
    """
    Default options for invoice PDF rendering for this customer.
    """


class SessionCreateParamsInvoiceCreationInvoiceDataCustomField(TypedDict):
    name: str
    """
    The name of the custom field. This may be up to 40 characters.
    """
    value: str
    """
    The value of the custom field. This may be up to 140 characters.
    """


class SessionCreateParamsInvoiceCreationInvoiceDataIssuer(TypedDict):
    account: NotRequired[str]
    """
    The connected account being referenced when `type` is `account`.
    """
    type: Literal["account", "self"]
    """
    Type of the account referenced in the request.
    """


class SessionCreateParamsInvoiceCreationInvoiceDataRenderingOptions(TypedDict):
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


class SessionCreateParamsLineItem(TypedDict):
    adjustable_quantity: NotRequired[
        "SessionCreateParamsLineItemAdjustableQuantity"
    ]
    """
    When set, provides configuration for this item's quantity to be adjusted by the customer during Checkout.
    """
    dynamic_tax_rates: NotRequired[List[str]]
    """
    The [tax rates](https://docs.stripe.com/api/tax_rates) that will be applied to this line item depending on the customer's billing/shipping address. We currently support the following countries: US, GB, AU, and all countries in the EU. You can't set this parameter if `ui_mode` is `custom`.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    price: NotRequired[str]
    """
    The ID of the [Price](https://docs.stripe.com/api/prices) or [Plan](https://docs.stripe.com/api/plans) object. One of `price` or `price_data` is required.
    """
    price_data: NotRequired["SessionCreateParamsLineItemPriceData"]
    """
    Data used to generate a new [Price](https://docs.stripe.com/api/prices) object inline. One of `price` or `price_data` is required.
    """
    quantity: NotRequired[int]
    """
    The quantity of the line item being purchased. Quantity should not be defined when `recurring.usage_type=metered`.
    """
    tax_rates: NotRequired[List[str]]
    """
    The [tax rates](https://docs.stripe.com/api/tax_rates) which apply to this line item.
    """


class SessionCreateParamsLineItemAdjustableQuantity(TypedDict):
    enabled: bool
    """
    Set to true if the quantity can be adjusted to any non-negative integer.
    """
    maximum: NotRequired[int]
    """
    The maximum quantity the customer can purchase for the Checkout Session. By default this value is 99. You can specify a value up to 999999.
    """
    minimum: NotRequired[int]
    """
    The minimum quantity the customer must purchase for the Checkout Session. By default this value is 0.
    """


class SessionCreateParamsLineItemPriceData(TypedDict):
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    product: NotRequired[str]
    """
    The ID of the [Product](https://docs.stripe.com/api/products) that this [Price](https://docs.stripe.com/api/prices) will belong to. One of `product` or `product_data` is required.
    """
    product_data: NotRequired[
        "SessionCreateParamsLineItemPriceDataProductData"
    ]
    """
    Data used to generate a new [Product](https://docs.stripe.com/api/products) object inline. One of `product` or `product_data` is required.
    """
    recurring: NotRequired["SessionCreateParamsLineItemPriceDataRecurring"]
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


class SessionCreateParamsLineItemPriceDataProductData(TypedDict):
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


class SessionCreateParamsLineItemPriceDataRecurring(TypedDict):
    interval: Literal["day", "month", "week", "year"]
    """
    Specifies billing frequency. Either `day`, `week`, `month` or `year`.
    """
    interval_count: NotRequired[int]
    """
    The number of intervals between subscription billings. For example, `interval=month` and `interval_count=3` bills every 3 months. Maximum of three years interval allowed (3 years, 36 months, or 156 weeks).
    """


class SessionCreateParamsNameCollection(TypedDict):
    business: NotRequired["SessionCreateParamsNameCollectionBusiness"]
    """
    Controls settings applied for collecting the customer's business name on the session.
    """
    individual: NotRequired["SessionCreateParamsNameCollectionIndividual"]
    """
    Controls settings applied for collecting the customer's individual name on the session.
    """


class SessionCreateParamsNameCollectionBusiness(TypedDict):
    enabled: bool
    """
    Enable business name collection on the Checkout Session. Defaults to `false`.
    """
    optional: NotRequired[bool]
    """
    Whether the customer is required to provide a business name before completing the Checkout Session. Defaults to `false`.
    """


class SessionCreateParamsNameCollectionIndividual(TypedDict):
    enabled: bool
    """
    Enable individual name collection on the Checkout Session. Defaults to `false`.
    """
    optional: NotRequired[bool]
    """
    Whether the customer is required to provide their name before completing the Checkout Session. Defaults to `false`.
    """


class SessionCreateParamsOptionalItem(TypedDict):
    adjustable_quantity: NotRequired[
        "SessionCreateParamsOptionalItemAdjustableQuantity"
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


class SessionCreateParamsOptionalItemAdjustableQuantity(TypedDict):
    enabled: bool
    """
    Set to true if the quantity can be adjusted to any non-negative integer.
    """
    maximum: NotRequired[int]
    """
    The maximum quantity of this item the customer can purchase. By default this value is 99. You can specify a value up to 999999.
    """
    minimum: NotRequired[int]
    """
    The minimum quantity of this item the customer must purchase, if they choose to purchase it. Because this item is optional, the customer will always be able to remove it from their order, even if the `minimum` configured here is greater than 0. By default this value is 0.
    """


class SessionCreateParamsPaymentIntentData(TypedDict):
    application_fee_amount: NotRequired[int]
    """
    The amount of the application fee (if any) that will be requested to be applied to the payment and transferred to the application owner's Stripe account. The amount of the application fee collected will be capped at the total amount captured. For more information, see the PaymentIntents [use case for connected accounts](https://docs.stripe.com/payments/connected-accounts).
    """
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
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    on_behalf_of: NotRequired[str]
    """
    The Stripe account ID for which these funds are intended. For details,
    see the PaymentIntents [use case for connected
    accounts](https://docs.stripe.com/docs/payments/connected-accounts).
    """
    receipt_email: NotRequired[str]
    """
    Email address that the receipt for the resulting payment will be sent to. If `receipt_email` is specified for a payment in live mode, a receipt will be sent regardless of your [email settings](https://dashboard.stripe.com/account/emails).
    """
    setup_future_usage: NotRequired[Literal["off_session", "on_session"]]
    """
    Indicates that you intend to [make future payments](https://docs.stripe.com/payments/payment-intents#future-usage) with the payment
    method collected by this Checkout Session.

    When setting this to `on_session`, Checkout will show a notice to the
    customer that their payment details will be saved.

    When setting this to `off_session`, Checkout will show a notice to the
    customer that their payment details will be saved and used for future
    payments.

    If a Customer has been provided or Checkout creates a new Customer,
    Checkout will attach the payment method to the Customer.

    If Checkout does not create a Customer, the payment method is not attached
    to a Customer. To reuse the payment method, you can retrieve it from the
    Checkout Session's PaymentIntent.

    When processing card payments, Checkout also uses `setup_future_usage`
    to dynamically optimize your payment flow and comply with regional
    legislation and network rules, such as SCA.
    """
    shipping: NotRequired["SessionCreateParamsPaymentIntentDataShipping"]
    """
    Shipping information for this payment.
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
    transfer_data: NotRequired[
        "SessionCreateParamsPaymentIntentDataTransferData"
    ]
    """
    The parameters used to automatically create a Transfer when the payment succeeds.
    For more information, see the PaymentIntents [use case for connected accounts](https://docs.stripe.com/payments/connected-accounts).
    """
    transfer_group: NotRequired[str]
    """
    A string that identifies the resulting payment as part of a group. See the PaymentIntents [use case for connected accounts](https://docs.stripe.com/connect/separate-charges-and-transfers) for details.
    """


class SessionCreateParamsPaymentIntentDataShipping(TypedDict):
    address: "SessionCreateParamsPaymentIntentDataShippingAddress"
    """
    Shipping address.
    """
    carrier: NotRequired[str]
    """
    The delivery service that shipped a physical product, such as Fedex, UPS, USPS, etc.
    """
    name: str
    """
    Recipient name.
    """
    phone: NotRequired[str]
    """
    Recipient phone (including extension).
    """
    tracking_number: NotRequired[str]
    """
    The tracking number for a physical product, obtained from the delivery service. If multiple tracking numbers were generated for this purchase, please separate them with commas.
    """


class SessionCreateParamsPaymentIntentDataShippingAddress(TypedDict):
    city: NotRequired[str]
    """
    City, district, suburb, town, or village.
    """
    country: NotRequired[str]
    """
    Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
    """
    line1: str
    """
    Address line 1, such as the street, PO Box, or company name.
    """
    line2: NotRequired[str]
    """
    Address line 2, such as the apartment, suite, unit, or building.
    """
    postal_code: NotRequired[str]
    """
    ZIP or postal code.
    """
    state: NotRequired[str]
    """
    State, county, province, or region ([ISO 3166-2](https://en.wikipedia.org/wiki/ISO_3166-2)).
    """


class SessionCreateParamsPaymentIntentDataTransferData(TypedDict):
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


class SessionCreateParamsPaymentMethodData(TypedDict):
    allow_redisplay: NotRequired[Literal["always", "limited", "unspecified"]]
    """
    Allow redisplay will be set on the payment method on confirmation and indicates whether this payment method can be shown again to the customer in a checkout flow. Only set this field if you wish to override the allow_redisplay value determined by Checkout.
    """


class SessionCreateParamsPaymentMethodOptions(TypedDict):
    acss_debit: NotRequired["SessionCreateParamsPaymentMethodOptionsAcssDebit"]
    """
    contains details about the ACSS Debit payment method options. You can't set this parameter if `ui_mode` is `custom`.
    """
    affirm: NotRequired["SessionCreateParamsPaymentMethodOptionsAffirm"]
    """
    contains details about the Affirm payment method options.
    """
    afterpay_clearpay: NotRequired[
        "SessionCreateParamsPaymentMethodOptionsAfterpayClearpay"
    ]
    """
    contains details about the Afterpay Clearpay payment method options.
    """
    alipay: NotRequired["SessionCreateParamsPaymentMethodOptionsAlipay"]
    """
    contains details about the Alipay payment method options.
    """
    alma: NotRequired["SessionCreateParamsPaymentMethodOptionsAlma"]
    """
    contains details about the Alma payment method options.
    """
    amazon_pay: NotRequired["SessionCreateParamsPaymentMethodOptionsAmazonPay"]
    """
    contains details about the AmazonPay payment method options.
    """
    au_becs_debit: NotRequired[
        "SessionCreateParamsPaymentMethodOptionsAuBecsDebit"
    ]
    """
    contains details about the AU Becs Debit payment method options.
    """
    bacs_debit: NotRequired["SessionCreateParamsPaymentMethodOptionsBacsDebit"]
    """
    contains details about the Bacs Debit payment method options.
    """
    bancontact: NotRequired[
        "SessionCreateParamsPaymentMethodOptionsBancontact"
    ]
    """
    contains details about the Bancontact payment method options.
    """
    billie: NotRequired["SessionCreateParamsPaymentMethodOptionsBillie"]
    """
    contains details about the Billie payment method options.
    """
    boleto: NotRequired["SessionCreateParamsPaymentMethodOptionsBoleto"]
    """
    contains details about the Boleto payment method options.
    """
    card: NotRequired["SessionCreateParamsPaymentMethodOptionsCard"]
    """
    contains details about the Card payment method options.
    """
    cashapp: NotRequired["SessionCreateParamsPaymentMethodOptionsCashapp"]
    """
    contains details about the Cashapp Pay payment method options.
    """
    customer_balance: NotRequired[
        "SessionCreateParamsPaymentMethodOptionsCustomerBalance"
    ]
    """
    contains details about the Customer Balance payment method options.
    """
    demo_pay: NotRequired["SessionCreateParamsPaymentMethodOptionsDemoPay"]
    """
    contains details about the DemoPay payment method options.
    """
    eps: NotRequired["SessionCreateParamsPaymentMethodOptionsEps"]
    """
    contains details about the EPS payment method options.
    """
    fpx: NotRequired["SessionCreateParamsPaymentMethodOptionsFpx"]
    """
    contains details about the FPX payment method options.
    """
    giropay: NotRequired["SessionCreateParamsPaymentMethodOptionsGiropay"]
    """
    contains details about the Giropay payment method options.
    """
    grabpay: NotRequired["SessionCreateParamsPaymentMethodOptionsGrabpay"]
    """
    contains details about the Grabpay payment method options.
    """
    ideal: NotRequired["SessionCreateParamsPaymentMethodOptionsIdeal"]
    """
    contains details about the Ideal payment method options.
    """
    kakao_pay: NotRequired["SessionCreateParamsPaymentMethodOptionsKakaoPay"]
    """
    contains details about the Kakao Pay payment method options.
    """
    klarna: NotRequired["SessionCreateParamsPaymentMethodOptionsKlarna"]
    """
    contains details about the Klarna payment method options.
    """
    konbini: NotRequired["SessionCreateParamsPaymentMethodOptionsKonbini"]
    """
    contains details about the Konbini payment method options.
    """
    kr_card: NotRequired["SessionCreateParamsPaymentMethodOptionsKrCard"]
    """
    contains details about the Korean card payment method options.
    """
    link: NotRequired["SessionCreateParamsPaymentMethodOptionsLink"]
    """
    contains details about the Link payment method options.
    """
    mobilepay: NotRequired["SessionCreateParamsPaymentMethodOptionsMobilepay"]
    """
    contains details about the Mobilepay payment method options.
    """
    multibanco: NotRequired[
        "SessionCreateParamsPaymentMethodOptionsMultibanco"
    ]
    """
    contains details about the Multibanco payment method options.
    """
    naver_pay: NotRequired["SessionCreateParamsPaymentMethodOptionsNaverPay"]
    """
    contains details about the Naver Pay payment method options.
    """
    oxxo: NotRequired["SessionCreateParamsPaymentMethodOptionsOxxo"]
    """
    contains details about the OXXO payment method options.
    """
    p24: NotRequired["SessionCreateParamsPaymentMethodOptionsP24"]
    """
    contains details about the P24 payment method options.
    """
    pay_by_bank: NotRequired[
        "SessionCreateParamsPaymentMethodOptionsPayByBank"
    ]
    """
    contains details about the Pay By Bank payment method options.
    """
    payco: NotRequired["SessionCreateParamsPaymentMethodOptionsPayco"]
    """
    contains details about the PAYCO payment method options.
    """
    paynow: NotRequired["SessionCreateParamsPaymentMethodOptionsPaynow"]
    """
    contains details about the PayNow payment method options.
    """
    paypal: NotRequired["SessionCreateParamsPaymentMethodOptionsPaypal"]
    """
    contains details about the PayPal payment method options.
    """
    payto: NotRequired["SessionCreateParamsPaymentMethodOptionsPayto"]
    """
    contains details about the PayTo payment method options.
    """
    pix: NotRequired["SessionCreateParamsPaymentMethodOptionsPix"]
    """
    contains details about the Pix payment method options.
    """
    revolut_pay: NotRequired[
        "SessionCreateParamsPaymentMethodOptionsRevolutPay"
    ]
    """
    contains details about the RevolutPay payment method options.
    """
    samsung_pay: NotRequired[
        "SessionCreateParamsPaymentMethodOptionsSamsungPay"
    ]
    """
    contains details about the Samsung Pay payment method options.
    """
    satispay: NotRequired["SessionCreateParamsPaymentMethodOptionsSatispay"]
    """
    contains details about the Satispay payment method options.
    """
    sepa_debit: NotRequired["SessionCreateParamsPaymentMethodOptionsSepaDebit"]
    """
    contains details about the Sepa Debit payment method options.
    """
    sofort: NotRequired["SessionCreateParamsPaymentMethodOptionsSofort"]
    """
    contains details about the Sofort payment method options.
    """
    swish: NotRequired["SessionCreateParamsPaymentMethodOptionsSwish"]
    """
    contains details about the Swish payment method options.
    """
    twint: NotRequired["SessionCreateParamsPaymentMethodOptionsTwint"]
    """
    contains details about the TWINT payment method options.
    """
    us_bank_account: NotRequired[
        "SessionCreateParamsPaymentMethodOptionsUsBankAccount"
    ]
    """
    contains details about the Us Bank Account payment method options.
    """
    wechat_pay: NotRequired["SessionCreateParamsPaymentMethodOptionsWechatPay"]
    """
    contains details about the WeChat Pay payment method options.
    """


class SessionCreateParamsPaymentMethodOptionsAcssDebit(TypedDict):
    currency: NotRequired[Literal["cad", "usd"]]
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies). This is only accepted for Checkout Sessions in `setup` mode.
    """
    mandate_options: NotRequired[
        "SessionCreateParamsPaymentMethodOptionsAcssDebitMandateOptions"
    ]
    """
    Additional fields for Mandate creation
    """
    setup_future_usage: NotRequired[
        Literal["none", "off_session", "on_session"]
    ]
    """
    Indicates that you intend to make future payments with this PaymentIntent's payment method.

    If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

    If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

    When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
    """
    target_date: NotRequired[str]
    """
    Controls when Stripe will attempt to debit the funds from the customer's account. The date must be a string in YYYY-MM-DD format. The date must be in the future and between 3 and 15 calendar days from now.
    """
    verification_method: NotRequired[
        Literal["automatic", "instant", "microdeposits"]
    ]
    """
    Verification method for the intent
    """


class SessionCreateParamsPaymentMethodOptionsAcssDebitMandateOptions(
    TypedDict
):
    custom_mandate_url: NotRequired["Literal['']|str"]
    """
    A URL for custom mandate text to render during confirmation step.
    The URL will be rendered with additional GET parameters `payment_intent` and `payment_intent_client_secret` when confirming a Payment Intent,
    or `setup_intent` and `setup_intent_client_secret` when confirming a Setup Intent.
    """
    default_for: NotRequired[List[Literal["invoice", "subscription"]]]
    """
    List of Stripe products where this mandate can be selected automatically. Only usable in `setup` mode.
    """
    interval_description: NotRequired[str]
    """
    Description of the mandate interval. Only required if 'payment_schedule' parameter is 'interval' or 'combined'.
    """
    payment_schedule: NotRequired[Literal["combined", "interval", "sporadic"]]
    """
    Payment schedule for the mandate.
    """
    transaction_type: NotRequired[Literal["business", "personal"]]
    """
    Transaction type of the mandate.
    """


class SessionCreateParamsPaymentMethodOptionsAffirm(TypedDict):
    capture_method: NotRequired[Literal["manual"]]
    """
    Controls when the funds will be captured from the customer's account.
    """
    setup_future_usage: NotRequired[Literal["none"]]
    """
    Indicates that you intend to make future payments with this PaymentIntent's payment method.

    If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

    If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

    When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
    """


class SessionCreateParamsPaymentMethodOptionsAfterpayClearpay(TypedDict):
    capture_method: NotRequired[Literal["manual"]]
    """
    Controls when the funds will be captured from the customer's account.
    """
    setup_future_usage: NotRequired[Literal["none"]]
    """
    Indicates that you intend to make future payments with this PaymentIntent's payment method.

    If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

    If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

    When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
    """


class SessionCreateParamsPaymentMethodOptionsAlipay(TypedDict):
    setup_future_usage: NotRequired[Literal["none"]]
    """
    Indicates that you intend to make future payments with this PaymentIntent's payment method.

    If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

    If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

    When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
    """


class SessionCreateParamsPaymentMethodOptionsAlma(TypedDict):
    capture_method: NotRequired[Literal["manual"]]
    """
    Controls when the funds will be captured from the customer's account.
    """


class SessionCreateParamsPaymentMethodOptionsAmazonPay(TypedDict):
    capture_method: NotRequired[Literal["manual"]]
    """
    Controls when the funds will be captured from the customer's account.
    """
    setup_future_usage: NotRequired[Literal["none", "off_session"]]
    """
    Indicates that you intend to make future payments with this PaymentIntent's payment method.

    If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

    If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

    When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
    """


class SessionCreateParamsPaymentMethodOptionsAuBecsDebit(TypedDict):
    setup_future_usage: NotRequired[Literal["none"]]
    """
    Indicates that you intend to make future payments with this PaymentIntent's payment method.

    If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

    If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

    When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
    """
    target_date: NotRequired[str]
    """
    Controls when Stripe will attempt to debit the funds from the customer's account. The date must be a string in YYYY-MM-DD format. The date must be in the future and between 3 and 15 calendar days from now.
    """


class SessionCreateParamsPaymentMethodOptionsBacsDebit(TypedDict):
    mandate_options: NotRequired[
        "SessionCreateParamsPaymentMethodOptionsBacsDebitMandateOptions"
    ]
    """
    Additional fields for Mandate creation
    """
    setup_future_usage: NotRequired[
        Literal["none", "off_session", "on_session"]
    ]
    """
    Indicates that you intend to make future payments with this PaymentIntent's payment method.

    If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

    If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

    When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
    """
    target_date: NotRequired[str]
    """
    Controls when Stripe will attempt to debit the funds from the customer's account. The date must be a string in YYYY-MM-DD format. The date must be in the future and between 3 and 15 calendar days from now.
    """


class SessionCreateParamsPaymentMethodOptionsBacsDebitMandateOptions(
    TypedDict
):
    reference_prefix: NotRequired["Literal['']|str"]
    """
    Prefix used to generate the Mandate reference. Must be at most 12 characters long. Must consist of only uppercase letters, numbers, spaces, or the following special characters: '/', '_', '-', '&', '.'. Cannot begin with 'DDIC' or 'STRIPE'.
    """


class SessionCreateParamsPaymentMethodOptionsBancontact(TypedDict):
    setup_future_usage: NotRequired[Literal["none"]]
    """
    Indicates that you intend to make future payments with this PaymentIntent's payment method.

    If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

    If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

    When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
    """


class SessionCreateParamsPaymentMethodOptionsBillie(TypedDict):
    capture_method: NotRequired[Literal["manual"]]
    """
    Controls when the funds will be captured from the customer's account.
    """


class SessionCreateParamsPaymentMethodOptionsBoleto(TypedDict):
    expires_after_days: NotRequired[int]
    """
    The number of calendar days before a Boleto voucher expires. For example, if you create a Boleto voucher on Monday and you set expires_after_days to 2, the Boleto invoice will expire on Wednesday at 23:59 America/Sao_Paulo time.
    """
    setup_future_usage: NotRequired[
        Literal["none", "off_session", "on_session"]
    ]
    """
    Indicates that you intend to make future payments with this PaymentIntent's payment method.

    If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

    If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

    When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
    """


class SessionCreateParamsPaymentMethodOptionsCard(TypedDict):
    capture_method: NotRequired[Literal["manual"]]
    """
    Controls when the funds will be captured from the customer's account.
    """
    installments: NotRequired[
        "SessionCreateParamsPaymentMethodOptionsCardInstallments"
    ]
    """
    Installment options for card payments
    """
    request_extended_authorization: NotRequired[
        Literal["if_available", "never"]
    ]
    """
    Request ability to [capture beyond the standard authorization validity window](https://docs.stripe.com/payments/extended-authorization) for this CheckoutSession.
    """
    request_incremental_authorization: NotRequired[
        Literal["if_available", "never"]
    ]
    """
    Request ability to [increment the authorization](https://docs.stripe.com/payments/incremental-authorization) for this CheckoutSession.
    """
    request_multicapture: NotRequired[Literal["if_available", "never"]]
    """
    Request ability to make [multiple captures](https://docs.stripe.com/payments/multicapture) for this CheckoutSession.
    """
    request_overcapture: NotRequired[Literal["if_available", "never"]]
    """
    Request ability to [overcapture](https://docs.stripe.com/payments/overcapture) for this CheckoutSession.
    """
    request_three_d_secure: NotRequired[
        Literal["any", "automatic", "challenge"]
    ]
    """
    We strongly recommend that you rely on our SCA Engine to automatically prompt your customers for authentication based on risk level and [other requirements](https://docs.stripe.com/strong-customer-authentication). However, if you wish to request 3D Secure based on logic from your own fraud engine, provide this option. If not provided, this value defaults to `automatic`. Read our guide on [manually requesting 3D Secure](https://docs.stripe.com/payments/3d-secure/authentication-flow#manual-three-ds) for more information on how this configuration interacts with Radar and our SCA Engine.
    """
    restrictions: NotRequired[
        "SessionCreateParamsPaymentMethodOptionsCardRestrictions"
    ]
    """
    Restrictions to apply to the card payment method. For example, you can block specific card brands. You can't set this parameter if `ui_mode` is `custom`.
    """
    setup_future_usage: NotRequired[Literal["off_session", "on_session"]]
    """
    Indicates that you intend to make future payments with this PaymentIntent's payment method.

    If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

    If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

    When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
    """
    statement_descriptor_suffix_kana: NotRequired[str]
    """
    Provides information about a card payment that customers see on their statements. Concatenated with the Kana prefix (shortened Kana descriptor) or Kana statement descriptor that's set on the account to form the complete statement descriptor. Maximum 22 characters. On card statements, the *concatenation* of both prefix and suffix (including separators) will appear truncated to 22 characters.
    """
    statement_descriptor_suffix_kanji: NotRequired[str]
    """
    Provides information about a card payment that customers see on their statements. Concatenated with the Kanji prefix (shortened Kanji descriptor) or Kanji statement descriptor that's set on the account to form the complete statement descriptor. Maximum 17 characters. On card statements, the *concatenation* of both prefix and suffix (including separators) will appear truncated to 17 characters.
    """


class SessionCreateParamsPaymentMethodOptionsCardInstallments(TypedDict):
    enabled: NotRequired[bool]
    """
    Setting to true enables installments for this Checkout Session.
    Setting to false will prevent any installment plan from applying to a payment.
    """


class SessionCreateParamsPaymentMethodOptionsCardRestrictions(TypedDict):
    brands_blocked: NotRequired[
        List[
            Literal[
                "american_express",
                "discover_global_network",
                "mastercard",
                "visa",
            ]
        ]
    ]
    """
    Specify the card brands to block in the Checkout Session. If a customer enters or selects a card belonging to a blocked brand, they can't complete the Session.
    """


class SessionCreateParamsPaymentMethodOptionsCashapp(TypedDict):
    capture_method: NotRequired[Literal["manual"]]
    """
    Controls when the funds will be captured from the customer's account.
    """
    setup_future_usage: NotRequired[
        Literal["none", "off_session", "on_session"]
    ]
    """
    Indicates that you intend to make future payments with this PaymentIntent's payment method.

    If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

    If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

    When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
    """


class SessionCreateParamsPaymentMethodOptionsCustomerBalance(TypedDict):
    bank_transfer: NotRequired[
        "SessionCreateParamsPaymentMethodOptionsCustomerBalanceBankTransfer"
    ]
    """
    Configuration for the bank transfer funding type, if the `funding_type` is set to `bank_transfer`.
    """
    funding_type: NotRequired[Literal["bank_transfer"]]
    """
    The funding method type to be used when there are not enough funds in the customer balance. Permitted values include: `bank_transfer`.
    """
    setup_future_usage: NotRequired[Literal["none"]]
    """
    Indicates that you intend to make future payments with this PaymentIntent's payment method.

    If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

    If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

    When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
    """


class SessionCreateParamsPaymentMethodOptionsCustomerBalanceBankTransfer(
    TypedDict,
):
    eu_bank_transfer: NotRequired[
        "SessionCreateParamsPaymentMethodOptionsCustomerBalanceBankTransferEuBankTransfer"
    ]
    """
    Configuration for eu_bank_transfer funding type.
    """
    requested_address_types: NotRequired[
        List[
            Literal[
                "aba", "iban", "sepa", "sort_code", "spei", "swift", "zengin"
            ]
        ]
    ]
    """
    List of address types that should be returned in the financial_addresses response. If not specified, all valid types will be returned.

    Permitted values include: `sort_code`, `zengin`, `iban`, or `spei`.
    """
    type: Literal[
        "eu_bank_transfer",
        "gb_bank_transfer",
        "jp_bank_transfer",
        "mx_bank_transfer",
        "us_bank_transfer",
    ]
    """
    The list of bank transfer types that this PaymentIntent is allowed to use for funding.
    """


class SessionCreateParamsPaymentMethodOptionsCustomerBalanceBankTransferEuBankTransfer(
    TypedDict,
):
    country: str
    """
    The desired country code of the bank account information. Permitted values include: `DE`, `FR`, `IE`, or `NL`.
    """


class SessionCreateParamsPaymentMethodOptionsDemoPay(TypedDict):
    setup_future_usage: NotRequired[Literal["none", "off_session"]]
    """
    Indicates that you intend to make future payments with this PaymentIntent's payment method.

    If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

    If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

    When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
    """


class SessionCreateParamsPaymentMethodOptionsEps(TypedDict):
    setup_future_usage: NotRequired[Literal["none"]]
    """
    Indicates that you intend to make future payments with this PaymentIntent's payment method.

    If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

    If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

    When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
    """


class SessionCreateParamsPaymentMethodOptionsFpx(TypedDict):
    setup_future_usage: NotRequired[Literal["none"]]
    """
    Indicates that you intend to make future payments with this PaymentIntent's payment method.

    If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

    If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

    When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
    """


class SessionCreateParamsPaymentMethodOptionsGiropay(TypedDict):
    setup_future_usage: NotRequired[Literal["none"]]
    """
    Indicates that you intend to make future payments with this PaymentIntent's payment method.

    If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

    If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

    When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
    """


class SessionCreateParamsPaymentMethodOptionsGrabpay(TypedDict):
    setup_future_usage: NotRequired[Literal["none"]]
    """
    Indicates that you intend to make future payments with this PaymentIntent's payment method.

    If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

    If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

    When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
    """


class SessionCreateParamsPaymentMethodOptionsIdeal(TypedDict):
    setup_future_usage: NotRequired[Literal["none"]]
    """
    Indicates that you intend to make future payments with this PaymentIntent's payment method.

    If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

    If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

    When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
    """


class SessionCreateParamsPaymentMethodOptionsKakaoPay(TypedDict):
    capture_method: NotRequired[Literal["manual"]]
    """
    Controls when the funds will be captured from the customer's account.
    """
    setup_future_usage: NotRequired[Literal["none", "off_session"]]
    """
    Indicates that you intend to make future payments with this PaymentIntent's payment method.

    If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

    If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

    When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
    """


class SessionCreateParamsPaymentMethodOptionsKlarna(TypedDict):
    capture_method: NotRequired[Literal["manual"]]
    """
    Controls when the funds will be captured from the customer's account.
    """
    setup_future_usage: NotRequired[Literal["none"]]
    """
    Indicates that you intend to make future payments with this PaymentIntent's payment method.

    If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

    If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

    When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
    """
    subscriptions: NotRequired[
        "Literal['']|List[SessionCreateParamsPaymentMethodOptionsKlarnaSubscription]"
    ]
    """
    Subscription details if the Checkout Session sets up a future subscription.
    """


class SessionCreateParamsPaymentMethodOptionsKlarnaSubscription(TypedDict):
    interval: Literal["day", "month", "week", "year"]
    """
    Unit of time between subscription charges.
    """
    interval_count: NotRequired[int]
    """
    The number of intervals (specified in the `interval` attribute) between subscription charges. For example, `interval=month` and `interval_count=3` charges every 3 months.
    """
    name: NotRequired[str]
    """
    Name for subscription.
    """
    next_billing: (
        "SessionCreateParamsPaymentMethodOptionsKlarnaSubscriptionNextBilling"
    )
    """
    Describes the upcoming charge for this subscription.
    """
    reference: str
    """
    A non-customer-facing reference to correlate subscription charges in the Klarna app. Use a value that persists across subscription charges.
    """


class SessionCreateParamsPaymentMethodOptionsKlarnaSubscriptionNextBilling(
    TypedDict,
):
    amount: int
    """
    The amount of the next charge for the subscription.
    """
    date: str
    """
    The date of the next charge for the subscription in YYYY-MM-DD format.
    """


class SessionCreateParamsPaymentMethodOptionsKonbini(TypedDict):
    expires_after_days: NotRequired[int]
    """
    The number of calendar days (between 1 and 60) after which Konbini payment instructions will expire. For example, if a PaymentIntent is confirmed with Konbini and `expires_after_days` set to 2 on Monday JST, the instructions will expire on Wednesday 23:59:59 JST. Defaults to 3 days.
    """
    setup_future_usage: NotRequired[Literal["none"]]
    """
    Indicates that you intend to make future payments with this PaymentIntent's payment method.

    If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

    If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

    When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
    """


class SessionCreateParamsPaymentMethodOptionsKrCard(TypedDict):
    capture_method: NotRequired[Literal["manual"]]
    """
    Controls when the funds will be captured from the customer's account.
    """
    setup_future_usage: NotRequired[Literal["none", "off_session"]]
    """
    Indicates that you intend to make future payments with this PaymentIntent's payment method.

    If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

    If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

    When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
    """


class SessionCreateParamsPaymentMethodOptionsLink(TypedDict):
    capture_method: NotRequired[Literal["manual"]]
    """
    Controls when the funds will be captured from the customer's account.
    """
    setup_future_usage: NotRequired[Literal["none", "off_session"]]
    """
    Indicates that you intend to make future payments with this PaymentIntent's payment method.

    If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

    If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

    When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
    """


class SessionCreateParamsPaymentMethodOptionsMobilepay(TypedDict):
    capture_method: NotRequired[Literal["manual"]]
    """
    Controls when the funds will be captured from the customer's account.
    """
    setup_future_usage: NotRequired[Literal["none"]]
    """
    Indicates that you intend to make future payments with this PaymentIntent's payment method.

    If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

    If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

    When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
    """


class SessionCreateParamsPaymentMethodOptionsMultibanco(TypedDict):
    setup_future_usage: NotRequired[Literal["none"]]
    """
    Indicates that you intend to make future payments with this PaymentIntent's payment method.

    If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

    If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

    When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
    """


class SessionCreateParamsPaymentMethodOptionsNaverPay(TypedDict):
    capture_method: NotRequired[Literal["manual"]]
    """
    Controls when the funds will be captured from the customer's account.
    """
    setup_future_usage: NotRequired[Literal["none", "off_session"]]
    """
    Indicates that you intend to make future payments with this PaymentIntent's payment method.

    If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

    If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

    When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
    """


class SessionCreateParamsPaymentMethodOptionsOxxo(TypedDict):
    expires_after_days: NotRequired[int]
    """
    The number of calendar days before an OXXO voucher expires. For example, if you create an OXXO voucher on Monday and you set expires_after_days to 2, the OXXO invoice will expire on Wednesday at 23:59 America/Mexico_City time.
    """
    setup_future_usage: NotRequired[Literal["none"]]
    """
    Indicates that you intend to make future payments with this PaymentIntent's payment method.

    If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

    If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

    When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
    """


class SessionCreateParamsPaymentMethodOptionsP24(TypedDict):
    setup_future_usage: NotRequired[Literal["none"]]
    """
    Indicates that you intend to make future payments with this PaymentIntent's payment method.

    If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

    If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

    When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
    """
    tos_shown_and_accepted: NotRequired[bool]
    """
    Confirm that the payer has accepted the P24 terms and conditions.
    """


class SessionCreateParamsPaymentMethodOptionsPayByBank(TypedDict):
    pass


class SessionCreateParamsPaymentMethodOptionsPayco(TypedDict):
    capture_method: NotRequired[Literal["manual"]]
    """
    Controls when the funds will be captured from the customer's account.
    """


class SessionCreateParamsPaymentMethodOptionsPaynow(TypedDict):
    setup_future_usage: NotRequired[Literal["none"]]
    """
    Indicates that you intend to make future payments with this PaymentIntent's payment method.

    If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

    If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

    When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
    """


class SessionCreateParamsPaymentMethodOptionsPaypal(TypedDict):
    capture_method: NotRequired["Literal['']|Literal['manual']"]
    """
    Controls when the funds will be captured from the customer's account.
    """
    preferred_locale: NotRequired[
        Literal[
            "cs-CZ",
            "da-DK",
            "de-AT",
            "de-DE",
            "de-LU",
            "el-GR",
            "en-GB",
            "en-US",
            "es-ES",
            "fi-FI",
            "fr-BE",
            "fr-FR",
            "fr-LU",
            "hu-HU",
            "it-IT",
            "nl-BE",
            "nl-NL",
            "pl-PL",
            "pt-PT",
            "sk-SK",
            "sv-SE",
        ]
    ]
    """
    [Preferred locale](https://docs.stripe.com/payments/paypal/supported-locales) of the PayPal checkout page that the customer is redirected to.
    """
    reference: NotRequired[str]
    """
    A reference of the PayPal transaction visible to customer which is mapped to PayPal's invoice ID. This must be a globally unique ID if you have configured in your PayPal settings to block multiple payments per invoice ID.
    """
    risk_correlation_id: NotRequired[str]
    """
    The risk correlation ID for an on-session payment using a saved PayPal payment method.
    """
    setup_future_usage: NotRequired[
        "Literal['']|Literal['none', 'off_session']"
    ]
    """
    Indicates that you intend to make future payments with this PaymentIntent's payment method.

    If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

    If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

    When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).

    If you've already set `setup_future_usage` and you're performing a request using a publishable key, you can only update the value from `on_session` to `off_session`.
    """


class SessionCreateParamsPaymentMethodOptionsPayto(TypedDict):
    mandate_options: NotRequired[
        "SessionCreateParamsPaymentMethodOptionsPaytoMandateOptions"
    ]
    """
    Additional fields for Mandate creation
    """
    setup_future_usage: NotRequired[Literal["none", "off_session"]]
    """
    Indicates that you intend to make future payments with this PaymentIntent's payment method.

    If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

    If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

    When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
    """


class SessionCreateParamsPaymentMethodOptionsPaytoMandateOptions(TypedDict):
    amount: NotRequired["Literal['']|int"]
    """
    Amount that will be collected. It is required when `amount_type` is `fixed`.
    """
    amount_type: NotRequired["Literal['']|Literal['fixed', 'maximum']"]
    """
    The type of amount that will be collected. The amount charged must be exact or up to the value of `amount` param for `fixed` or `maximum` type respectively. Defaults to `maximum`.
    """
    end_date: NotRequired["Literal['']|str"]
    """
    Date, in YYYY-MM-DD format, after which payments will not be collected. Defaults to no end date.
    """
    payment_schedule: NotRequired[
        "Literal['']|Literal['adhoc', 'annual', 'daily', 'fortnightly', 'monthly', 'quarterly', 'semi_annual', 'weekly']"
    ]
    """
    The periodicity at which payments will be collected. Defaults to `adhoc`.
    """
    payments_per_period: NotRequired["Literal['']|int"]
    """
    The number of payments that will be made during a payment period. Defaults to 1 except for when `payment_schedule` is `adhoc`. In that case, it defaults to no limit.
    """
    purpose: NotRequired[
        "Literal['']|Literal['dependant_support', 'government', 'loan', 'mortgage', 'other', 'pension', 'personal', 'retail', 'salary', 'tax', 'utility']"
    ]
    """
    The purpose for which payments are made. Has a default value based on your merchant category code.
    """
    start_date: NotRequired["Literal['']|str"]
    """
    Date, in YYYY-MM-DD format, from which payments will be collected. Defaults to confirmation time.
    """


class SessionCreateParamsPaymentMethodOptionsPix(TypedDict):
    amount_includes_iof: NotRequired[Literal["always", "never"]]
    """
    Determines if the amount includes the IOF tax. Defaults to `never`.
    """
    expires_after_seconds: NotRequired[int]
    """
    The number of seconds (between 10 and 1209600) after which Pix payment will expire. Defaults to 86400 seconds.
    """
    setup_future_usage: NotRequired[Literal["none"]]
    """
    Indicates that you intend to make future payments with this PaymentIntent's payment method.

    If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

    If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

    When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
    """


class SessionCreateParamsPaymentMethodOptionsRevolutPay(TypedDict):
    capture_method: NotRequired[Literal["manual"]]
    """
    Controls when the funds will be captured from the customer's account.
    """
    setup_future_usage: NotRequired[Literal["none", "off_session"]]
    """
    Indicates that you intend to make future payments with this PaymentIntent's payment method.

    If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

    If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

    When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
    """


class SessionCreateParamsPaymentMethodOptionsSamsungPay(TypedDict):
    capture_method: NotRequired[Literal["manual"]]
    """
    Controls when the funds will be captured from the customer's account.
    """


class SessionCreateParamsPaymentMethodOptionsSatispay(TypedDict):
    capture_method: NotRequired[Literal["manual"]]
    """
    Controls when the funds will be captured from the customer's account.
    """


class SessionCreateParamsPaymentMethodOptionsSepaDebit(TypedDict):
    mandate_options: NotRequired[
        "SessionCreateParamsPaymentMethodOptionsSepaDebitMandateOptions"
    ]
    """
    Additional fields for Mandate creation
    """
    setup_future_usage: NotRequired[
        Literal["none", "off_session", "on_session"]
    ]
    """
    Indicates that you intend to make future payments with this PaymentIntent's payment method.

    If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

    If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

    When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
    """
    target_date: NotRequired[str]
    """
    Controls when Stripe will attempt to debit the funds from the customer's account. The date must be a string in YYYY-MM-DD format. The date must be in the future and between 3 and 15 calendar days from now.
    """


class SessionCreateParamsPaymentMethodOptionsSepaDebitMandateOptions(
    TypedDict
):
    reference_prefix: NotRequired["Literal['']|str"]
    """
    Prefix used to generate the Mandate reference. Must be at most 12 characters long. Must consist of only uppercase letters, numbers, spaces, or the following special characters: '/', '_', '-', '&', '.'. Cannot begin with 'STRIPE'.
    """


class SessionCreateParamsPaymentMethodOptionsSofort(TypedDict):
    setup_future_usage: NotRequired[Literal["none"]]
    """
    Indicates that you intend to make future payments with this PaymentIntent's payment method.

    If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

    If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

    When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
    """


class SessionCreateParamsPaymentMethodOptionsSwish(TypedDict):
    reference: NotRequired[str]
    """
    The order reference that will be displayed to customers in the Swish application. Defaults to the `id` of the Payment Intent.
    """


class SessionCreateParamsPaymentMethodOptionsTwint(TypedDict):
    setup_future_usage: NotRequired[Literal["none"]]
    """
    Indicates that you intend to make future payments with this PaymentIntent's payment method.

    If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

    If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

    When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
    """


class SessionCreateParamsPaymentMethodOptionsUsBankAccount(TypedDict):
    financial_connections: NotRequired[
        "SessionCreateParamsPaymentMethodOptionsUsBankAccountFinancialConnections"
    ]
    """
    Additional fields for Financial Connections Session creation
    """
    setup_future_usage: NotRequired[
        Literal["none", "off_session", "on_session"]
    ]
    """
    Indicates that you intend to make future payments with this PaymentIntent's payment method.

    If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

    If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

    When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
    """
    target_date: NotRequired[str]
    """
    Controls when Stripe will attempt to debit the funds from the customer's account. The date must be a string in YYYY-MM-DD format. The date must be in the future and between 3 and 15 calendar days from now.
    """
    verification_method: NotRequired[Literal["automatic", "instant"]]
    """
    Verification method for the intent
    """


class SessionCreateParamsPaymentMethodOptionsUsBankAccountFinancialConnections(
    TypedDict,
):
    permissions: NotRequired[
        List[
            Literal["balances", "ownership", "payment_method", "transactions"]
        ]
    ]
    """
    The list of permissions to request. If this parameter is passed, the `payment_method` permission must be included. Valid permissions include: `balances`, `ownership`, `payment_method`, and `transactions`.
    """
    prefetch: NotRequired[
        List[Literal["balances", "ownership", "transactions"]]
    ]
    """
    List of data features that you would like to retrieve upon account creation.
    """


class SessionCreateParamsPaymentMethodOptionsWechatPay(TypedDict):
    app_id: NotRequired[str]
    """
    The app ID registered with WeChat Pay. Only required when client is ios or android.
    """
    client: Literal["android", "ios", "web"]
    """
    The client type that the end customer will pay from
    """
    setup_future_usage: NotRequired[Literal["none"]]
    """
    Indicates that you intend to make future payments with this PaymentIntent's payment method.

    If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

    If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

    When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
    """


class SessionCreateParamsPermissions(TypedDict):
    update_shipping_details: NotRequired[Literal["client_only", "server_only"]]
    """
    Determines which entity is allowed to update the shipping details.

    Default is `client_only`. Stripe Checkout client will automatically update the shipping details. If set to `server_only`, only your server is allowed to update the shipping details.

    When set to `server_only`, you must add the onShippingDetailsChange event handler when initializing the Stripe Checkout client and manually update the shipping details from your server using the Stripe API.
    """


class SessionCreateParamsPhoneNumberCollection(TypedDict):
    enabled: bool
    """
    Set to `true` to enable phone number collection.

    Can only be set in `payment` and `subscription` mode.
    """


class SessionCreateParamsSavedPaymentMethodOptions(TypedDict):
    allow_redisplay_filters: NotRequired[
        List[Literal["always", "limited", "unspecified"]]
    ]
    """
    Uses the `allow_redisplay` value of each saved payment method to filter the set presented to a returning customer. By default, only saved payment methods with 'allow_redisplay: ‘always' are shown in Checkout.
    """
    payment_method_remove: NotRequired[Literal["disabled", "enabled"]]
    """
    Enable customers to choose if they wish to remove their saved payment methods. Disabled by default.
    """
    payment_method_save: NotRequired[Literal["disabled", "enabled"]]
    """
    Enable customers to choose if they wish to save their payment method for future use. Disabled by default.
    """


class SessionCreateParamsSetupIntentData(TypedDict):
    description: NotRequired[str]
    """
    An arbitrary string attached to the object. Often useful for displaying to users.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    on_behalf_of: NotRequired[str]
    """
    The Stripe account for which the setup is intended.
    """


class SessionCreateParamsShippingAddressCollection(TypedDict):
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


class SessionCreateParamsShippingOption(TypedDict):
    shipping_rate: NotRequired[str]
    """
    The ID of the Shipping Rate to use for this shipping option.
    """
    shipping_rate_data: NotRequired[
        "SessionCreateParamsShippingOptionShippingRateData"
    ]
    """
    Parameters to be passed to Shipping Rate creation for this shipping option.
    """


class SessionCreateParamsShippingOptionShippingRateData(TypedDict):
    delivery_estimate: NotRequired[
        "SessionCreateParamsShippingOptionShippingRateDataDeliveryEstimate"
    ]
    """
    The estimated range for how long shipping will take, meant to be displayable to the customer. This will appear on CheckoutSessions.
    """
    display_name: str
    """
    The name of the shipping rate, meant to be displayable to the customer. This will appear on CheckoutSessions.
    """
    fixed_amount: NotRequired[
        "SessionCreateParamsShippingOptionShippingRateDataFixedAmount"
    ]
    """
    Describes a fixed amount to charge for shipping. Must be present if type is `fixed_amount`.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    tax_behavior: NotRequired[Literal["exclusive", "inclusive", "unspecified"]]
    """
    Specifies whether the rate is considered inclusive of taxes or exclusive of taxes. One of `inclusive`, `exclusive`, or `unspecified`.
    """
    tax_code: NotRequired[str]
    """
    A [tax code](https://docs.stripe.com/tax/tax-categories) ID. The Shipping tax code is `txcd_92010001`.
    """
    type: NotRequired[Literal["fixed_amount"]]
    """
    The type of calculation to use on the shipping rate.
    """


class SessionCreateParamsShippingOptionShippingRateDataDeliveryEstimate(
    TypedDict,
):
    maximum: NotRequired[
        "SessionCreateParamsShippingOptionShippingRateDataDeliveryEstimateMaximum"
    ]
    """
    The upper bound of the estimated range. If empty, represents no upper bound i.e., infinite.
    """
    minimum: NotRequired[
        "SessionCreateParamsShippingOptionShippingRateDataDeliveryEstimateMinimum"
    ]
    """
    The lower bound of the estimated range. If empty, represents no lower bound.
    """


class SessionCreateParamsShippingOptionShippingRateDataDeliveryEstimateMaximum(
    TypedDict,
):
    unit: Literal["business_day", "day", "hour", "month", "week"]
    """
    A unit of time.
    """
    value: int
    """
    Must be greater than 0.
    """


class SessionCreateParamsShippingOptionShippingRateDataDeliveryEstimateMinimum(
    TypedDict,
):
    unit: Literal["business_day", "day", "hour", "month", "week"]
    """
    A unit of time.
    """
    value: int
    """
    Must be greater than 0.
    """


class SessionCreateParamsShippingOptionShippingRateDataFixedAmount(TypedDict):
    amount: int
    """
    A non-negative integer in cents representing how much to charge.
    """
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    currency_options: NotRequired[
        Dict[
            str,
            "SessionCreateParamsShippingOptionShippingRateDataFixedAmountCurrencyOptions",
        ]
    ]
    """
    Shipping rates defined in each available currency option. Each key must be a three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html) and a [supported currency](https://stripe.com/docs/currencies).
    """


class SessionCreateParamsShippingOptionShippingRateDataFixedAmountCurrencyOptions(
    TypedDict,
):
    amount: int
    """
    A non-negative integer in cents representing how much to charge.
    """
    tax_behavior: NotRequired[Literal["exclusive", "inclusive", "unspecified"]]
    """
    Specifies whether the rate is considered inclusive of taxes or exclusive of taxes. One of `inclusive`, `exclusive`, or `unspecified`.
    """


class SessionCreateParamsSubscriptionData(TypedDict):
    application_fee_percent: NotRequired[float]
    """
    A non-negative decimal between 0 and 100, with at most two decimal places. This represents the percentage of the subscription invoice total that will be transferred to the application owner's Stripe account. To use an application fee percent, the request must be made on behalf of another account, using the `Stripe-Account` header or an OAuth key. For more information, see the application fees [documentation](https://stripe.com/docs/connect/subscriptions#collecting-fees-on-subscriptions).
    """
    billing_cycle_anchor: NotRequired[int]
    """
    A future timestamp to anchor the subscription's billing cycle for new subscriptions. You can't set this parameter if `ui_mode` is `custom`.
    """
    billing_mode: NotRequired["SessionCreateParamsSubscriptionDataBillingMode"]
    """
    Controls how prorations and invoices for subscriptions are calculated and orchestrated.
    """
    default_tax_rates: NotRequired[List[str]]
    """
    The tax rates that will apply to any subscription item that does not have
    `tax_rates` set. Invoices created will have their `default_tax_rates` populated
    from the subscription.
    """
    description: NotRequired[str]
    """
    The subscription's description, meant to be displayable to the customer.
    Use this field to optionally store an explanation of the subscription
    for rendering in the [customer portal](https://docs.stripe.com/customer-management).
    """
    invoice_settings: NotRequired[
        "SessionCreateParamsSubscriptionDataInvoiceSettings"
    ]
    """
    All invoices will be billed using the specified settings.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    on_behalf_of: NotRequired[str]
    """
    The account on behalf of which to charge, for each of the subscription's invoices.
    """
    proration_behavior: NotRequired[Literal["create_prorations", "none"]]
    """
    Determines how to handle prorations resulting from the `billing_cycle_anchor`. If no value is passed, the default is `create_prorations`.
    """
    transfer_data: NotRequired[
        "SessionCreateParamsSubscriptionDataTransferData"
    ]
    """
    If specified, the funds from the subscription's invoices will be transferred to the destination and the ID of the resulting transfers will be found on the resulting charges.
    """
    trial_end: NotRequired[int]
    """
    Unix timestamp representing the end of the trial period the customer will get before being charged for the first time. Has to be at least 48 hours in the future.
    """
    trial_period_days: NotRequired[int]
    """
    Integer representing the number of trial period days before the customer is charged for the first time. Has to be at least 1.
    """
    trial_settings: NotRequired[
        "SessionCreateParamsSubscriptionDataTrialSettings"
    ]
    """
    Settings related to subscription trials.
    """


class SessionCreateParamsSubscriptionDataBillingMode(TypedDict):
    flexible: NotRequired[
        "SessionCreateParamsSubscriptionDataBillingModeFlexible"
    ]
    """
    Configure behavior for flexible billing mode.
    """
    type: Literal["classic", "flexible"]
    """
    Controls the calculation and orchestration of prorations and invoices for subscriptions. If no value is passed, the default is `flexible`.
    """


class SessionCreateParamsSubscriptionDataBillingModeFlexible(TypedDict):
    proration_discounts: NotRequired[Literal["included", "itemized"]]
    """
    Controls how invoices and invoice items display proration amounts and discount amounts.
    """


class SessionCreateParamsSubscriptionDataInvoiceSettings(TypedDict):
    issuer: NotRequired[
        "SessionCreateParamsSubscriptionDataInvoiceSettingsIssuer"
    ]
    """
    The connected account that issues the invoice. The invoice is presented with the branding and support information of the specified account.
    """


class SessionCreateParamsSubscriptionDataInvoiceSettingsIssuer(TypedDict):
    account: NotRequired[str]
    """
    The connected account being referenced when `type` is `account`.
    """
    type: Literal["account", "self"]
    """
    Type of the account referenced in the request.
    """


class SessionCreateParamsSubscriptionDataTransferData(TypedDict):
    amount_percent: NotRequired[float]
    """
    A non-negative decimal between 0 and 100, with at most two decimal places. This represents the percentage of the subscription invoice total that will be transferred to the destination account. By default, the entire amount is transferred to the destination.
    """
    destination: str
    """
    ID of an existing, connected Stripe account.
    """


class SessionCreateParamsSubscriptionDataTrialSettings(TypedDict):
    end_behavior: "SessionCreateParamsSubscriptionDataTrialSettingsEndBehavior"
    """
    Defines how the subscription should behave when the user's free trial ends.
    """


class SessionCreateParamsSubscriptionDataTrialSettingsEndBehavior(TypedDict):
    missing_payment_method: Literal["cancel", "create_invoice", "pause"]
    """
    Indicates how the subscription should change when the trial ends if the user did not provide a payment method.
    """


class SessionCreateParamsTaxIdCollection(TypedDict):
    enabled: bool
    """
    Enable tax ID collection during checkout. Defaults to `false`.
    """
    required: NotRequired[Literal["if_supported", "never"]]
    """
    Describes whether a tax ID is required during checkout. Defaults to `never`. You can't set this parameter if `ui_mode` is `custom`.
    """


class SessionCreateParamsWalletOptions(TypedDict):
    link: NotRequired["SessionCreateParamsWalletOptionsLink"]
    """
    contains details about the Link wallet options.
    """


class SessionCreateParamsWalletOptionsLink(TypedDict):
    display: NotRequired[Literal["auto", "never"]]
    """
    Specifies whether Checkout should display Link as a payment option. By default, Checkout will display all the supported wallets that the Checkout Session was created with. This is the `auto` behavior, and it is the default choice.
    """
