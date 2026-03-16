# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class CustomerUpdateParams(TypedDict):
    address: NotRequired["Literal['']|CustomerUpdateParamsAddress"]
    """
    The customer's address. Learn about [country-specific requirements for calculating tax](https://docs.stripe.com/invoicing/taxes?dashboard-or-api=dashboard#set-up-customer).
    """
    balance: NotRequired[int]
    """
    An integer amount in cents (or local equivalent) that represents the customer's current balance, which affect the customer's future invoices. A negative amount represents a credit that decreases the amount due on an invoice; a positive amount increases the amount due on an invoice.
    """
    business_name: NotRequired["Literal['']|str"]
    """
    The customer's business name. This may be up to *150 characters*.
    """
    cash_balance: NotRequired["CustomerUpdateParamsCashBalance"]
    """
    Balance information and default balance settings for this customer.
    """
    default_source: NotRequired[str]
    """
    If you are using payment methods created via the PaymentMethods API, see the [invoice_settings.default_payment_method](https://docs.stripe.com/api/customers/update#update_customer-invoice_settings-default_payment_method) parameter.

    Provide the ID of a payment source already attached to this customer to make it this customer's default payment source.

    If you want to add a new payment source and make it the default, see the [source](https://docs.stripe.com/api/customers/update#update_customer-source) property.
    """
    description: NotRequired[str]
    """
    An arbitrary string that you can attach to a customer object. It is displayed alongside the customer in the dashboard.
    """
    email: NotRequired[str]
    """
    Customer's email address. It's displayed alongside the customer in your dashboard and can be useful for searching and tracking. This may be up to *512 characters*.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    individual_name: NotRequired["Literal['']|str"]
    """
    The customer's full name. This may be up to *150 characters*.
    """
    invoice_prefix: NotRequired[str]
    """
    The prefix for the customer used to generate unique invoice numbers. Must be 3–12 uppercase letters or numbers.
    """
    invoice_settings: NotRequired["CustomerUpdateParamsInvoiceSettings"]
    """
    Default invoice settings for this customer.
    """
    metadata: NotRequired["Literal['']|Dict[str, str]"]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    name: NotRequired[str]
    """
    The customer's full name or business name.
    """
    next_invoice_sequence: NotRequired[int]
    """
    The sequence to be used on the customer's next invoice. Defaults to 1.
    """
    phone: NotRequired[str]
    """
    The customer's phone number.
    """
    preferred_locales: NotRequired[List[str]]
    """
    Customer's preferred languages, ordered by preference.
    """
    shipping: NotRequired["Literal['']|CustomerUpdateParamsShipping"]
    """
    The customer's shipping information. Appears on invoices emailed to this customer.
    """
    source: NotRequired[str]
    tax: NotRequired["CustomerUpdateParamsTax"]
    """
    Tax details about the customer.
    """
    tax_exempt: NotRequired["Literal['']|Literal['exempt', 'none', 'reverse']"]
    """
    The customer's tax exemption. One of `none`, `exempt`, or `reverse`.
    """
    validate: NotRequired[bool]


class CustomerUpdateParamsAddress(TypedDict):
    city: NotRequired[str]
    """
    City, district, suburb, town, or village.
    """
    country: NotRequired[str]
    """
    A freeform text field for the country. However, in order to activate some tax features, the format should be a two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
    """
    line1: NotRequired[str]
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


class CustomerUpdateParamsCashBalance(TypedDict):
    settings: NotRequired["CustomerUpdateParamsCashBalanceSettings"]
    """
    Settings controlling the behavior of the customer's cash balance,
    such as reconciliation of funds received.
    """


class CustomerUpdateParamsCashBalanceSettings(TypedDict):
    reconciliation_mode: NotRequired[
        Literal["automatic", "manual", "merchant_default"]
    ]
    """
    Controls how funds transferred by the customer are applied to payment intents and invoices. Valid options are `automatic`, `manual`, or `merchant_default`. For more information about these reconciliation modes, see [Reconciliation](https://docs.stripe.com/payments/customer-balance/reconciliation).
    """


class CustomerUpdateParamsInvoiceSettings(TypedDict):
    custom_fields: NotRequired[
        "Literal['']|List[CustomerUpdateParamsInvoiceSettingsCustomField]"
    ]
    """
    The list of up to 4 default custom fields to be displayed on invoices for this customer. When updating, pass an empty string to remove previously-defined fields.
    """
    default_payment_method: NotRequired[str]
    """
    ID of a payment method that's attached to the customer, to be used as the customer's default payment method for subscriptions and invoices.
    """
    footer: NotRequired[str]
    """
    Default footer to be displayed on invoices for this customer.
    """
    rendering_options: NotRequired[
        "Literal['']|CustomerUpdateParamsInvoiceSettingsRenderingOptions"
    ]
    """
    Default options for invoice PDF rendering for this customer.
    """


class CustomerUpdateParamsInvoiceSettingsCustomField(TypedDict):
    name: str
    """
    The name of the custom field. This may be up to 40 characters.
    """
    value: str
    """
    The value of the custom field. This may be up to 140 characters.
    """


class CustomerUpdateParamsInvoiceSettingsRenderingOptions(TypedDict):
    amount_tax_display: NotRequired[
        "Literal['']|Literal['exclude_tax', 'include_inclusive_tax']"
    ]
    """
    How line-item prices and amounts will be displayed with respect to tax on invoice PDFs. One of `exclude_tax` or `include_inclusive_tax`. `include_inclusive_tax` will include inclusive tax (and exclude exclusive tax) in invoice PDF amounts. `exclude_tax` will exclude all tax (inclusive and exclusive alike) from invoice PDF amounts.
    """
    template: NotRequired[str]
    """
    ID of the invoice rendering template to use for future invoices.
    """


class CustomerUpdateParamsShipping(TypedDict):
    address: "CustomerUpdateParamsShippingAddress"
    """
    Customer shipping address.
    """
    name: str
    """
    Customer name.
    """
    phone: NotRequired[str]
    """
    Customer phone (including extension).
    """


class CustomerUpdateParamsShippingAddress(TypedDict):
    city: NotRequired[str]
    """
    City, district, suburb, town, or village.
    """
    country: NotRequired[str]
    """
    A freeform text field for the country. However, in order to activate some tax features, the format should be a two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
    """
    line1: NotRequired[str]
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


class CustomerUpdateParamsTax(TypedDict):
    ip_address: NotRequired["Literal['']|str"]
    """
    A recent IP address of the customer used for tax reporting and tax location inference. Stripe recommends updating the IP address when a new PaymentMethod is attached or the address field on the customer is updated. We recommend against updating this field more frequently since it could result in unexpected tax location/reporting outcomes.
    """
    validate_location: NotRequired[Literal["auto", "deferred", "immediately"]]
    """
    A flag that indicates when Stripe should validate the customer tax location. Defaults to `auto`.
    """
