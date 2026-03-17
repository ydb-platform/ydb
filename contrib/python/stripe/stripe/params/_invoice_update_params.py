# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class InvoiceUpdateParams(TypedDict):
    account_tax_ids: NotRequired["Literal['']|List[str]"]
    """
    The account tax IDs associated with the invoice. Only editable when the invoice is a draft.
    """
    application_fee_amount: NotRequired[int]
    """
    A fee in cents (or local equivalent) that will be applied to the invoice and transferred to the application owner's Stripe account. The request must be made with an OAuth key or the Stripe-Account header in order to take an application fee. For more information, see the application fees [documentation](https://docs.stripe.com/billing/invoices/connect#collecting-fees).
    """
    auto_advance: NotRequired[bool]
    """
    Controls whether Stripe performs [automatic collection](https://docs.stripe.com/invoicing/integration/automatic-advancement-collection) of the invoice.
    """
    automatic_tax: NotRequired["InvoiceUpdateParamsAutomaticTax"]
    """
    Settings for automatic tax lookup for this invoice.
    """
    automatically_finalizes_at: NotRequired[int]
    """
    The time when this invoice should be scheduled to finalize (up to 5 years in the future). The invoice is finalized at this time if it's still in draft state. To turn off automatic finalization, set `auto_advance` to false.
    """
    collection_method: NotRequired[
        Literal["charge_automatically", "send_invoice"]
    ]
    """
    Either `charge_automatically` or `send_invoice`. This field can be updated only on `draft` invoices.
    """
    custom_fields: NotRequired[
        "Literal['']|List[InvoiceUpdateParamsCustomField]"
    ]
    """
    A list of up to 4 custom fields to be displayed on the invoice. If a value for `custom_fields` is specified, the list specified will replace the existing custom field list on this invoice. Pass an empty string to remove previously-defined fields.
    """
    days_until_due: NotRequired[int]
    """
    The number of days from which the invoice is created until it is due. Only valid for invoices where `collection_method=send_invoice`. This field can only be updated on `draft` invoices.
    """
    default_payment_method: NotRequired[str]
    """
    ID of the default payment method for the invoice. It must belong to the customer associated with the invoice. If not set, defaults to the subscription's default payment method, if any, or to the default payment method in the customer's invoice settings.
    """
    default_source: NotRequired["Literal['']|str"]
    """
    ID of the default payment source for the invoice. It must belong to the customer associated with the invoice and be in a chargeable state. If not set, defaults to the subscription's default source, if any, or to the customer's default source.
    """
    default_tax_rates: NotRequired["Literal['']|List[str]"]
    """
    The tax rates that will apply to any line item that does not have `tax_rates` set. Pass an empty string to remove previously-defined tax rates.
    """
    description: NotRequired[str]
    """
    An arbitrary string attached to the object. Often useful for displaying to users. Referenced as 'memo' in the Dashboard.
    """
    discounts: NotRequired["Literal['']|List[InvoiceUpdateParamsDiscount]"]
    """
    The discounts that will apply to the invoice. Pass an empty string to remove previously-defined discounts.
    """
    due_date: NotRequired[int]
    """
    The date on which payment for this invoice is due. Only valid for invoices where `collection_method=send_invoice`. This field can only be updated on `draft` invoices.
    """
    effective_at: NotRequired["Literal['']|int"]
    """
    The date when this invoice is in effect. Same as `finalized_at` unless overwritten. When defined, this value replaces the system-generated 'Date of issue' printed on the invoice PDF and receipt.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    footer: NotRequired[str]
    """
    Footer to be displayed on the invoice.
    """
    issuer: NotRequired["InvoiceUpdateParamsIssuer"]
    """
    The connected account that issues the invoice. The invoice is presented with the branding and support information of the specified account.
    """
    metadata: NotRequired["Literal['']|Dict[str, str]"]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    number: NotRequired["Literal['']|str"]
    """
    Set the number for this invoice. If no number is present then a number will be assigned automatically when the invoice is finalized. In many markets, regulations require invoices to be unique, sequential and / or gapless. You are responsible for ensuring this is true across all your different invoicing systems in the event that you edit the invoice number using our API. If you use only Stripe for your invoices and do not change invoice numbers, Stripe handles this aspect of compliance for you automatically.
    """
    on_behalf_of: NotRequired["Literal['']|str"]
    """
    The account (if any) for which the funds of the invoice payment are intended. If set, the invoice will be presented with the branding and support information of the specified account. See the [Invoices with Connect](https://docs.stripe.com/billing/invoices/connect) documentation for details.
    """
    payment_settings: NotRequired["InvoiceUpdateParamsPaymentSettings"]
    """
    Configuration settings for the PaymentIntent that is generated when the invoice is finalized.
    """
    rendering: NotRequired["InvoiceUpdateParamsRendering"]
    """
    The rendering-related settings that control how the invoice is displayed on customer-facing surfaces such as PDF and Hosted Invoice Page.
    """
    shipping_cost: NotRequired["Literal['']|InvoiceUpdateParamsShippingCost"]
    """
    Settings for the cost of shipping for this invoice.
    """
    shipping_details: NotRequired[
        "Literal['']|InvoiceUpdateParamsShippingDetails"
    ]
    """
    Shipping details for the invoice. The Invoice PDF will use the `shipping_details` value if it is set, otherwise the PDF will render the shipping address from the customer.
    """
    statement_descriptor: NotRequired[str]
    """
    Extra information about a charge for the customer's credit card statement. It must contain at least one letter. If not specified and this invoice is part of a subscription, the default `statement_descriptor` will be set to the first subscription item's product's `statement_descriptor`.
    """
    transfer_data: NotRequired["Literal['']|InvoiceUpdateParamsTransferData"]
    """
    If specified, the funds from the invoice will be transferred to the destination and the ID of the resulting transfer will be found on the invoice's charge. This will be unset if you POST an empty value.
    """


class InvoiceUpdateParamsAutomaticTax(TypedDict):
    enabled: bool
    """
    Whether Stripe automatically computes tax on this invoice. Note that incompatible invoice items (invoice items with manually specified [tax rates](https://docs.stripe.com/api/tax_rates), negative amounts, or `tax_behavior=unspecified`) cannot be added to automatic tax invoices.
    """
    liability: NotRequired["InvoiceUpdateParamsAutomaticTaxLiability"]
    """
    The account that's liable for tax. If set, the business address and tax registrations required to perform the tax calculation are loaded from this account. The tax transaction is returned in the report of the connected account.
    """


class InvoiceUpdateParamsAutomaticTaxLiability(TypedDict):
    account: NotRequired[str]
    """
    The connected account being referenced when `type` is `account`.
    """
    type: Literal["account", "self"]
    """
    Type of the account referenced in the request.
    """


class InvoiceUpdateParamsCustomField(TypedDict):
    name: str
    """
    The name of the custom field. This may be up to 40 characters.
    """
    value: str
    """
    The value of the custom field. This may be up to 140 characters.
    """


class InvoiceUpdateParamsDiscount(TypedDict):
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


class InvoiceUpdateParamsIssuer(TypedDict):
    account: NotRequired[str]
    """
    The connected account being referenced when `type` is `account`.
    """
    type: Literal["account", "self"]
    """
    Type of the account referenced in the request.
    """


class InvoiceUpdateParamsPaymentSettings(TypedDict):
    default_mandate: NotRequired["Literal['']|str"]
    """
    ID of the mandate to be used for this invoice. It must correspond to the payment method used to pay the invoice, including the invoice's default_payment_method or default_source, if set.
    """
    payment_method_options: NotRequired[
        "InvoiceUpdateParamsPaymentSettingsPaymentMethodOptions"
    ]
    """
    Payment-method-specific configuration to provide to the invoice's PaymentIntent.
    """
    payment_method_types: NotRequired[
        "Literal['']|List[Literal['ach_credit_transfer', 'ach_debit', 'acss_debit', 'affirm', 'amazon_pay', 'au_becs_debit', 'bacs_debit', 'bancontact', 'boleto', 'card', 'cashapp', 'crypto', 'custom', 'customer_balance', 'eps', 'fpx', 'giropay', 'grabpay', 'ideal', 'jp_credit_transfer', 'kakao_pay', 'klarna', 'konbini', 'kr_card', 'link', 'multibanco', 'naver_pay', 'nz_bank_account', 'p24', 'pay_by_bank', 'payco', 'paynow', 'paypal', 'payto', 'promptpay', 'revolut_pay', 'sepa_credit_transfer', 'sepa_debit', 'sofort', 'swish', 'us_bank_account', 'wechat_pay']]"
    ]
    """
    The list of payment method types (e.g. card) to provide to the invoice's PaymentIntent. If not set, Stripe attempts to automatically determine the types to use by looking at the invoice's default payment method, the subscription's default payment method, the customer's default payment method, and your [invoice template settings](https://dashboard.stripe.com/settings/billing/invoice).
    """


class InvoiceUpdateParamsPaymentSettingsPaymentMethodOptions(TypedDict):
    acss_debit: NotRequired[
        "Literal['']|InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsAcssDebit"
    ]
    """
    If paying by `acss_debit`, this sub-hash contains details about the Canadian pre-authorized debit payment method options to pass to the invoice's PaymentIntent.
    """
    bancontact: NotRequired[
        "Literal['']|InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsBancontact"
    ]
    """
    If paying by `bancontact`, this sub-hash contains details about the Bancontact payment method options to pass to the invoice's PaymentIntent.
    """
    card: NotRequired[
        "Literal['']|InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsCard"
    ]
    """
    If paying by `card`, this sub-hash contains details about the Card payment method options to pass to the invoice's PaymentIntent.
    """
    customer_balance: NotRequired[
        "Literal['']|InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsCustomerBalance"
    ]
    """
    If paying by `customer_balance`, this sub-hash contains details about the Bank transfer payment method options to pass to the invoice's PaymentIntent.
    """
    konbini: NotRequired[
        "Literal['']|InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsKonbini"
    ]
    """
    If paying by `konbini`, this sub-hash contains details about the Konbini payment method options to pass to the invoice's PaymentIntent.
    """
    payto: NotRequired[
        "Literal['']|InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsPayto"
    ]
    """
    If paying by `payto`, this sub-hash contains details about the PayTo payment method options to pass to the invoice's PaymentIntent.
    """
    sepa_debit: NotRequired[
        "Literal['']|InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsSepaDebit"
    ]
    """
    If paying by `sepa_debit`, this sub-hash contains details about the SEPA Direct Debit payment method options to pass to the invoice's PaymentIntent.
    """
    us_bank_account: NotRequired[
        "Literal['']|InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsUsBankAccount"
    ]
    """
    If paying by `us_bank_account`, this sub-hash contains details about the ACH direct debit payment method options to pass to the invoice's PaymentIntent.
    """


class InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsAcssDebit(
    TypedDict,
):
    mandate_options: NotRequired[
        "InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsAcssDebitMandateOptions"
    ]
    """
    Additional fields for Mandate creation
    """
    verification_method: NotRequired[
        Literal["automatic", "instant", "microdeposits"]
    ]
    """
    Verification method for the intent
    """


class InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsAcssDebitMandateOptions(
    TypedDict,
):
    transaction_type: NotRequired[Literal["business", "personal"]]
    """
    Transaction type of the mandate.
    """


class InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsBancontact(
    TypedDict,
):
    preferred_language: NotRequired[Literal["de", "en", "fr", "nl"]]
    """
    Preferred language of the Bancontact authorization page that the customer is redirected to.
    """


class InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsCard(TypedDict):
    installments: NotRequired[
        "InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsCardInstallments"
    ]
    """
    Installment configuration for payments attempted on this invoice.

    For more information, see the [installments integration guide](https://docs.stripe.com/payments/installments).
    """
    request_three_d_secure: NotRequired[
        Literal["any", "automatic", "challenge"]
    ]
    """
    We strongly recommend that you rely on our SCA Engine to automatically prompt your customers for authentication based on risk level and [other requirements](https://docs.stripe.com/strong-customer-authentication). However, if you wish to request 3D Secure based on logic from your own fraud engine, provide this option. Read our guide on [manually requesting 3D Secure](https://docs.stripe.com/payments/3d-secure/authentication-flow#manual-three-ds) for more information on how this configuration interacts with Radar and our SCA Engine.
    """


class InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsCardInstallments(
    TypedDict,
):
    enabled: NotRequired[bool]
    """
    Setting to true enables installments for this invoice.
    Setting to false will prevent any selected plan from applying to a payment.
    """
    plan: NotRequired[
        "Literal['']|InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsCardInstallmentsPlan"
    ]
    """
    The selected installment plan to use for this invoice.
    """


class InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsCardInstallmentsPlan(
    TypedDict,
):
    count: NotRequired[int]
    """
    For `fixed_count` installment plans, this is required. It represents the number of installment payments your customer will make to their credit card.
    """
    interval: NotRequired[Literal["month"]]
    """
    For `fixed_count` installment plans, this is required. It represents the interval between installment payments your customer will make to their credit card.
    One of `month`.
    """
    type: Literal["bonus", "fixed_count", "revolving"]
    """
    Type of installment plan, one of `fixed_count`, `bonus`, or `revolving`.
    """


class InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsCustomerBalance(
    TypedDict,
):
    bank_transfer: NotRequired[
        "InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsCustomerBalanceBankTransfer"
    ]
    """
    Configuration for the bank transfer funding type, if the `funding_type` is set to `bank_transfer`.
    """
    funding_type: NotRequired[str]
    """
    The funding method type to be used when there are not enough funds in the customer balance. Permitted values include: `bank_transfer`.
    """


class InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsCustomerBalanceBankTransfer(
    TypedDict,
):
    eu_bank_transfer: NotRequired[
        "InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsCustomerBalanceBankTransferEuBankTransfer"
    ]
    """
    Configuration for eu_bank_transfer funding type.
    """
    type: NotRequired[str]
    """
    The bank transfer type that can be used for funding. Permitted values include: `eu_bank_transfer`, `gb_bank_transfer`, `jp_bank_transfer`, `mx_bank_transfer`, or `us_bank_transfer`.
    """


class InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsCustomerBalanceBankTransferEuBankTransfer(
    TypedDict,
):
    country: str
    """
    The desired country code of the bank account information. Permitted values include: `DE`, `FR`, `IE`, or `NL`.
    """


class InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsKonbini(TypedDict):
    pass


class InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsPayto(TypedDict):
    mandate_options: NotRequired[
        "InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsPaytoMandateOptions"
    ]
    """
    Additional fields for Mandate creation.
    """


class InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsPaytoMandateOptions(
    TypedDict,
):
    amount: NotRequired[int]
    """
    The maximum amount that can be collected in a single invoice. If you don't specify a maximum, then there is no limit.
    """
    purpose: NotRequired[
        Literal[
            "dependant_support",
            "government",
            "loan",
            "mortgage",
            "other",
            "pension",
            "personal",
            "retail",
            "salary",
            "tax",
            "utility",
        ]
    ]
    """
    The purpose for which payments are made. Has a default value based on your merchant category code.
    """


class InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsSepaDebit(
    TypedDict,
):
    pass


class InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsUsBankAccount(
    TypedDict,
):
    financial_connections: NotRequired[
        "InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsUsBankAccountFinancialConnections"
    ]
    """
    Additional fields for Financial Connections Session creation
    """
    verification_method: NotRequired[
        Literal["automatic", "instant", "microdeposits"]
    ]
    """
    Verification method for the intent
    """


class InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsUsBankAccountFinancialConnections(
    TypedDict,
):
    filters: NotRequired[
        "InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters"
    ]
    """
    Provide filters for the linked accounts that the customer can select for the payment method.
    """
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


class InvoiceUpdateParamsPaymentSettingsPaymentMethodOptionsUsBankAccountFinancialConnectionsFilters(
    TypedDict,
):
    account_subcategories: NotRequired[List[Literal["checking", "savings"]]]
    """
    The account subcategories to use to filter for selectable accounts. Valid subcategories are `checking` and `savings`.
    """


class InvoiceUpdateParamsRendering(TypedDict):
    amount_tax_display: NotRequired[
        "Literal['']|Literal['exclude_tax', 'include_inclusive_tax']"
    ]
    """
    How line-item prices and amounts will be displayed with respect to tax on invoice PDFs. One of `exclude_tax` or `include_inclusive_tax`. `include_inclusive_tax` will include inclusive tax (and exclude exclusive tax) in invoice PDF amounts. `exclude_tax` will exclude all tax (inclusive and exclusive alike) from invoice PDF amounts.
    """
    pdf: NotRequired["InvoiceUpdateParamsRenderingPdf"]
    """
    Invoice pdf rendering options
    """
    template: NotRequired[str]
    """
    ID of the invoice rendering template to use for this invoice.
    """
    template_version: NotRequired["Literal['']|int"]
    """
    The specific version of invoice rendering template to use for this invoice.
    """


class InvoiceUpdateParamsRenderingPdf(TypedDict):
    page_size: NotRequired[Literal["a4", "auto", "letter"]]
    """
    Page size for invoice PDF. Can be set to `a4`, `letter`, or `auto`.
     If set to `auto`, invoice PDF page size defaults to `a4` for customers with
     Japanese locale and `letter` for customers with other locales.
    """


class InvoiceUpdateParamsShippingCost(TypedDict):
    shipping_rate: NotRequired[str]
    """
    The ID of the shipping rate to use for this order.
    """
    shipping_rate_data: NotRequired[
        "InvoiceUpdateParamsShippingCostShippingRateData"
    ]
    """
    Parameters to create a new ad-hoc shipping rate for this order.
    """


class InvoiceUpdateParamsShippingCostShippingRateData(TypedDict):
    delivery_estimate: NotRequired[
        "InvoiceUpdateParamsShippingCostShippingRateDataDeliveryEstimate"
    ]
    """
    The estimated range for how long shipping will take, meant to be displayable to the customer. This will appear on CheckoutSessions.
    """
    display_name: str
    """
    The name of the shipping rate, meant to be displayable to the customer. This will appear on CheckoutSessions.
    """
    fixed_amount: NotRequired[
        "InvoiceUpdateParamsShippingCostShippingRateDataFixedAmount"
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


class InvoiceUpdateParamsShippingCostShippingRateDataDeliveryEstimate(
    TypedDict,
):
    maximum: NotRequired[
        "InvoiceUpdateParamsShippingCostShippingRateDataDeliveryEstimateMaximum"
    ]
    """
    The upper bound of the estimated range. If empty, represents no upper bound i.e., infinite.
    """
    minimum: NotRequired[
        "InvoiceUpdateParamsShippingCostShippingRateDataDeliveryEstimateMinimum"
    ]
    """
    The lower bound of the estimated range. If empty, represents no lower bound.
    """


class InvoiceUpdateParamsShippingCostShippingRateDataDeliveryEstimateMaximum(
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


class InvoiceUpdateParamsShippingCostShippingRateDataDeliveryEstimateMinimum(
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


class InvoiceUpdateParamsShippingCostShippingRateDataFixedAmount(TypedDict):
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
            "InvoiceUpdateParamsShippingCostShippingRateDataFixedAmountCurrencyOptions",
        ]
    ]
    """
    Shipping rates defined in each available currency option. Each key must be a three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html) and a [supported currency](https://stripe.com/docs/currencies).
    """


class InvoiceUpdateParamsShippingCostShippingRateDataFixedAmountCurrencyOptions(
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


class InvoiceUpdateParamsShippingDetails(TypedDict):
    address: "InvoiceUpdateParamsShippingDetailsAddress"
    """
    Shipping address
    """
    name: str
    """
    Recipient name.
    """
    phone: NotRequired["Literal['']|str"]
    """
    Recipient phone (including extension)
    """


class InvoiceUpdateParamsShippingDetailsAddress(TypedDict):
    city: NotRequired[str]
    """
    City, district, suburb, town, or village.
    """
    country: NotRequired[str]
    """
    Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
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


class InvoiceUpdateParamsTransferData(TypedDict):
    amount: NotRequired[int]
    """
    The amount that will be transferred automatically when the invoice is paid. If no amount is set, the full amount is transferred.
    """
    destination: str
    """
    ID of an existing, connected Stripe account.
    """
