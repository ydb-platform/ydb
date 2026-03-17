# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class InvoiceCreatePreviewParams(RequestOptions):
    automatic_tax: NotRequired["InvoiceCreatePreviewParamsAutomaticTax"]
    """
    Settings for automatic tax lookup for this invoice preview.
    """
    currency: NotRequired[str]
    """
    The currency to preview this invoice in. Defaults to that of `customer` if not specified.
    """
    customer: NotRequired[str]
    """
    The identifier of the customer whose upcoming invoice you're retrieving. If `automatic_tax` is enabled then one of `customer`, `customer_details`, `subscription`, or `schedule` must be set.
    """
    customer_account: NotRequired[str]
    """
    The identifier of the account representing the customer whose upcoming invoice you're retrieving. If `automatic_tax` is enabled then one of `customer`, `customer_account`, `customer_details`, `subscription`, or `schedule` must be set.
    """
    customer_details: NotRequired["InvoiceCreatePreviewParamsCustomerDetails"]
    """
    Details about the customer you want to invoice or overrides for an existing customer. If `automatic_tax` is enabled then one of `customer`, `customer_details`, `subscription`, or `schedule` must be set.
    """
    discounts: NotRequired[
        "Literal['']|List[InvoiceCreatePreviewParamsDiscount]"
    ]
    """
    The coupons to redeem into discounts for the invoice preview. If not specified, inherits the discount from the subscription or customer. This works for both coupons directly applied to an invoice and coupons applied to a subscription. Pass an empty string to avoid inheriting any discounts.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    invoice_items: NotRequired[List["InvoiceCreatePreviewParamsInvoiceItem"]]
    """
    List of invoice items to add or update in the upcoming invoice preview (up to 250).
    """
    issuer: NotRequired["InvoiceCreatePreviewParamsIssuer"]
    """
    The connected account that issues the invoice. The invoice is presented with the branding and support information of the specified account.
    """
    on_behalf_of: NotRequired["Literal['']|str"]
    """
    The account (if any) for which the funds of the invoice payment are intended. If set, the invoice will be presented with the branding and support information of the specified account. See the [Invoices with Connect](https://docs.stripe.com/billing/invoices/connect) documentation for details.
    """
    preview_mode: NotRequired[Literal["next", "recurring"]]
    """
    Customizes the types of values to include when calculating the invoice. Defaults to `next` if unspecified.
    """
    schedule: NotRequired[str]
    """
    The identifier of the schedule whose upcoming invoice you'd like to retrieve. Cannot be used with subscription or subscription fields.
    """
    schedule_details: NotRequired["InvoiceCreatePreviewParamsScheduleDetails"]
    """
    The schedule creation or modification params to apply as a preview. Cannot be used with `subscription` or `subscription_` prefixed fields.
    """
    subscription: NotRequired[str]
    """
    The identifier of the subscription for which you'd like to retrieve the upcoming invoice. If not provided, but a `subscription_details.items` is provided, you will preview creating a subscription with those items. If neither `subscription` nor `subscription_details.items` is provided, you will retrieve the next upcoming invoice from among the customer's subscriptions.
    """
    subscription_details: NotRequired[
        "InvoiceCreatePreviewParamsSubscriptionDetails"
    ]
    """
    The subscription creation or modification params to apply as a preview. Cannot be used with `schedule` or `schedule_details` fields.
    """


class InvoiceCreatePreviewParamsAutomaticTax(TypedDict):
    enabled: bool
    """
    Whether Stripe automatically computes tax on this invoice. Note that incompatible invoice items (invoice items with manually specified [tax rates](https://docs.stripe.com/api/tax_rates), negative amounts, or `tax_behavior=unspecified`) cannot be added to automatic tax invoices.
    """
    liability: NotRequired["InvoiceCreatePreviewParamsAutomaticTaxLiability"]
    """
    The account that's liable for tax. If set, the business address and tax registrations required to perform the tax calculation are loaded from this account. The tax transaction is returned in the report of the connected account.
    """


class InvoiceCreatePreviewParamsAutomaticTaxLiability(TypedDict):
    account: NotRequired[str]
    """
    The connected account being referenced when `type` is `account`.
    """
    type: Literal["account", "self"]
    """
    Type of the account referenced in the request.
    """


class InvoiceCreatePreviewParamsCustomerDetails(TypedDict):
    address: NotRequired[
        "Literal['']|InvoiceCreatePreviewParamsCustomerDetailsAddress"
    ]
    """
    The customer's address. Learn about [country-specific requirements for calculating tax](https://docs.stripe.com/invoicing/taxes?dashboard-or-api=dashboard#set-up-customer).
    """
    shipping: NotRequired[
        "Literal['']|InvoiceCreatePreviewParamsCustomerDetailsShipping"
    ]
    """
    The customer's shipping information. Appears on invoices emailed to this customer.
    """
    tax: NotRequired["InvoiceCreatePreviewParamsCustomerDetailsTax"]
    """
    Tax details about the customer.
    """
    tax_exempt: NotRequired["Literal['']|Literal['exempt', 'none', 'reverse']"]
    """
    The customer's tax exemption. One of `none`, `exempt`, or `reverse`.
    """
    tax_ids: NotRequired[
        List["InvoiceCreatePreviewParamsCustomerDetailsTaxId"]
    ]
    """
    The customer's tax IDs.
    """


class InvoiceCreatePreviewParamsCustomerDetailsAddress(TypedDict):
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


class InvoiceCreatePreviewParamsCustomerDetailsShipping(TypedDict):
    address: "InvoiceCreatePreviewParamsCustomerDetailsShippingAddress"
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


class InvoiceCreatePreviewParamsCustomerDetailsShippingAddress(TypedDict):
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


class InvoiceCreatePreviewParamsCustomerDetailsTax(TypedDict):
    ip_address: NotRequired["Literal['']|str"]
    """
    A recent IP address of the customer used for tax reporting and tax location inference. Stripe recommends updating the IP address when a new PaymentMethod is attached or the address field on the customer is updated. We recommend against updating this field more frequently since it could result in unexpected tax location/reporting outcomes.
    """


class InvoiceCreatePreviewParamsCustomerDetailsTaxId(TypedDict):
    type: Literal[
        "ad_nrt",
        "ae_trn",
        "al_tin",
        "am_tin",
        "ao_tin",
        "ar_cuit",
        "au_abn",
        "au_arn",
        "aw_tin",
        "az_tin",
        "ba_tin",
        "bb_tin",
        "bd_bin",
        "bf_ifu",
        "bg_uic",
        "bh_vat",
        "bj_ifu",
        "bo_tin",
        "br_cnpj",
        "br_cpf",
        "bs_tin",
        "by_tin",
        "ca_bn",
        "ca_gst_hst",
        "ca_pst_bc",
        "ca_pst_mb",
        "ca_pst_sk",
        "ca_qst",
        "cd_nif",
        "ch_uid",
        "ch_vat",
        "cl_tin",
        "cm_niu",
        "cn_tin",
        "co_nit",
        "cr_tin",
        "cv_nif",
        "de_stn",
        "do_rcn",
        "ec_ruc",
        "eg_tin",
        "es_cif",
        "et_tin",
        "eu_oss_vat",
        "eu_vat",
        "gb_vat",
        "ge_vat",
        "gn_nif",
        "hk_br",
        "hr_oib",
        "hu_tin",
        "id_npwp",
        "il_vat",
        "in_gst",
        "is_vat",
        "jp_cn",
        "jp_rn",
        "jp_trn",
        "ke_pin",
        "kg_tin",
        "kh_tin",
        "kr_brn",
        "kz_bin",
        "la_tin",
        "li_uid",
        "li_vat",
        "lk_vat",
        "ma_vat",
        "md_vat",
        "me_pib",
        "mk_vat",
        "mr_nif",
        "mx_rfc",
        "my_frp",
        "my_itn",
        "my_sst",
        "ng_tin",
        "no_vat",
        "no_voec",
        "np_pan",
        "nz_gst",
        "om_vat",
        "pe_ruc",
        "ph_tin",
        "pl_nip",
        "ro_tin",
        "rs_pib",
        "ru_inn",
        "ru_kpp",
        "sa_vat",
        "sg_gst",
        "sg_uen",
        "si_tin",
        "sn_ninea",
        "sr_fin",
        "sv_nit",
        "th_vat",
        "tj_tin",
        "tr_tin",
        "tw_vat",
        "tz_vat",
        "ua_vat",
        "ug_tin",
        "us_ein",
        "uy_ruc",
        "uz_tin",
        "uz_vat",
        "ve_rif",
        "vn_tin",
        "za_vat",
        "zm_tin",
        "zw_tin",
    ]
    """
    Type of the tax ID, one of `ad_nrt`, `ae_trn`, `al_tin`, `am_tin`, `ao_tin`, `ar_cuit`, `au_abn`, `au_arn`, `aw_tin`, `az_tin`, `ba_tin`, `bb_tin`, `bd_bin`, `bf_ifu`, `bg_uic`, `bh_vat`, `bj_ifu`, `bo_tin`, `br_cnpj`, `br_cpf`, `bs_tin`, `by_tin`, `ca_bn`, `ca_gst_hst`, `ca_pst_bc`, `ca_pst_mb`, `ca_pst_sk`, `ca_qst`, `cd_nif`, `ch_uid`, `ch_vat`, `cl_tin`, `cm_niu`, `cn_tin`, `co_nit`, `cr_tin`, `cv_nif`, `de_stn`, `do_rcn`, `ec_ruc`, `eg_tin`, `es_cif`, `et_tin`, `eu_oss_vat`, `eu_vat`, `gb_vat`, `ge_vat`, `gn_nif`, `hk_br`, `hr_oib`, `hu_tin`, `id_npwp`, `il_vat`, `in_gst`, `is_vat`, `jp_cn`, `jp_rn`, `jp_trn`, `ke_pin`, `kg_tin`, `kh_tin`, `kr_brn`, `kz_bin`, `la_tin`, `li_uid`, `li_vat`, `lk_vat`, `ma_vat`, `md_vat`, `me_pib`, `mk_vat`, `mr_nif`, `mx_rfc`, `my_frp`, `my_itn`, `my_sst`, `ng_tin`, `no_vat`, `no_voec`, `np_pan`, `nz_gst`, `om_vat`, `pe_ruc`, `ph_tin`, `pl_nip`, `ro_tin`, `rs_pib`, `ru_inn`, `ru_kpp`, `sa_vat`, `sg_gst`, `sg_uen`, `si_tin`, `sn_ninea`, `sr_fin`, `sv_nit`, `th_vat`, `tj_tin`, `tr_tin`, `tw_vat`, `tz_vat`, `ua_vat`, `ug_tin`, `us_ein`, `uy_ruc`, `uz_tin`, `uz_vat`, `ve_rif`, `vn_tin`, `za_vat`, `zm_tin`, or `zw_tin`
    """
    value: str
    """
    Value of the tax ID.
    """


class InvoiceCreatePreviewParamsDiscount(TypedDict):
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


class InvoiceCreatePreviewParamsInvoiceItem(TypedDict):
    amount: NotRequired[int]
    """
    The integer amount in cents (or local equivalent) of previewed invoice item.
    """
    currency: NotRequired[str]
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies). Only applicable to new invoice items.
    """
    description: NotRequired[str]
    """
    An arbitrary string which you can attach to the invoice item. The description is displayed in the invoice for easy tracking.
    """
    discountable: NotRequired[bool]
    """
    Explicitly controls whether discounts apply to this invoice item. Defaults to true, except for negative invoice items.
    """
    discounts: NotRequired[
        "Literal['']|List[InvoiceCreatePreviewParamsInvoiceItemDiscount]"
    ]
    """
    The coupons to redeem into discounts for the invoice item in the preview.
    """
    invoiceitem: NotRequired[str]
    """
    The ID of the invoice item to update in preview. If not specified, a new invoice item will be added to the preview of the upcoming invoice.
    """
    metadata: NotRequired["Literal['']|Dict[str, str]"]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    period: NotRequired["InvoiceCreatePreviewParamsInvoiceItemPeriod"]
    """
    The period associated with this invoice item. When set to different values, the period will be rendered on the invoice. If you have [Stripe Revenue Recognition](https://docs.stripe.com/revenue-recognition) enabled, the period will be used to recognize and defer revenue. See the [Revenue Recognition documentation](https://docs.stripe.com/revenue-recognition/methodology/subscriptions-and-invoicing) for details.
    """
    price: NotRequired[str]
    """
    The ID of the price object. One of `price` or `price_data` is required.
    """
    price_data: NotRequired["InvoiceCreatePreviewParamsInvoiceItemPriceData"]
    """
    Data used to generate a new [Price](https://docs.stripe.com/api/prices) object inline. One of `price` or `price_data` is required.
    """
    quantity: NotRequired[int]
    """
    Non-negative integer. The quantity of units for the invoice item.
    """
    tax_behavior: NotRequired[Literal["exclusive", "inclusive", "unspecified"]]
    """
    Only required if a [default tax behavior](https://docs.stripe.com/tax/products-prices-tax-categories-tax-behavior#setting-a-default-tax-behavior-(recommended)) was not provided in the Stripe Tax settings. Specifies whether the price is considered inclusive of taxes or exclusive of taxes. One of `inclusive`, `exclusive`, or `unspecified`. Once specified as either `inclusive` or `exclusive`, it cannot be changed.
    """
    tax_code: NotRequired["Literal['']|str"]
    """
    A [tax code](https://docs.stripe.com/tax/tax-categories) ID.
    """
    tax_rates: NotRequired["Literal['']|List[str]"]
    """
    The tax rates that apply to the item. When set, any `default_tax_rates` do not apply to this item.
    """
    unit_amount: NotRequired[int]
    """
    The integer unit amount in cents (or local equivalent) of the charge to be applied to the upcoming invoice. This unit_amount will be multiplied by the quantity to get the full amount. If you want to apply a credit to the customer's account, pass a negative unit_amount.
    """
    unit_amount_decimal: NotRequired[str]
    """
    Same as `unit_amount`, but accepts a decimal value in cents (or local equivalent) with at most 12 decimal places. Only one of `unit_amount` and `unit_amount_decimal` can be set.
    """


class InvoiceCreatePreviewParamsInvoiceItemDiscount(TypedDict):
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


class InvoiceCreatePreviewParamsInvoiceItemPeriod(TypedDict):
    end: int
    """
    The end of the period, which must be greater than or equal to the start. This value is inclusive.
    """
    start: int
    """
    The start of the period. This value is inclusive.
    """


class InvoiceCreatePreviewParamsInvoiceItemPriceData(TypedDict):
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    product: str
    """
    The ID of the [Product](https://docs.stripe.com/api/products) that this [Price](https://docs.stripe.com/api/prices) will belong to.
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


class InvoiceCreatePreviewParamsIssuer(TypedDict):
    account: NotRequired[str]
    """
    The connected account being referenced when `type` is `account`.
    """
    type: Literal["account", "self"]
    """
    Type of the account referenced in the request.
    """


class InvoiceCreatePreviewParamsScheduleDetails(TypedDict):
    billing_mode: NotRequired[
        "InvoiceCreatePreviewParamsScheduleDetailsBillingMode"
    ]
    """
    Controls how prorations and invoices for subscriptions are calculated and orchestrated.
    """
    end_behavior: NotRequired[Literal["cancel", "release"]]
    """
    Behavior of the subscription schedule and underlying subscription when it ends. Possible values are `release` or `cancel` with the default being `release`. `release` will end the subscription schedule and keep the underlying subscription running. `cancel` will end the subscription schedule and cancel the underlying subscription.
    """
    phases: NotRequired[List["InvoiceCreatePreviewParamsScheduleDetailsPhase"]]
    """
    List representing phases of the subscription schedule. Each phase can be customized to have different durations, plans, and coupons. If there are multiple phases, the `end_date` of one phase will always equal the `start_date` of the next phase.
    """
    proration_behavior: NotRequired[
        Literal["always_invoice", "create_prorations", "none"]
    ]
    """
    In cases where the `schedule_details` params update the currently active phase, specifies if and how to prorate at the time of the request.
    """


class InvoiceCreatePreviewParamsScheduleDetailsBillingMode(TypedDict):
    flexible: NotRequired[
        "InvoiceCreatePreviewParamsScheduleDetailsBillingModeFlexible"
    ]
    """
    Configure behavior for flexible billing mode.
    """
    type: Literal["classic", "flexible"]
    """
    Controls the calculation and orchestration of prorations and invoices for subscriptions. If no value is passed, the default is `flexible`.
    """


class InvoiceCreatePreviewParamsScheduleDetailsBillingModeFlexible(TypedDict):
    proration_discounts: NotRequired[Literal["included", "itemized"]]
    """
    Controls how invoices and invoice items display proration amounts and discount amounts.
    """


class InvoiceCreatePreviewParamsScheduleDetailsPhase(TypedDict):
    add_invoice_items: NotRequired[
        List["InvoiceCreatePreviewParamsScheduleDetailsPhaseAddInvoiceItem"]
    ]
    """
    A list of prices and quantities that will generate invoice items appended to the next invoice for this phase. You may pass up to 20 items.
    """
    application_fee_percent: NotRequired[float]
    """
    A non-negative decimal between 0 and 100, with at most two decimal places. This represents the percentage of the subscription invoice total that will be transferred to the application owner's Stripe account. The request must be made by a platform account on a connected account in order to set an application fee percentage. For more information, see the application fees [documentation](https://stripe.com/docs/connect/subscriptions#collecting-fees-on-subscriptions).
    """
    automatic_tax: NotRequired[
        "InvoiceCreatePreviewParamsScheduleDetailsPhaseAutomaticTax"
    ]
    """
    Automatic tax settings for this phase.
    """
    billing_cycle_anchor: NotRequired[Literal["automatic", "phase_start"]]
    """
    Can be set to `phase_start` to set the anchor to the start of the phase or `automatic` to automatically change it if needed. Cannot be set to `phase_start` if this phase specifies a trial. For more information, see the billing cycle [documentation](https://docs.stripe.com/billing/subscriptions/billing-cycle).
    """
    billing_thresholds: NotRequired[
        "Literal['']|InvoiceCreatePreviewParamsScheduleDetailsPhaseBillingThresholds"
    ]
    """
    Define thresholds at which an invoice will be sent, and the subscription advanced to a new billing period. Pass an empty string to remove previously-defined thresholds.
    """
    collection_method: NotRequired[
        Literal["charge_automatically", "send_invoice"]
    ]
    """
    Either `charge_automatically`, or `send_invoice`. When charging automatically, Stripe will attempt to pay the underlying subscription at the end of each billing cycle using the default source attached to the customer. When sending an invoice, Stripe will email your customer an invoice with payment instructions and mark the subscription as `active`. Defaults to `charge_automatically` on creation.
    """
    currency: NotRequired[str]
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    default_payment_method: NotRequired[str]
    """
    ID of the default payment method for the subscription schedule. It must belong to the customer associated with the subscription schedule. If not set, invoices will use the default payment method in the customer's invoice settings.
    """
    default_tax_rates: NotRequired["Literal['']|List[str]"]
    """
    A list of [Tax Rate](https://docs.stripe.com/api/tax_rates) ids. These Tax Rates will set the Subscription's [`default_tax_rates`](https://docs.stripe.com/api/subscriptions/create#create_subscription-default_tax_rates), which means they will be the Invoice's [`default_tax_rates`](https://docs.stripe.com/api/invoices/create#create_invoice-default_tax_rates) for any Invoices issued by the Subscription during this Phase.
    """
    description: NotRequired["Literal['']|str"]
    """
    Subscription description, meant to be displayable to the customer. Use this field to optionally store an explanation of the subscription for rendering in Stripe surfaces and certain local payment methods UIs.
    """
    discounts: NotRequired[
        "Literal['']|List[InvoiceCreatePreviewParamsScheduleDetailsPhaseDiscount]"
    ]
    """
    The coupons to redeem into discounts for the schedule phase. If not specified, inherits the discount from the subscription's customer. Pass an empty string to avoid inheriting any discounts.
    """
    duration: NotRequired[
        "InvoiceCreatePreviewParamsScheduleDetailsPhaseDuration"
    ]
    """
    The number of intervals the phase should last. If set, `end_date` must not be set.
    """
    end_date: NotRequired["int|Literal['now']"]
    """
    The date at which this phase of the subscription schedule ends. If set, `duration` must not be set.
    """
    invoice_settings: NotRequired[
        "InvoiceCreatePreviewParamsScheduleDetailsPhaseInvoiceSettings"
    ]
    """
    All invoices will be billed using the specified settings.
    """
    items: List["InvoiceCreatePreviewParamsScheduleDetailsPhaseItem"]
    """
    List of configuration items, each with an attached price, to apply during this phase of the subscription schedule.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to a phase. Metadata on a schedule's phase will update the underlying subscription's `metadata` when the phase is entered, adding new keys and replacing existing keys in the subscription's `metadata`. Individual keys in the subscription's `metadata` can be unset by posting an empty value to them in the phase's `metadata`. To unset all keys in the subscription's `metadata`, update the subscription directly or unset every key individually from the phase's `metadata`.
    """
    on_behalf_of: NotRequired[str]
    """
    The account on behalf of which to charge, for each of the associated subscription's invoices.
    """
    proration_behavior: NotRequired[
        Literal["always_invoice", "create_prorations", "none"]
    ]
    """
    Controls whether the subscription schedule should create [prorations](https://docs.stripe.com/billing/subscriptions/prorations) when transitioning to this phase if there is a difference in billing configuration. It's different from the request-level [proration_behavior](https://docs.stripe.com/api/subscription_schedules/update#update_subscription_schedule-proration_behavior) parameter which controls what happens if the update request affects the billing configuration (item price, quantity, etc.) of the current phase.
    """
    start_date: NotRequired["int|Literal['now']"]
    """
    The date at which this phase of the subscription schedule starts or `now`. Must be set on the first phase.
    """
    transfer_data: NotRequired[
        "InvoiceCreatePreviewParamsScheduleDetailsPhaseTransferData"
    ]
    """
    The data with which to automatically create a Transfer for each of the associated subscription's invoices.
    """
    trial: NotRequired[bool]
    """
    If set to true the entire phase is counted as a trial and the customer will not be charged for any fees.
    """
    trial_end: NotRequired["int|Literal['now']"]
    """
    Sets the phase to trialing from the start date to this date. Must be before the phase end date, can not be combined with `trial`
    """


class InvoiceCreatePreviewParamsScheduleDetailsPhaseAddInvoiceItem(TypedDict):
    discounts: NotRequired[
        List[
            "InvoiceCreatePreviewParamsScheduleDetailsPhaseAddInvoiceItemDiscount"
        ]
    ]
    """
    The coupons to redeem into discounts for the item.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    period: NotRequired[
        "InvoiceCreatePreviewParamsScheduleDetailsPhaseAddInvoiceItemPeriod"
    ]
    """
    The period associated with this invoice item. If not set, `period.start.type` defaults to `max_item_period_start` and `period.end.type` defaults to `min_item_period_end`.
    """
    price: NotRequired[str]
    """
    The ID of the price object. One of `price` or `price_data` is required.
    """
    price_data: NotRequired[
        "InvoiceCreatePreviewParamsScheduleDetailsPhaseAddInvoiceItemPriceData"
    ]
    """
    Data used to generate a new [Price](https://docs.stripe.com/api/prices) object inline. One of `price` or `price_data` is required.
    """
    quantity: NotRequired[int]
    """
    Quantity for this item. Defaults to 1.
    """
    tax_rates: NotRequired["Literal['']|List[str]"]
    """
    The tax rates which apply to the item. When set, the `default_tax_rates` do not apply to this item.
    """


class InvoiceCreatePreviewParamsScheduleDetailsPhaseAddInvoiceItemDiscount(
    TypedDict,
):
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


class InvoiceCreatePreviewParamsScheduleDetailsPhaseAddInvoiceItemPeriod(
    TypedDict,
):
    end: (
        "InvoiceCreatePreviewParamsScheduleDetailsPhaseAddInvoiceItemPeriodEnd"
    )
    """
    End of the invoice item period.
    """
    start: "InvoiceCreatePreviewParamsScheduleDetailsPhaseAddInvoiceItemPeriodStart"
    """
    Start of the invoice item period.
    """


class InvoiceCreatePreviewParamsScheduleDetailsPhaseAddInvoiceItemPeriodEnd(
    TypedDict,
):
    timestamp: NotRequired[int]
    """
    A precise Unix timestamp for the end of the invoice item period. Must be greater than or equal to `period.start`.
    """
    type: Literal["min_item_period_end", "phase_end", "timestamp"]
    """
    Select how to calculate the end of the invoice item period.
    """


class InvoiceCreatePreviewParamsScheduleDetailsPhaseAddInvoiceItemPeriodStart(
    TypedDict,
):
    timestamp: NotRequired[int]
    """
    A precise Unix timestamp for the start of the invoice item period. Must be less than or equal to `period.end`.
    """
    type: Literal["max_item_period_start", "phase_start", "timestamp"]
    """
    Select how to calculate the start of the invoice item period.
    """


class InvoiceCreatePreviewParamsScheduleDetailsPhaseAddInvoiceItemPriceData(
    TypedDict,
):
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    product: str
    """
    The ID of the [Product](https://docs.stripe.com/api/products) that this [Price](https://docs.stripe.com/api/prices) will belong to.
    """
    tax_behavior: NotRequired[Literal["exclusive", "inclusive", "unspecified"]]
    """
    Only required if a [default tax behavior](https://docs.stripe.com/tax/products-prices-tax-categories-tax-behavior#setting-a-default-tax-behavior-(recommended)) was not provided in the Stripe Tax settings. Specifies whether the price is considered inclusive of taxes or exclusive of taxes. One of `inclusive`, `exclusive`, or `unspecified`. Once specified as either `inclusive` or `exclusive`, it cannot be changed.
    """
    unit_amount: NotRequired[int]
    """
    A positive integer in cents (or local equivalent) (or 0 for a free price) representing how much to charge or a negative integer representing the amount to credit to the customer.
    """
    unit_amount_decimal: NotRequired[str]
    """
    Same as `unit_amount`, but accepts a decimal value in cents (or local equivalent) with at most 12 decimal places. Only one of `unit_amount` and `unit_amount_decimal` can be set.
    """


class InvoiceCreatePreviewParamsScheduleDetailsPhaseAutomaticTax(TypedDict):
    enabled: bool
    """
    Enabled automatic tax calculation which will automatically compute tax rates on all invoices generated by the subscription.
    """
    liability: NotRequired[
        "InvoiceCreatePreviewParamsScheduleDetailsPhaseAutomaticTaxLiability"
    ]
    """
    The account that's liable for tax. If set, the business address and tax registrations required to perform the tax calculation are loaded from this account. The tax transaction is returned in the report of the connected account.
    """


class InvoiceCreatePreviewParamsScheduleDetailsPhaseAutomaticTaxLiability(
    TypedDict,
):
    account: NotRequired[str]
    """
    The connected account being referenced when `type` is `account`.
    """
    type: Literal["account", "self"]
    """
    Type of the account referenced in the request.
    """


class InvoiceCreatePreviewParamsScheduleDetailsPhaseBillingThresholds(
    TypedDict,
):
    amount_gte: NotRequired[int]
    """
    Monetary threshold that triggers the subscription to advance to a new billing period
    """
    reset_billing_cycle_anchor: NotRequired[bool]
    """
    Indicates if the `billing_cycle_anchor` should be reset when a threshold is reached. If true, `billing_cycle_anchor` will be updated to the date/time the threshold was last reached; otherwise, the value will remain unchanged.
    """


class InvoiceCreatePreviewParamsScheduleDetailsPhaseDiscount(TypedDict):
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


class InvoiceCreatePreviewParamsScheduleDetailsPhaseDuration(TypedDict):
    interval: Literal["day", "month", "week", "year"]
    """
    Specifies phase duration. Either `day`, `week`, `month` or `year`.
    """
    interval_count: NotRequired[int]
    """
    The multiplier applied to the interval.
    """


class InvoiceCreatePreviewParamsScheduleDetailsPhaseInvoiceSettings(TypedDict):
    account_tax_ids: NotRequired["Literal['']|List[str]"]
    """
    The account tax IDs associated with this phase of the subscription schedule. Will be set on invoices generated by this phase of the subscription schedule.
    """
    days_until_due: NotRequired[int]
    """
    Number of days within which a customer must pay invoices generated by this subscription schedule. This value will be `null` for subscription schedules where `billing=charge_automatically`.
    """
    issuer: NotRequired[
        "InvoiceCreatePreviewParamsScheduleDetailsPhaseInvoiceSettingsIssuer"
    ]
    """
    The connected account that issues the invoice. The invoice is presented with the branding and support information of the specified account.
    """


class InvoiceCreatePreviewParamsScheduleDetailsPhaseInvoiceSettingsIssuer(
    TypedDict,
):
    account: NotRequired[str]
    """
    The connected account being referenced when `type` is `account`.
    """
    type: Literal["account", "self"]
    """
    Type of the account referenced in the request.
    """


class InvoiceCreatePreviewParamsScheduleDetailsPhaseItem(TypedDict):
    billing_thresholds: NotRequired[
        "Literal['']|InvoiceCreatePreviewParamsScheduleDetailsPhaseItemBillingThresholds"
    ]
    """
    Define thresholds at which an invoice will be sent, and the subscription advanced to a new billing period. Pass an empty string to remove previously-defined thresholds.
    """
    discounts: NotRequired[
        "Literal['']|List[InvoiceCreatePreviewParamsScheduleDetailsPhaseItemDiscount]"
    ]
    """
    The coupons to redeem into discounts for the subscription item.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to a configuration item. Metadata on a configuration item will update the underlying subscription item's `metadata` when the phase is entered, adding new keys and replacing existing keys. Individual keys in the subscription item's `metadata` can be unset by posting an empty value to them in the configuration item's `metadata`. To unset all keys in the subscription item's `metadata`, update the subscription item directly or unset every key individually from the configuration item's `metadata`.
    """
    plan: NotRequired[str]
    """
    The plan ID to subscribe to. You may specify the same ID in `plan` and `price`.
    """
    price: NotRequired[str]
    """
    The ID of the price object.
    """
    price_data: NotRequired[
        "InvoiceCreatePreviewParamsScheduleDetailsPhaseItemPriceData"
    ]
    """
    Data used to generate a new [Price](https://docs.stripe.com/api/prices) object inline.
    """
    quantity: NotRequired[int]
    """
    Quantity for the given price. Can be set only if the price's `usage_type` is `licensed` and not `metered`.
    """
    tax_rates: NotRequired["Literal['']|List[str]"]
    """
    A list of [Tax Rate](https://docs.stripe.com/api/tax_rates) ids. These Tax Rates will override the [`default_tax_rates`](https://docs.stripe.com/api/subscriptions/create#create_subscription-default_tax_rates) on the Subscription. When updating, pass an empty string to remove previously-defined tax rates.
    """


class InvoiceCreatePreviewParamsScheduleDetailsPhaseItemBillingThresholds(
    TypedDict,
):
    usage_gte: int
    """
    Number of units that meets the billing threshold to advance the subscription to a new billing period (e.g., it takes 10 $5 units to meet a $50 [monetary threshold](https://docs.stripe.com/api/subscriptions/update#update_subscription-billing_thresholds-amount_gte))
    """


class InvoiceCreatePreviewParamsScheduleDetailsPhaseItemDiscount(TypedDict):
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


class InvoiceCreatePreviewParamsScheduleDetailsPhaseItemPriceData(TypedDict):
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    product: str
    """
    The ID of the [Product](https://docs.stripe.com/api/products) that this [Price](https://docs.stripe.com/api/prices) will belong to.
    """
    recurring: (
        "InvoiceCreatePreviewParamsScheduleDetailsPhaseItemPriceDataRecurring"
    )
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


class InvoiceCreatePreviewParamsScheduleDetailsPhaseItemPriceDataRecurring(
    TypedDict,
):
    interval: Literal["day", "month", "week", "year"]
    """
    Specifies billing frequency. Either `day`, `week`, `month` or `year`.
    """
    interval_count: NotRequired[int]
    """
    The number of intervals between subscription billings. For example, `interval=month` and `interval_count=3` bills every 3 months. Maximum of three years interval allowed (3 years, 36 months, or 156 weeks).
    """


class InvoiceCreatePreviewParamsScheduleDetailsPhaseTransferData(TypedDict):
    amount_percent: NotRequired[float]
    """
    A non-negative decimal between 0 and 100, with at most two decimal places. This represents the percentage of the subscription invoice total that will be transferred to the destination account. By default, the entire amount is transferred to the destination.
    """
    destination: str
    """
    ID of an existing, connected Stripe account.
    """


class InvoiceCreatePreviewParamsSubscriptionDetails(TypedDict):
    billing_cycle_anchor: NotRequired["Literal['now', 'unchanged']|int"]
    """
    For new subscriptions, a future timestamp to anchor the subscription's [billing cycle](https://docs.stripe.com/subscriptions/billing-cycle). This is used to determine the date of the first full invoice, and, for plans with `month` or `year` intervals, the day of the month for subsequent invoices. For existing subscriptions, the value can only be set to `now` or `unchanged`.
    """
    billing_mode: NotRequired[
        "InvoiceCreatePreviewParamsSubscriptionDetailsBillingMode"
    ]
    """
    Controls how prorations and invoices for subscriptions are calculated and orchestrated.
    """
    cancel_at: NotRequired[
        "Literal['']|int|Literal['max_period_end', 'min_period_end']"
    ]
    """
    A timestamp at which the subscription should cancel. If set to a date before the current period ends, this will cause a proration if prorations have been enabled using `proration_behavior`. If set during a future period, this will always cause a proration for that period.
    """
    cancel_at_period_end: NotRequired[bool]
    """
    Indicate whether this subscription should cancel at the end of the current period (`current_period_end`). Defaults to `false`.
    """
    cancel_now: NotRequired[bool]
    """
    This simulates the subscription being canceled or expired immediately.
    """
    default_tax_rates: NotRequired["Literal['']|List[str]"]
    """
    If provided, the invoice returned will preview updating or creating a subscription with these default tax rates. The default tax rates will apply to any line item that does not have `tax_rates` set.
    """
    items: NotRequired[
        List["InvoiceCreatePreviewParamsSubscriptionDetailsItem"]
    ]
    """
    A list of up to 20 subscription items, each with an attached price.
    """
    proration_behavior: NotRequired[
        Literal["always_invoice", "create_prorations", "none"]
    ]
    """
    Determines how to handle [prorations](https://docs.stripe.com/billing/subscriptions/prorations) when the billing cycle changes (e.g., when switching plans, resetting `billing_cycle_anchor=now`, or starting a trial), or if an item's `quantity` changes. The default value is `create_prorations`.
    """
    proration_date: NotRequired[int]
    """
    If previewing an update to a subscription, and doing proration, `subscription_details.proration_date` forces the proration to be calculated as though the update was done at the specified time. The time given must be within the current subscription period and within the current phase of the schedule backing this subscription, if the schedule exists. If set, `subscription`, and one of `subscription_details.items`, or `subscription_details.trial_end` are required. Also, `subscription_details.proration_behavior` cannot be set to 'none'.
    """
    resume_at: NotRequired[Literal["now"]]
    """
    For paused subscriptions, setting `subscription_details.resume_at` to `now` will preview the invoice that will be generated if the subscription is resumed.
    """
    start_date: NotRequired[int]
    """
    Date a subscription is intended to start (can be future or past).
    """
    trial_end: NotRequired["Literal['now']|int"]
    """
    If provided, the invoice returned will preview updating or creating a subscription with that trial end. If set, one of `subscription_details.items` or `subscription` is required.
    """


class InvoiceCreatePreviewParamsSubscriptionDetailsBillingMode(TypedDict):
    flexible: NotRequired[
        "InvoiceCreatePreviewParamsSubscriptionDetailsBillingModeFlexible"
    ]
    """
    Configure behavior for flexible billing mode.
    """
    type: Literal["classic", "flexible"]
    """
    Controls the calculation and orchestration of prorations and invoices for subscriptions. If no value is passed, the default is `flexible`.
    """


class InvoiceCreatePreviewParamsSubscriptionDetailsBillingModeFlexible(
    TypedDict,
):
    proration_discounts: NotRequired[Literal["included", "itemized"]]
    """
    Controls how invoices and invoice items display proration amounts and discount amounts.
    """


class InvoiceCreatePreviewParamsSubscriptionDetailsItem(TypedDict):
    billing_thresholds: NotRequired[
        "Literal['']|InvoiceCreatePreviewParamsSubscriptionDetailsItemBillingThresholds"
    ]
    """
    Define thresholds at which an invoice will be sent, and the subscription advanced to a new billing period. Pass an empty string to remove previously-defined thresholds.
    """
    clear_usage: NotRequired[bool]
    """
    Delete all usage for a given subscription item. You must pass this when deleting a usage records subscription item. `clear_usage` has no effect if the plan has a billing meter attached.
    """
    deleted: NotRequired[bool]
    """
    A flag that, if set to `true`, will delete the specified item.
    """
    discounts: NotRequired[
        "Literal['']|List[InvoiceCreatePreviewParamsSubscriptionDetailsItemDiscount]"
    ]
    """
    The coupons to redeem into discounts for the subscription item.
    """
    id: NotRequired[str]
    """
    Subscription item to update.
    """
    metadata: NotRequired["Literal['']|Dict[str, str]"]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    plan: NotRequired[str]
    """
    Plan ID for this item, as a string.
    """
    price: NotRequired[str]
    """
    The ID of the price object. One of `price` or `price_data` is required. When changing a subscription item's price, `quantity` is set to 1 unless a `quantity` parameter is provided.
    """
    price_data: NotRequired[
        "InvoiceCreatePreviewParamsSubscriptionDetailsItemPriceData"
    ]
    """
    Data used to generate a new [Price](https://docs.stripe.com/api/prices) object inline. One of `price` or `price_data` is required.
    """
    quantity: NotRequired[int]
    """
    Quantity for this item.
    """
    tax_rates: NotRequired["Literal['']|List[str]"]
    """
    A list of [Tax Rate](https://docs.stripe.com/api/tax_rates) ids. These Tax Rates will override the [`default_tax_rates`](https://docs.stripe.com/api/subscriptions/create#create_subscription-default_tax_rates) on the Subscription. When updating, pass an empty string to remove previously-defined tax rates.
    """


class InvoiceCreatePreviewParamsSubscriptionDetailsItemBillingThresholds(
    TypedDict,
):
    usage_gte: int
    """
    Number of units that meets the billing threshold to advance the subscription to a new billing period (e.g., it takes 10 $5 units to meet a $50 [monetary threshold](https://docs.stripe.com/api/subscriptions/update#update_subscription-billing_thresholds-amount_gte))
    """


class InvoiceCreatePreviewParamsSubscriptionDetailsItemDiscount(TypedDict):
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


class InvoiceCreatePreviewParamsSubscriptionDetailsItemPriceData(TypedDict):
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    product: str
    """
    The ID of the [Product](https://docs.stripe.com/api/products) that this [Price](https://docs.stripe.com/api/prices) will belong to.
    """
    recurring: (
        "InvoiceCreatePreviewParamsSubscriptionDetailsItemPriceDataRecurring"
    )
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


class InvoiceCreatePreviewParamsSubscriptionDetailsItemPriceDataRecurring(
    TypedDict,
):
    interval: Literal["day", "month", "week", "year"]
    """
    Specifies billing frequency. Either `day`, `week`, `month` or `year`.
    """
    interval_count: NotRequired[int]
    """
    The number of intervals between subscription billings. For example, `interval=month` and `interval_count=3` bills every 3 months. Maximum of three years interval allowed (3 years, 36 months, or 156 weeks).
    """
