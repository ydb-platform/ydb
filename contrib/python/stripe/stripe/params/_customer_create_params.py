# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class CustomerCreateParams(RequestOptions):
    address: NotRequired["Literal['']|CustomerCreateParamsAddress"]
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
    cash_balance: NotRequired["CustomerCreateParamsCashBalance"]
    """
    Balance information and default balance settings for this customer.
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
    invoice_settings: NotRequired["CustomerCreateParamsInvoiceSettings"]
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
    payment_method: NotRequired[str]
    phone: NotRequired[str]
    """
    The customer's phone number.
    """
    preferred_locales: NotRequired[List[str]]
    """
    Customer's preferred languages, ordered by preference.
    """
    shipping: NotRequired["Literal['']|CustomerCreateParamsShipping"]
    """
    The customer's shipping information. Appears on invoices emailed to this customer.
    """
    source: NotRequired[str]
    tax: NotRequired["CustomerCreateParamsTax"]
    """
    Tax details about the customer.
    """
    tax_exempt: NotRequired["Literal['']|Literal['exempt', 'none', 'reverse']"]
    """
    The customer's tax exemption. One of `none`, `exempt`, or `reverse`.
    """
    tax_id_data: NotRequired[List["CustomerCreateParamsTaxIdDatum"]]
    """
    The customer's tax IDs.
    """
    test_clock: NotRequired[str]
    """
    ID of the test clock to attach to the customer.
    """
    validate: NotRequired[bool]


class CustomerCreateParamsAddress(TypedDict):
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


class CustomerCreateParamsCashBalance(TypedDict):
    settings: NotRequired["CustomerCreateParamsCashBalanceSettings"]
    """
    Settings controlling the behavior of the customer's cash balance,
    such as reconciliation of funds received.
    """


class CustomerCreateParamsCashBalanceSettings(TypedDict):
    reconciliation_mode: NotRequired[
        Literal["automatic", "manual", "merchant_default"]
    ]
    """
    Controls how funds transferred by the customer are applied to payment intents and invoices. Valid options are `automatic`, `manual`, or `merchant_default`. For more information about these reconciliation modes, see [Reconciliation](https://docs.stripe.com/payments/customer-balance/reconciliation).
    """


class CustomerCreateParamsInvoiceSettings(TypedDict):
    custom_fields: NotRequired[
        "Literal['']|List[CustomerCreateParamsInvoiceSettingsCustomField]"
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
        "Literal['']|CustomerCreateParamsInvoiceSettingsRenderingOptions"
    ]
    """
    Default options for invoice PDF rendering for this customer.
    """


class CustomerCreateParamsInvoiceSettingsCustomField(TypedDict):
    name: str
    """
    The name of the custom field. This may be up to 40 characters.
    """
    value: str
    """
    The value of the custom field. This may be up to 140 characters.
    """


class CustomerCreateParamsInvoiceSettingsRenderingOptions(TypedDict):
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


class CustomerCreateParamsShipping(TypedDict):
    address: "CustomerCreateParamsShippingAddress"
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


class CustomerCreateParamsShippingAddress(TypedDict):
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


class CustomerCreateParamsTax(TypedDict):
    ip_address: NotRequired["Literal['']|str"]
    """
    A recent IP address of the customer used for tax reporting and tax location inference. Stripe recommends updating the IP address when a new PaymentMethod is attached or the address field on the customer is updated. We recommend against updating this field more frequently since it could result in unexpected tax location/reporting outcomes.
    """
    validate_location: NotRequired[Literal["deferred", "immediately"]]
    """
    A flag that indicates when Stripe should validate the customer tax location. Defaults to `deferred`.
    """


class CustomerCreateParamsTaxIdDatum(TypedDict):
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
