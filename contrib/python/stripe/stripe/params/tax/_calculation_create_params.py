# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class CalculationCreateParams(RequestOptions):
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    customer: NotRequired[str]
    """
    The ID of an existing customer to use for this calculation. If provided, the customer's address and tax IDs are copied to `customer_details`.
    """
    customer_details: NotRequired["CalculationCreateParamsCustomerDetails"]
    """
    Details about the customer, including address and tax IDs.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    line_items: List["CalculationCreateParamsLineItem"]
    """
    A list of items the customer is purchasing.
    """
    ship_from_details: NotRequired["CalculationCreateParamsShipFromDetails"]
    """
    Details about the address from which the goods are being shipped.
    """
    shipping_cost: NotRequired["CalculationCreateParamsShippingCost"]
    """
    Shipping cost details to be used for the calculation.
    """
    tax_date: NotRequired[int]
    """
    Timestamp of date at which the tax rules and rates in effect applies for the calculation. Measured in seconds since the Unix epoch. Can be up to 48 hours in the past, and up to 48 hours in the future.
    """


class CalculationCreateParamsCustomerDetails(TypedDict):
    address: NotRequired["CalculationCreateParamsCustomerDetailsAddress"]
    """
    The customer's postal address (for example, home or business location).
    """
    address_source: NotRequired[Literal["billing", "shipping"]]
    """
    The type of customer address provided.
    """
    ip_address: NotRequired[str]
    """
    The customer's IP address (IPv4 or IPv6).
    """
    tax_ids: NotRequired[List["CalculationCreateParamsCustomerDetailsTaxId"]]
    """
    The customer's tax IDs. Stripe Tax might consider a transaction with applicable tax IDs to be B2B, which might affect the tax calculation result. Stripe Tax doesn't validate tax IDs for correctness.
    """
    taxability_override: NotRequired[
        Literal["customer_exempt", "none", "reverse_charge"]
    ]
    """
    Overrides the tax calculation result to allow you to not collect tax from your customer. Use this if you've manually checked your customer's tax exemptions. Prefer providing the customer's `tax_ids` where possible, which automatically determines whether `reverse_charge` applies.
    """


class CalculationCreateParamsCustomerDetailsAddress(TypedDict):
    city: NotRequired["Literal['']|str"]
    """
    City, district, suburb, town, or village.
    """
    country: str
    """
    Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
    """
    line1: NotRequired["Literal['']|str"]
    """
    Address line 1, such as the street, PO Box, or company name.
    """
    line2: NotRequired["Literal['']|str"]
    """
    Address line 2, such as the apartment, suite, unit, or building.
    """
    postal_code: NotRequired["Literal['']|str"]
    """
    ZIP or postal code.
    """
    state: NotRequired["Literal['']|str"]
    """
    State, county, province, or region. We recommend sending [ISO 3166-2](https://en.wikipedia.org/wiki/ISO_3166-2) subdivision code value when possible.
    """


class CalculationCreateParamsCustomerDetailsTaxId(TypedDict):
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


class CalculationCreateParamsLineItem(TypedDict):
    amount: int
    """
    A positive integer representing the line item's total price in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal).
    If `tax_behavior=inclusive`, then this amount includes taxes. Otherwise, taxes are calculated on top of this amount.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    product: NotRequired[str]
    """
    If provided, the product's `tax_code` will be used as the line item's `tax_code`.
    """
    quantity: NotRequired[int]
    """
    The number of units of the item being purchased. Used to calculate the per-unit price from the total `amount` for the line. For example, if `amount=100` and `quantity=4`, the calculated unit price is 25.
    """
    reference: NotRequired[str]
    """
    A custom identifier for this line item, which must be unique across the line items in the calculation. The reference helps identify each line item in exported [tax reports](https://docs.stripe.com/tax/reports).
    """
    tax_behavior: NotRequired[Literal["exclusive", "inclusive"]]
    """
    Specifies whether the `amount` includes taxes. Defaults to `exclusive`.
    """
    tax_code: NotRequired[str]
    """
    A [tax code](https://docs.stripe.com/tax/tax-categories) ID to use for this line item. If not provided, we will use the tax code from the provided `product` param. If neither `tax_code` nor `product` is provided, we will use the default tax code from your Tax Settings.
    """


class CalculationCreateParamsShipFromDetails(TypedDict):
    address: "CalculationCreateParamsShipFromDetailsAddress"
    """
    The address from which the goods are being shipped from.
    """


class CalculationCreateParamsShipFromDetailsAddress(TypedDict):
    city: NotRequired["Literal['']|str"]
    """
    City, district, suburb, town, or village.
    """
    country: str
    """
    Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
    """
    line1: NotRequired["Literal['']|str"]
    """
    Address line 1, such as the street, PO Box, or company name.
    """
    line2: NotRequired["Literal['']|str"]
    """
    Address line 2, such as the apartment, suite, unit, or building.
    """
    postal_code: NotRequired["Literal['']|str"]
    """
    ZIP or postal code.
    """
    state: NotRequired["Literal['']|str"]
    """
    State/province as an [ISO 3166-2](https://en.wikipedia.org/wiki/ISO_3166-2) subdivision code, without country prefix, such as "NY" or "TX".
    """


class CalculationCreateParamsShippingCost(TypedDict):
    amount: NotRequired[int]
    """
    A positive integer in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal) representing the shipping charge. If `tax_behavior=inclusive`, then this amount includes taxes. Otherwise, taxes are calculated on top of this amount.
    """
    shipping_rate: NotRequired[str]
    """
    If provided, the [shipping rate](https://docs.stripe.com/api/shipping_rates/object)'s `amount`, `tax_code` and `tax_behavior` are used. If you provide a shipping rate, then you cannot pass the `amount`, `tax_code`, or `tax_behavior` parameters.
    """
    tax_behavior: NotRequired[Literal["exclusive", "inclusive"]]
    """
    Specifies whether the `amount` includes taxes. If `tax_behavior=inclusive`, then the amount includes taxes. Defaults to `exclusive`.
    """
    tax_code: NotRequired[str]
    """
    The [tax code](https://docs.stripe.com/tax/tax-categories) used to calculate tax on shipping. If not provided, the default shipping tax code from your [Tax Settings](https://dashboard.stripe.com/settings/tax) is used.
    """
