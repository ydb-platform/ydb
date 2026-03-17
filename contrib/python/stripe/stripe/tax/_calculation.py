# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._list_object import ListObject
from stripe._stripe_object import StripeObject
from stripe._util import class_method_variant, sanitize_id
from typing import ClassVar, List, Optional, cast, overload
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params.tax._calculation_create_params import (
        CalculationCreateParams,
    )
    from stripe.params.tax._calculation_list_line_items_params import (
        CalculationListLineItemsParams,
    )
    from stripe.params.tax._calculation_retrieve_params import (
        CalculationRetrieveParams,
    )
    from stripe.tax._calculation_line_item import CalculationLineItem


class Calculation(CreateableAPIResource["Calculation"]):
    """
    A Tax Calculation allows you to calculate the tax to collect from your customer.

    Related guide: [Calculate tax in your custom payment flow](https://docs.stripe.com/tax/custom)
    """

    OBJECT_NAME: ClassVar[Literal["tax.calculation"]] = "tax.calculation"

    class CustomerDetails(StripeObject):
        class Address(StripeObject):
            city: Optional[str]
            """
            City, district, suburb, town, or village.
            """
            country: str
            """
            Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
            """
            line1: Optional[str]
            """
            Address line 1, such as the street, PO Box, or company name.
            """
            line2: Optional[str]
            """
            Address line 2, such as the apartment, suite, unit, or building.
            """
            postal_code: Optional[str]
            """
            ZIP or postal code.
            """
            state: Optional[str]
            """
            State/province as an [ISO 3166-2](https://en.wikipedia.org/wiki/ISO_3166-2) subdivision code, without country prefix, such as "NY" or "TX".
            """

        class TaxId(StripeObject):
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
                "unknown",
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
            The type of the tax ID, one of `ad_nrt`, `ar_cuit`, `eu_vat`, `bo_tin`, `br_cnpj`, `br_cpf`, `cn_tin`, `co_nit`, `cr_tin`, `do_rcn`, `ec_ruc`, `eu_oss_vat`, `hr_oib`, `pe_ruc`, `ro_tin`, `rs_pib`, `sv_nit`, `uy_ruc`, `ve_rif`, `vn_tin`, `gb_vat`, `nz_gst`, `au_abn`, `au_arn`, `in_gst`, `no_vat`, `no_voec`, `za_vat`, `ch_vat`, `mx_rfc`, `sg_uen`, `ru_inn`, `ru_kpp`, `ca_bn`, `hk_br`, `es_cif`, `pl_nip`, `tw_vat`, `th_vat`, `jp_cn`, `jp_rn`, `jp_trn`, `li_uid`, `li_vat`, `lk_vat`, `my_itn`, `us_ein`, `kr_brn`, `ca_qst`, `ca_gst_hst`, `ca_pst_bc`, `ca_pst_mb`, `ca_pst_sk`, `my_sst`, `sg_gst`, `ae_trn`, `cl_tin`, `sa_vat`, `id_npwp`, `my_frp`, `il_vat`, `ge_vat`, `ua_vat`, `is_vat`, `bg_uic`, `hu_tin`, `si_tin`, `ke_pin`, `tr_tin`, `eg_tin`, `ph_tin`, `al_tin`, `bh_vat`, `kz_bin`, `ng_tin`, `om_vat`, `de_stn`, `ch_uid`, `tz_vat`, `uz_vat`, `uz_tin`, `md_vat`, `ma_vat`, `by_tin`, `ao_tin`, `bs_tin`, `bb_tin`, `cd_nif`, `mr_nif`, `me_pib`, `zw_tin`, `ba_tin`, `gn_nif`, `mk_vat`, `sr_fin`, `sn_ninea`, `am_tin`, `np_pan`, `tj_tin`, `ug_tin`, `zm_tin`, `kh_tin`, `aw_tin`, `az_tin`, `bd_bin`, `bj_ifu`, `et_tin`, `kg_tin`, `la_tin`, `cm_niu`, `cv_nif`, `bf_ifu`, or `unknown`
            """
            value: str
            """
            The value of the tax ID.
            """

        address: Optional[Address]
        """
        The customer's postal address (for example, home or business location).
        """
        address_source: Optional[Literal["billing", "shipping"]]
        """
        The type of customer address provided.
        """
        ip_address: Optional[str]
        """
        The customer's IP address (IPv4 or IPv6).
        """
        tax_ids: List[TaxId]
        """
        The customer's tax IDs (for example, EU VAT numbers).
        """
        taxability_override: Literal[
            "customer_exempt", "none", "reverse_charge"
        ]
        """
        The taxability override used for taxation.
        """
        _inner_class_types = {"address": Address, "tax_ids": TaxId}

    class ShipFromDetails(StripeObject):
        class Address(StripeObject):
            city: Optional[str]
            """
            City, district, suburb, town, or village.
            """
            country: str
            """
            Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
            """
            line1: Optional[str]
            """
            Address line 1, such as the street, PO Box, or company name.
            """
            line2: Optional[str]
            """
            Address line 2, such as the apartment, suite, unit, or building.
            """
            postal_code: Optional[str]
            """
            ZIP or postal code.
            """
            state: Optional[str]
            """
            State/province as an [ISO 3166-2](https://en.wikipedia.org/wiki/ISO_3166-2) subdivision code, without country prefix, such as "NY" or "TX".
            """

        address: Address
        _inner_class_types = {"address": Address}

    class ShippingCost(StripeObject):
        class TaxBreakdown(StripeObject):
            class Jurisdiction(StripeObject):
                country: str
                """
                Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
                """
                display_name: str
                """
                A human-readable name for the jurisdiction imposing the tax.
                """
                level: Literal[
                    "city", "country", "county", "district", "state"
                ]
                """
                Indicates the level of the jurisdiction imposing the tax.
                """
                state: Optional[str]
                """
                [ISO 3166-2 subdivision code](https://en.wikipedia.org/wiki/ISO_3166-2), without country prefix. For example, "NY" for New York, United States.
                """

            class TaxRateDetails(StripeObject):
                display_name: str
                """
                A localized display name for tax type, intended to be human-readable. For example, "Local Sales and Use Tax", "Value-added tax (VAT)", or "Umsatzsteuer (USt.)".
                """
                percentage_decimal: str
                """
                The tax rate percentage as a string. For example, 8.5% is represented as "8.5".
                """
                tax_type: Literal[
                    "amusement_tax",
                    "communications_tax",
                    "gst",
                    "hst",
                    "igst",
                    "jct",
                    "lease_tax",
                    "pst",
                    "qst",
                    "retail_delivery_fee",
                    "rst",
                    "sales_tax",
                    "service_tax",
                    "vat",
                ]
                """
                The tax type, such as `vat` or `sales_tax`.
                """

            amount: int
            """
            The amount of tax, in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal).
            """
            jurisdiction: Jurisdiction
            sourcing: Literal["destination", "origin"]
            """
            Indicates whether the jurisdiction was determined by the origin (merchant's address) or destination (customer's address).
            """
            tax_rate_details: Optional[TaxRateDetails]
            """
            Details regarding the rate for this tax. This field will be `null` when the tax is not imposed, for example if the product is exempt from tax.
            """
            taxability_reason: Literal[
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
            """
            The reasoning behind this tax, for example, if the product is tax exempt. The possible values for this field may be extended as new tax rules are supported.
            """
            taxable_amount: int
            """
            The amount on which tax is calculated, in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal).
            """
            _inner_class_types = {
                "jurisdiction": Jurisdiction,
                "tax_rate_details": TaxRateDetails,
            }

        amount: int
        """
        The shipping amount in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal). If `tax_behavior=inclusive`, then this amount includes taxes. Otherwise, taxes were calculated on top of this amount.
        """
        amount_tax: int
        """
        The amount of tax calculated for shipping, in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal).
        """
        shipping_rate: Optional[str]
        """
        The ID of an existing [ShippingRate](https://docs.stripe.com/api/shipping_rates/object).
        """
        tax_behavior: Literal["exclusive", "inclusive"]
        """
        Specifies whether the `amount` includes taxes. If `tax_behavior=inclusive`, then the amount includes taxes.
        """
        tax_breakdown: Optional[List[TaxBreakdown]]
        """
        Detailed account of taxes relevant to shipping cost.
        """
        tax_code: str
        """
        The [tax code](https://docs.stripe.com/tax/tax-categories) ID used for shipping.
        """
        _inner_class_types = {"tax_breakdown": TaxBreakdown}

    class TaxBreakdown(StripeObject):
        class TaxRateDetails(StripeObject):
            class FlatAmount(StripeObject):
                amount: int
                """
                Amount of the tax when the `rate_type` is `flat_amount`. This positive integer represents how much to charge in the smallest currency unit (e.g., 100 cents to charge $1.00 or 100 to charge ¥100, a zero-decimal currency). The amount value supports up to eight digits (e.g., a value of 99999999 for a USD charge of $999,999.99).
                """
                currency: str
                """
                Three-letter ISO currency code, in lowercase.
                """

            country: Optional[str]
            """
            Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
            """
            flat_amount: Optional[FlatAmount]
            """
            The amount of the tax rate when the `rate_type` is `flat_amount`. Tax rates with `rate_type` `percentage` can vary based on the transaction, resulting in this field being `null`. This field exposes the amount and currency of the flat tax rate.
            """
            percentage_decimal: str
            """
            The tax rate percentage as a string. For example, 8.5% is represented as `"8.5"`.
            """
            rate_type: Optional[Literal["flat_amount", "percentage"]]
            """
            Indicates the type of tax rate applied to the taxable amount. This value can be `null` when no tax applies to the location. This field is only present for TaxRates created by Stripe Tax.
            """
            state: Optional[str]
            """
            State, county, province, or region ([ISO 3166-2](https://en.wikipedia.org/wiki/ISO_3166-2)).
            """
            tax_type: Optional[
                Literal[
                    "amusement_tax",
                    "communications_tax",
                    "gst",
                    "hst",
                    "igst",
                    "jct",
                    "lease_tax",
                    "pst",
                    "qst",
                    "retail_delivery_fee",
                    "rst",
                    "sales_tax",
                    "service_tax",
                    "vat",
                ]
            ]
            """
            The tax type, such as `vat` or `sales_tax`.
            """
            _inner_class_types = {"flat_amount": FlatAmount}

        amount: int
        """
        The amount of tax, in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal).
        """
        inclusive: bool
        """
        Specifies whether the tax amount is included in the line item amount.
        """
        tax_rate_details: TaxRateDetails
        taxability_reason: Literal[
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
        """
        The reasoning behind this tax, for example, if the product is tax exempt. We might extend the possible values for this field to support new tax rules.
        """
        taxable_amount: int
        """
        The amount on which tax is calculated, in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal).
        """
        _inner_class_types = {"tax_rate_details": TaxRateDetails}

    amount_total: int
    """
    Total amount after taxes in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal).
    """
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    customer: Optional[str]
    """
    The ID of an existing [Customer](https://docs.stripe.com/api/customers/object) used for the resource.
    """
    customer_details: CustomerDetails
    expires_at: Optional[int]
    """
    Timestamp of date at which the tax calculation will expire.
    """
    id: Optional[str]
    """
    Unique identifier for the calculation.
    """
    line_items: Optional[ListObject["CalculationLineItem"]]
    """
    The list of items the customer is purchasing.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    object: Literal["tax.calculation"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    ship_from_details: Optional[ShipFromDetails]
    """
    The details of the ship from location, such as the address.
    """
    shipping_cost: Optional[ShippingCost]
    """
    The shipping cost details for the calculation.
    """
    tax_amount_exclusive: int
    """
    The amount of tax to be collected on top of the line item prices.
    """
    tax_amount_inclusive: int
    """
    The amount of tax already included in the line item prices.
    """
    tax_breakdown: List[TaxBreakdown]
    """
    Breakdown of individual tax amounts that add up to the total.
    """
    tax_date: int
    """
    Timestamp of date at which the tax rules and rates in effect applies for the calculation.
    """

    @classmethod
    def create(
        cls, **params: Unpack["CalculationCreateParams"]
    ) -> "Calculation":
        """
        Calculates tax based on the input and returns a Tax Calculation object.
        """
        return cast(
            "Calculation",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["CalculationCreateParams"]
    ) -> "Calculation":
        """
        Calculates tax based on the input and returns a Tax Calculation object.
        """
        return cast(
            "Calculation",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def _cls_list_line_items(
        cls,
        calculation: str,
        **params: Unpack["CalculationListLineItemsParams"],
    ) -> ListObject["CalculationLineItem"]:
        """
        Retrieves the line items of a tax calculation as a collection, if the calculation hasn't expired.
        """
        return cast(
            ListObject["CalculationLineItem"],
            cls._static_request(
                "get",
                "/v1/tax/calculations/{calculation}/line_items".format(
                    calculation=sanitize_id(calculation)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def list_line_items(
        calculation: str, **params: Unpack["CalculationListLineItemsParams"]
    ) -> ListObject["CalculationLineItem"]:
        """
        Retrieves the line items of a tax calculation as a collection, if the calculation hasn't expired.
        """
        ...

    @overload
    def list_line_items(
        self, **params: Unpack["CalculationListLineItemsParams"]
    ) -> ListObject["CalculationLineItem"]:
        """
        Retrieves the line items of a tax calculation as a collection, if the calculation hasn't expired.
        """
        ...

    @class_method_variant("_cls_list_line_items")
    def list_line_items(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["CalculationListLineItemsParams"]
    ) -> ListObject["CalculationLineItem"]:
        """
        Retrieves the line items of a tax calculation as a collection, if the calculation hasn't expired.
        """
        return cast(
            ListObject["CalculationLineItem"],
            self._request(
                "get",
                "/v1/tax/calculations/{calculation}/line_items".format(
                    calculation=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_list_line_items_async(
        cls,
        calculation: str,
        **params: Unpack["CalculationListLineItemsParams"],
    ) -> ListObject["CalculationLineItem"]:
        """
        Retrieves the line items of a tax calculation as a collection, if the calculation hasn't expired.
        """
        return cast(
            ListObject["CalculationLineItem"],
            await cls._static_request_async(
                "get",
                "/v1/tax/calculations/{calculation}/line_items".format(
                    calculation=sanitize_id(calculation)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def list_line_items_async(
        calculation: str, **params: Unpack["CalculationListLineItemsParams"]
    ) -> ListObject["CalculationLineItem"]:
        """
        Retrieves the line items of a tax calculation as a collection, if the calculation hasn't expired.
        """
        ...

    @overload
    async def list_line_items_async(
        self, **params: Unpack["CalculationListLineItemsParams"]
    ) -> ListObject["CalculationLineItem"]:
        """
        Retrieves the line items of a tax calculation as a collection, if the calculation hasn't expired.
        """
        ...

    @class_method_variant("_cls_list_line_items_async")
    async def list_line_items_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["CalculationListLineItemsParams"]
    ) -> ListObject["CalculationLineItem"]:
        """
        Retrieves the line items of a tax calculation as a collection, if the calculation hasn't expired.
        """
        return cast(
            ListObject["CalculationLineItem"],
            await self._request_async(
                "get",
                "/v1/tax/calculations/{calculation}/line_items".format(
                    calculation=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["CalculationRetrieveParams"]
    ) -> "Calculation":
        """
        Retrieves a Tax Calculation object, if the calculation hasn't expired.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["CalculationRetrieveParams"]
    ) -> "Calculation":
        """
        Retrieves a Tax Calculation object, if the calculation hasn't expired.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    _inner_class_types = {
        "customer_details": CustomerDetails,
        "ship_from_details": ShipFromDetails,
        "shipping_cost": ShippingCost,
        "tax_breakdown": TaxBreakdown,
    }
