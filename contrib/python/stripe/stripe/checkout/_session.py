# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._expandable_field import ExpandableField
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._stripe_object import StripeObject
from stripe._updateable_api_resource import UpdateableAPIResource
from stripe._util import class_method_variant, sanitize_id
from typing import ClassVar, Dict, List, Optional, cast, overload
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._account import Account
    from stripe._coupon import Coupon
    from stripe._customer import Customer
    from stripe._discount import Discount as DiscountResource
    from stripe._invoice import Invoice
    from stripe._line_item import LineItem
    from stripe._payment_intent import PaymentIntent
    from stripe._payment_link import PaymentLink
    from stripe._promotion_code import PromotionCode
    from stripe._setup_intent import SetupIntent
    from stripe._shipping_rate import ShippingRate
    from stripe._subscription import Subscription
    from stripe._tax_id import TaxId as TaxIdResource
    from stripe._tax_rate import TaxRate
    from stripe.params.checkout._session_create_params import (
        SessionCreateParams,
    )
    from stripe.params.checkout._session_expire_params import (
        SessionExpireParams,
    )
    from stripe.params.checkout._session_list_line_items_params import (
        SessionListLineItemsParams,
    )
    from stripe.params.checkout._session_list_params import SessionListParams
    from stripe.params.checkout._session_modify_params import (
        SessionModifyParams,
    )
    from stripe.params.checkout._session_retrieve_params import (
        SessionRetrieveParams,
    )


class Session(
    CreateableAPIResource["Session"],
    ListableAPIResource["Session"],
    UpdateableAPIResource["Session"],
):
    """
    A Checkout Session represents your customer's session as they pay for
    one-time purchases or subscriptions through [Checkout](https://docs.stripe.com/payments/checkout)
    or [Payment Links](https://docs.stripe.com/payments/payment-links). We recommend creating a
    new Session each time your customer attempts to pay.

    Once payment is successful, the Checkout Session will contain a reference
    to the [Customer](https://docs.stripe.com/api/customers), and either the successful
    [PaymentIntent](https://docs.stripe.com/api/payment_intents) or an active
    [Subscription](https://docs.stripe.com/api/subscriptions).

    You can create a Checkout Session on your server and redirect to its URL
    to begin Checkout.

    Related guide: [Checkout quickstart](https://docs.stripe.com/checkout/quickstart)
    """

    OBJECT_NAME: ClassVar[Literal["checkout.session"]] = "checkout.session"

    class AdaptivePricing(StripeObject):
        enabled: bool
        """
        If enabled, Adaptive Pricing is available on [eligible sessions](https://docs.stripe.com/payments/currencies/localize-prices/adaptive-pricing?payment-ui=stripe-hosted#restrictions).
        """

    class AfterExpiration(StripeObject):
        class Recovery(StripeObject):
            allow_promotion_codes: bool
            """
            Enables user redeemable promotion codes on the recovered Checkout Sessions. Defaults to `false`
            """
            enabled: bool
            """
            If `true`, a recovery url will be generated to recover this Checkout Session if it
            expires before a transaction is completed. It will be attached to the
            Checkout Session object upon expiration.
            """
            expires_at: Optional[int]
            """
            The timestamp at which the recovery URL will expire.
            """
            url: Optional[str]
            """
            URL that creates a new Checkout Session when clicked that is a copy of this expired Checkout Session
            """

        recovery: Optional[Recovery]
        """
        When set, configuration used to recover the Checkout Session on expiry.
        """
        _inner_class_types = {"recovery": Recovery}

    class AutomaticTax(StripeObject):
        class Liability(StripeObject):
            account: Optional[ExpandableField["Account"]]
            """
            The connected account being referenced when `type` is `account`.
            """
            type: Literal["account", "self"]
            """
            Type of the account referenced.
            """

        enabled: bool
        """
        Indicates whether automatic tax is enabled for the session
        """
        liability: Optional[Liability]
        """
        The account that's liable for tax. If set, the business address and tax registrations required to perform the tax calculation are loaded from this account. The tax transaction is returned in the report of the connected account.
        """
        provider: Optional[str]
        """
        The tax provider powering automatic tax.
        """
        status: Optional[
            Literal["complete", "failed", "requires_location_inputs"]
        ]
        """
        The status of the most recent automated tax calculation for this session.
        """
        _inner_class_types = {"liability": Liability}

    class BrandingSettings(StripeObject):
        class Icon(StripeObject):
            file: Optional[str]
            """
            The ID of a [File upload](https://stripe.com/docs/api/files) representing the icon. Purpose must be `business_icon`. Required if `type` is `file` and disallowed otherwise.
            """
            type: Literal["file", "url"]
            """
            The type of image for the icon. Must be one of `file` or `url`.
            """
            url: Optional[str]
            """
            The URL of the image. Present when `type` is `url`.
            """

        class Logo(StripeObject):
            file: Optional[str]
            """
            The ID of a [File upload](https://stripe.com/docs/api/files) representing the logo. Purpose must be `business_logo`. Required if `type` is `file` and disallowed otherwise.
            """
            type: Literal["file", "url"]
            """
            The type of image for the logo. Must be one of `file` or `url`.
            """
            url: Optional[str]
            """
            The URL of the image. Present when `type` is `url`.
            """

        background_color: str
        """
        A hex color value starting with `#` representing the background color for the Checkout Session.
        """
        border_style: Literal["pill", "rectangular", "rounded"]
        """
        The border style for the Checkout Session. Must be one of `rounded`, `rectangular`, or `pill`.
        """
        button_color: str
        """
        A hex color value starting with `#` representing the button color for the Checkout Session.
        """
        display_name: str
        """
        The display name shown on the Checkout Session.
        """
        font_family: str
        """
        The font family for the Checkout Session. Must be one of the [supported font families](https://docs.stripe.com/payments/checkout/customization/appearance?payment-ui=stripe-hosted#font-compatibility).
        """
        icon: Optional[Icon]
        """
        The icon for the Checkout Session. You cannot set both `logo` and `icon`.
        """
        logo: Optional[Logo]
        """
        The logo for the Checkout Session. You cannot set both `logo` and `icon`.
        """
        _inner_class_types = {"icon": Icon, "logo": Logo}

    class CollectedInformation(StripeObject):
        class ShippingDetails(StripeObject):
            class Address(StripeObject):
                city: Optional[str]
                """
                City, district, suburb, town, or village.
                """
                country: Optional[str]
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
                State, county, province, or region ([ISO 3166-2](https://en.wikipedia.org/wiki/ISO_3166-2)).
                """

            address: Address
            name: str
            """
            Customer name.
            """
            _inner_class_types = {"address": Address}

        business_name: Optional[str]
        """
        Customer's business name for this Checkout Session
        """
        individual_name: Optional[str]
        """
        Customer's individual name for this Checkout Session
        """
        shipping_details: Optional[ShippingDetails]
        """
        Shipping information for this Checkout Session.
        """
        _inner_class_types = {"shipping_details": ShippingDetails}

    class Consent(StripeObject):
        promotions: Optional[Literal["opt_in", "opt_out"]]
        """
        If `opt_in`, the customer consents to receiving promotional communications
        from the merchant about this Checkout Session.
        """
        terms_of_service: Optional[Literal["accepted"]]
        """
        If `accepted`, the customer in this Checkout Session has agreed to the merchant's terms of service.
        """

    class ConsentCollection(StripeObject):
        class PaymentMethodReuseAgreement(StripeObject):
            position: Literal["auto", "hidden"]
            """
            Determines the position and visibility of the payment method reuse agreement in the UI. When set to `auto`, Stripe's defaults will be used.

            When set to `hidden`, the payment method reuse agreement text will always be hidden in the UI.
            """

        payment_method_reuse_agreement: Optional[PaymentMethodReuseAgreement]
        """
        If set to `hidden`, it will hide legal text related to the reuse of a payment method.
        """
        promotions: Optional[Literal["auto", "none"]]
        """
        If set to `auto`, enables the collection of customer consent for promotional communications. The Checkout
        Session will determine whether to display an option to opt into promotional communication
        from the merchant depending on the customer's locale. Only available to US merchants.
        """
        terms_of_service: Optional[Literal["none", "required"]]
        """
        If set to `required`, it requires customers to accept the terms of service before being able to pay.
        """
        _inner_class_types = {
            "payment_method_reuse_agreement": PaymentMethodReuseAgreement,
        }

    class CurrencyConversion(StripeObject):
        amount_subtotal: int
        """
        Total of all items in source currency before discounts or taxes are applied.
        """
        amount_total: int
        """
        Total of all items in source currency after discounts and taxes are applied.
        """
        fx_rate: str
        """
        Exchange rate used to convert source currency amounts to customer currency amounts
        """
        source_currency: str
        """
        Creation currency of the CheckoutSession before localization
        """

    class CustomField(StripeObject):
        class Dropdown(StripeObject):
            class Option(StripeObject):
                label: str
                """
                The label for the option, displayed to the customer. Up to 100 characters.
                """
                value: str
                """
                The value for this option, not displayed to the customer, used by your integration to reconcile the option selected by the customer. Must be unique to this option, alphanumeric, and up to 100 characters.
                """

            default_value: Optional[str]
            """
            The value that pre-fills on the payment page.
            """
            options: List[Option]
            """
            The options available for the customer to select. Up to 200 options allowed.
            """
            value: Optional[str]
            """
            The option selected by the customer. This will be the `value` for the option.
            """
            _inner_class_types = {"options": Option}

        class Label(StripeObject):
            custom: Optional[str]
            """
            Custom text for the label, displayed to the customer. Up to 50 characters.
            """
            type: Literal["custom"]
            """
            The type of the label.
            """

        class Numeric(StripeObject):
            default_value: Optional[str]
            """
            The value that pre-fills the field on the payment page.
            """
            maximum_length: Optional[int]
            """
            The maximum character length constraint for the customer's input.
            """
            minimum_length: Optional[int]
            """
            The minimum character length requirement for the customer's input.
            """
            value: Optional[str]
            """
            The value entered by the customer, containing only digits.
            """

        class Text(StripeObject):
            default_value: Optional[str]
            """
            The value that pre-fills the field on the payment page.
            """
            maximum_length: Optional[int]
            """
            The maximum character length constraint for the customer's input.
            """
            minimum_length: Optional[int]
            """
            The minimum character length requirement for the customer's input.
            """
            value: Optional[str]
            """
            The value entered by the customer.
            """

        dropdown: Optional[Dropdown]
        key: str
        """
        String of your choice that your integration can use to reconcile this field. Must be unique to this field, alphanumeric, and up to 200 characters.
        """
        label: Label
        numeric: Optional[Numeric]
        optional: bool
        """
        Whether the customer is required to complete the field before completing the Checkout Session. Defaults to `false`.
        """
        text: Optional[Text]
        type: Literal["dropdown", "numeric", "text"]
        """
        The type of the field.
        """
        _inner_class_types = {
            "dropdown": Dropdown,
            "label": Label,
            "numeric": Numeric,
            "text": Text,
        }

    class CustomText(StripeObject):
        class AfterSubmit(StripeObject):
            message: str
            """
            Text can be up to 1200 characters in length.
            """

        class ShippingAddress(StripeObject):
            message: str
            """
            Text can be up to 1200 characters in length.
            """

        class Submit(StripeObject):
            message: str
            """
            Text can be up to 1200 characters in length.
            """

        class TermsOfServiceAcceptance(StripeObject):
            message: str
            """
            Text can be up to 1200 characters in length.
            """

        after_submit: Optional[AfterSubmit]
        """
        Custom text that should be displayed after the payment confirmation button.
        """
        shipping_address: Optional[ShippingAddress]
        """
        Custom text that should be displayed alongside shipping address collection.
        """
        submit: Optional[Submit]
        """
        Custom text that should be displayed alongside the payment confirmation button.
        """
        terms_of_service_acceptance: Optional[TermsOfServiceAcceptance]
        """
        Custom text that should be displayed in place of the default terms of service agreement text.
        """
        _inner_class_types = {
            "after_submit": AfterSubmit,
            "shipping_address": ShippingAddress,
            "submit": Submit,
            "terms_of_service_acceptance": TermsOfServiceAcceptance,
        }

    class CustomerDetails(StripeObject):
        class Address(StripeObject):
            city: Optional[str]
            """
            City, district, suburb, town, or village.
            """
            country: Optional[str]
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
            State, county, province, or region ([ISO 3166-2](https://en.wikipedia.org/wiki/ISO_3166-2)).
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
            value: Optional[str]
            """
            The value of the tax ID.
            """

        address: Optional[Address]
        """
        The customer's address after a completed Checkout Session. Note: This property is populated only for sessions on or after March 30, 2022.
        """
        business_name: Optional[str]
        """
        The customer's business name after a completed Checkout Session.
        """
        email: Optional[str]
        """
        The email associated with the Customer, if one exists, on the Checkout Session after a completed Checkout Session or at time of session expiry.
        Otherwise, if the customer has consented to promotional content, this value is the most recent valid email provided by the customer on the Checkout form.
        """
        individual_name: Optional[str]
        """
        The customer's individual name after a completed Checkout Session.
        """
        name: Optional[str]
        """
        The customer's name after a completed Checkout Session. Note: This property is populated only for sessions on or after March 30, 2022.
        """
        phone: Optional[str]
        """
        The customer's phone number after a completed Checkout Session.
        """
        tax_exempt: Optional[Literal["exempt", "none", "reverse"]]
        """
        The customer's tax exempt status after a completed Checkout Session.
        """
        tax_ids: Optional[List[TaxId]]
        """
        The customer's tax IDs after a completed Checkout Session.
        """
        _inner_class_types = {"address": Address, "tax_ids": TaxId}

    class Discount(StripeObject):
        coupon: Optional[ExpandableField["Coupon"]]
        """
        Coupon attached to the Checkout Session.
        """
        promotion_code: Optional[ExpandableField["PromotionCode"]]
        """
        Promotion code attached to the Checkout Session.
        """

    class InvoiceCreation(StripeObject):
        class InvoiceData(StripeObject):
            class CustomField(StripeObject):
                name: str
                """
                The name of the custom field.
                """
                value: str
                """
                The value of the custom field.
                """

            class Issuer(StripeObject):
                account: Optional[ExpandableField["Account"]]
                """
                The connected account being referenced when `type` is `account`.
                """
                type: Literal["account", "self"]
                """
                Type of the account referenced.
                """

            class RenderingOptions(StripeObject):
                amount_tax_display: Optional[str]
                """
                How line-item prices and amounts will be displayed with respect to tax on invoice PDFs.
                """
                template: Optional[str]
                """
                ID of the invoice rendering template to be used for the generated invoice.
                """

            account_tax_ids: Optional[List[ExpandableField["TaxIdResource"]]]
            """
            The account tax IDs associated with the invoice.
            """
            custom_fields: Optional[List[CustomField]]
            """
            Custom fields displayed on the invoice.
            """
            description: Optional[str]
            """
            An arbitrary string attached to the object. Often useful for displaying to users.
            """
            footer: Optional[str]
            """
            Footer displayed on the invoice.
            """
            issuer: Optional[Issuer]
            """
            The connected account that issues the invoice. The invoice is presented with the branding and support information of the specified account.
            """
            metadata: Optional[Dict[str, str]]
            """
            Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
            """
            rendering_options: Optional[RenderingOptions]
            """
            Options for invoice PDF rendering.
            """
            _inner_class_types = {
                "custom_fields": CustomField,
                "issuer": Issuer,
                "rendering_options": RenderingOptions,
            }

        enabled: bool
        """
        Indicates whether invoice creation is enabled for the Checkout Session.
        """
        invoice_data: InvoiceData
        _inner_class_types = {"invoice_data": InvoiceData}

    class NameCollection(StripeObject):
        class Business(StripeObject):
            enabled: bool
            """
            Indicates whether business name collection is enabled for the session
            """
            optional: bool
            """
            Whether the customer is required to complete the field before completing the Checkout Session. Defaults to `false`.
            """

        class Individual(StripeObject):
            enabled: bool
            """
            Indicates whether individual name collection is enabled for the session
            """
            optional: bool
            """
            Whether the customer is required to complete the field before completing the Checkout Session. Defaults to `false`.
            """

        business: Optional[Business]
        individual: Optional[Individual]
        _inner_class_types = {"business": Business, "individual": Individual}

    class OptionalItem(StripeObject):
        class AdjustableQuantity(StripeObject):
            enabled: bool
            """
            Set to true if the quantity can be adjusted to any non-negative integer.
            """
            maximum: Optional[int]
            """
            The maximum quantity of this item the customer can purchase. By default this value is 99. You can specify a value up to 999999.
            """
            minimum: Optional[int]
            """
            The minimum quantity of this item the customer must purchase, if they choose to purchase it. Because this item is optional, the customer will always be able to remove it from their order, even if the `minimum` configured here is greater than 0. By default this value is 0.
            """

        adjustable_quantity: Optional[AdjustableQuantity]
        price: str
        quantity: int
        _inner_class_types = {"adjustable_quantity": AdjustableQuantity}

    class PaymentMethodConfigurationDetails(StripeObject):
        id: str
        """
        ID of the payment method configuration used.
        """
        parent: Optional[str]
        """
        ID of the parent payment method configuration used.
        """

    class PaymentMethodOptions(StripeObject):
        class AcssDebit(StripeObject):
            class MandateOptions(StripeObject):
                custom_mandate_url: Optional[str]
                """
                A URL for custom mandate text
                """
                default_for: Optional[List[Literal["invoice", "subscription"]]]
                """
                List of Stripe products where this mandate can be selected automatically. Returned when the Session is in `setup` mode.
                """
                interval_description: Optional[str]
                """
                Description of the interval. Only required if the 'payment_schedule' parameter is 'interval' or 'combined'.
                """
                payment_schedule: Optional[
                    Literal["combined", "interval", "sporadic"]
                ]
                """
                Payment schedule for the mandate.
                """
                transaction_type: Optional[Literal["business", "personal"]]
                """
                Transaction type of the mandate.
                """

            currency: Optional[Literal["cad", "usd"]]
            """
            Currency supported by the bank account. Returned when the Session is in `setup` mode.
            """
            mandate_options: Optional[MandateOptions]
            setup_future_usage: Optional[
                Literal["none", "off_session", "on_session"]
            ]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
            """
            target_date: Optional[str]
            """
            Controls when Stripe will attempt to debit the funds from the customer's account. The date must be a string in YYYY-MM-DD format. The date must be in the future and between 3 and 15 calendar days from now.
            """
            verification_method: Optional[
                Literal["automatic", "instant", "microdeposits"]
            ]
            """
            Bank account verification method.
            """
            _inner_class_types = {"mandate_options": MandateOptions}

        class Affirm(StripeObject):
            capture_method: Optional[Literal["manual"]]
            """
            Controls when the funds will be captured from the customer's account.
            """
            setup_future_usage: Optional[Literal["none"]]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
            """

        class AfterpayClearpay(StripeObject):
            capture_method: Optional[Literal["manual"]]
            """
            Controls when the funds will be captured from the customer's account.
            """
            setup_future_usage: Optional[Literal["none"]]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
            """

        class Alipay(StripeObject):
            setup_future_usage: Optional[Literal["none"]]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
            """

        class Alma(StripeObject):
            capture_method: Optional[Literal["manual"]]
            """
            Controls when the funds will be captured from the customer's account.
            """

        class AmazonPay(StripeObject):
            capture_method: Optional[Literal["manual"]]
            """
            Controls when the funds will be captured from the customer's account.
            """
            setup_future_usage: Optional[Literal["none", "off_session"]]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
            """

        class AuBecsDebit(StripeObject):
            setup_future_usage: Optional[Literal["none"]]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
            """
            target_date: Optional[str]
            """
            Controls when Stripe will attempt to debit the funds from the customer's account. The date must be a string in YYYY-MM-DD format. The date must be in the future and between 3 and 15 calendar days from now.
            """

        class BacsDebit(StripeObject):
            class MandateOptions(StripeObject):
                reference_prefix: Optional[str]
                """
                Prefix used to generate the Mandate reference. Must be at most 12 characters long. Must consist of only uppercase letters, numbers, spaces, or the following special characters: '/', '_', '-', '&', '.'. Cannot begin with 'DDIC' or 'STRIPE'.
                """

            mandate_options: Optional[MandateOptions]
            setup_future_usage: Optional[
                Literal["none", "off_session", "on_session"]
            ]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
            """
            target_date: Optional[str]
            """
            Controls when Stripe will attempt to debit the funds from the customer's account. The date must be a string in YYYY-MM-DD format. The date must be in the future and between 3 and 15 calendar days from now.
            """
            _inner_class_types = {"mandate_options": MandateOptions}

        class Bancontact(StripeObject):
            setup_future_usage: Optional[Literal["none"]]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
            """

        class Billie(StripeObject):
            capture_method: Optional[Literal["manual"]]
            """
            Controls when the funds will be captured from the customer's account.
            """

        class Boleto(StripeObject):
            expires_after_days: int
            """
            The number of calendar days before a Boleto voucher expires. For example, if you create a Boleto voucher on Monday and you set expires_after_days to 2, the Boleto voucher will expire on Wednesday at 23:59 America/Sao_Paulo time.
            """
            setup_future_usage: Optional[
                Literal["none", "off_session", "on_session"]
            ]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
            """

        class Card(StripeObject):
            class Installments(StripeObject):
                enabled: Optional[bool]
                """
                Indicates if installments are enabled
                """

            class Restrictions(StripeObject):
                brands_blocked: Optional[
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

            capture_method: Optional[Literal["manual"]]
            """
            Controls when the funds will be captured from the customer's account.
            """
            installments: Optional[Installments]
            request_extended_authorization: Optional[
                Literal["if_available", "never"]
            ]
            """
            Request ability to [capture beyond the standard authorization validity window](https://docs.stripe.com/payments/extended-authorization) for this CheckoutSession.
            """
            request_incremental_authorization: Optional[
                Literal["if_available", "never"]
            ]
            """
            Request ability to [increment the authorization](https://docs.stripe.com/payments/incremental-authorization) for this CheckoutSession.
            """
            request_multicapture: Optional[Literal["if_available", "never"]]
            """
            Request ability to make [multiple captures](https://docs.stripe.com/payments/multicapture) for this CheckoutSession.
            """
            request_overcapture: Optional[Literal["if_available", "never"]]
            """
            Request ability to [overcapture](https://docs.stripe.com/payments/overcapture) for this CheckoutSession.
            """
            request_three_d_secure: Literal["any", "automatic", "challenge"]
            """
            We strongly recommend that you rely on our SCA Engine to automatically prompt your customers for authentication based on risk level and [other requirements](https://docs.stripe.com/strong-customer-authentication). However, if you wish to request 3D Secure based on logic from your own fraud engine, provide this option. If not provided, this value defaults to `automatic`. Read our guide on [manually requesting 3D Secure](https://docs.stripe.com/payments/3d-secure/authentication-flow#manual-three-ds) for more information on how this configuration interacts with Radar and our SCA Engine.
            """
            restrictions: Optional[Restrictions]
            setup_future_usage: Optional[
                Literal["none", "off_session", "on_session"]
            ]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
            """
            statement_descriptor_suffix_kana: Optional[str]
            """
            Provides information about a card payment that customers see on their statements. Concatenated with the Kana prefix (shortened Kana descriptor) or Kana statement descriptor that's set on the account to form the complete statement descriptor. Maximum 22 characters. On card statements, the *concatenation* of both prefix and suffix (including separators) will appear truncated to 22 characters.
            """
            statement_descriptor_suffix_kanji: Optional[str]
            """
            Provides information about a card payment that customers see on their statements. Concatenated with the Kanji prefix (shortened Kanji descriptor) or Kanji statement descriptor that's set on the account to form the complete statement descriptor. Maximum 17 characters. On card statements, the *concatenation* of both prefix and suffix (including separators) will appear truncated to 17 characters.
            """
            _inner_class_types = {
                "installments": Installments,
                "restrictions": Restrictions,
            }

        class Cashapp(StripeObject):
            capture_method: Optional[Literal["manual"]]
            """
            Controls when the funds will be captured from the customer's account.
            """
            setup_future_usage: Optional[Literal["none"]]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
            """

        class CustomerBalance(StripeObject):
            class BankTransfer(StripeObject):
                class EuBankTransfer(StripeObject):
                    country: Literal["BE", "DE", "ES", "FR", "IE", "NL"]
                    """
                    The desired country code of the bank account information. Permitted values include: `DE`, `FR`, `IE`, or `NL`.
                    """

                eu_bank_transfer: Optional[EuBankTransfer]
                requested_address_types: Optional[
                    List[
                        Literal[
                            "aba",
                            "iban",
                            "sepa",
                            "sort_code",
                            "spei",
                            "swift",
                            "zengin",
                        ]
                    ]
                ]
                """
                List of address types that should be returned in the financial_addresses response. If not specified, all valid types will be returned.

                Permitted values include: `sort_code`, `zengin`, `iban`, or `spei`.
                """
                type: Optional[
                    Literal[
                        "eu_bank_transfer",
                        "gb_bank_transfer",
                        "jp_bank_transfer",
                        "mx_bank_transfer",
                        "us_bank_transfer",
                    ]
                ]
                """
                The bank transfer type that this PaymentIntent is allowed to use for funding Permitted values include: `eu_bank_transfer`, `gb_bank_transfer`, `jp_bank_transfer`, `mx_bank_transfer`, or `us_bank_transfer`.
                """
                _inner_class_types = {"eu_bank_transfer": EuBankTransfer}

            bank_transfer: Optional[BankTransfer]
            funding_type: Optional[Literal["bank_transfer"]]
            """
            The funding method type to be used when there are not enough funds in the customer balance. Permitted values include: `bank_transfer`.
            """
            setup_future_usage: Optional[Literal["none"]]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
            """
            _inner_class_types = {"bank_transfer": BankTransfer}

        class Eps(StripeObject):
            setup_future_usage: Optional[Literal["none"]]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
            """

        class Fpx(StripeObject):
            setup_future_usage: Optional[Literal["none"]]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
            """

        class Giropay(StripeObject):
            setup_future_usage: Optional[Literal["none"]]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
            """

        class Grabpay(StripeObject):
            setup_future_usage: Optional[Literal["none"]]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
            """

        class Ideal(StripeObject):
            setup_future_usage: Optional[Literal["none"]]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
            """

        class KakaoPay(StripeObject):
            capture_method: Optional[Literal["manual"]]
            """
            Controls when the funds will be captured from the customer's account.
            """
            setup_future_usage: Optional[Literal["none", "off_session"]]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
            """

        class Klarna(StripeObject):
            capture_method: Optional[Literal["manual"]]
            """
            Controls when the funds will be captured from the customer's account.
            """
            setup_future_usage: Optional[
                Literal["none", "off_session", "on_session"]
            ]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
            """

        class Konbini(StripeObject):
            expires_after_days: Optional[int]
            """
            The number of calendar days (between 1 and 60) after which Konbini payment instructions will expire. For example, if a PaymentIntent is confirmed with Konbini and `expires_after_days` set to 2 on Monday JST, the instructions will expire on Wednesday 23:59:59 JST.
            """
            setup_future_usage: Optional[Literal["none"]]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
            """

        class KrCard(StripeObject):
            capture_method: Optional[Literal["manual"]]
            """
            Controls when the funds will be captured from the customer's account.
            """
            setup_future_usage: Optional[Literal["none", "off_session"]]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
            """

        class Link(StripeObject):
            capture_method: Optional[Literal["manual"]]
            """
            Controls when the funds will be captured from the customer's account.
            """
            setup_future_usage: Optional[Literal["none", "off_session"]]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
            """

        class Mobilepay(StripeObject):
            capture_method: Optional[Literal["manual"]]
            """
            Controls when the funds will be captured from the customer's account.
            """
            setup_future_usage: Optional[Literal["none"]]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
            """

        class Multibanco(StripeObject):
            setup_future_usage: Optional[Literal["none"]]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
            """

        class NaverPay(StripeObject):
            capture_method: Optional[Literal["manual"]]
            """
            Controls when the funds will be captured from the customer's account.
            """
            setup_future_usage: Optional[Literal["none", "off_session"]]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
            """

        class Oxxo(StripeObject):
            expires_after_days: int
            """
            The number of calendar days before an OXXO invoice expires. For example, if you create an OXXO invoice on Monday and you set expires_after_days to 2, the OXXO invoice will expire on Wednesday at 23:59 America/Mexico_City time.
            """
            setup_future_usage: Optional[Literal["none"]]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
            """

        class P24(StripeObject):
            setup_future_usage: Optional[Literal["none"]]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
            """

        class Payco(StripeObject):
            capture_method: Optional[Literal["manual"]]
            """
            Controls when the funds will be captured from the customer's account.
            """

        class Paynow(StripeObject):
            setup_future_usage: Optional[Literal["none"]]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
            """

        class Paypal(StripeObject):
            capture_method: Optional[Literal["manual"]]
            """
            Controls when the funds will be captured from the customer's account.
            """
            preferred_locale: Optional[str]
            """
            Preferred locale of the PayPal checkout page that the customer is redirected to.
            """
            reference: Optional[str]
            """
            A reference of the PayPal transaction visible to customer which is mapped to PayPal's invoice ID. This must be a globally unique ID if you have configured in your PayPal settings to block multiple payments per invoice ID.
            """
            setup_future_usage: Optional[Literal["none", "off_session"]]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
            """

        class Payto(StripeObject):
            class MandateOptions(StripeObject):
                amount: Optional[int]
                """
                Amount that will be collected. It is required when `amount_type` is `fixed`.
                """
                amount_type: Optional[Literal["fixed", "maximum"]]
                """
                The type of amount that will be collected. The amount charged must be exact or up to the value of `amount` param for `fixed` or `maximum` type respectively. Defaults to `maximum`.
                """
                end_date: Optional[str]
                """
                Date, in YYYY-MM-DD format, after which payments will not be collected. Defaults to no end date.
                """
                payment_schedule: Optional[
                    Literal[
                        "adhoc",
                        "annual",
                        "daily",
                        "fortnightly",
                        "monthly",
                        "quarterly",
                        "semi_annual",
                        "weekly",
                    ]
                ]
                """
                The periodicity at which payments will be collected. Defaults to `adhoc`.
                """
                payments_per_period: Optional[int]
                """
                The number of payments that will be made during a payment period. Defaults to 1 except for when `payment_schedule` is `adhoc`. In that case, it defaults to no limit.
                """
                purpose: Optional[
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
                start_date: Optional[str]
                """
                Date, in YYYY-MM-DD format, from which payments will be collected. Defaults to confirmation time.
                """

            mandate_options: Optional[MandateOptions]
            setup_future_usage: Optional[Literal["none", "off_session"]]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
            """
            _inner_class_types = {"mandate_options": MandateOptions}

        class Pix(StripeObject):
            amount_includes_iof: Optional[Literal["always", "never"]]
            """
            Determines if the amount includes the IOF tax.
            """
            expires_after_seconds: Optional[int]
            """
            The number of seconds after which Pix payment will expire.
            """
            setup_future_usage: Optional[Literal["none"]]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
            """

        class RevolutPay(StripeObject):
            capture_method: Optional[Literal["manual"]]
            """
            Controls when the funds will be captured from the customer's account.
            """
            setup_future_usage: Optional[Literal["none", "off_session"]]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
            """

        class SamsungPay(StripeObject):
            capture_method: Optional[Literal["manual"]]
            """
            Controls when the funds will be captured from the customer's account.
            """

        class Satispay(StripeObject):
            capture_method: Optional[Literal["manual"]]
            """
            Controls when the funds will be captured from the customer's account.
            """

        class SepaDebit(StripeObject):
            class MandateOptions(StripeObject):
                reference_prefix: Optional[str]
                """
                Prefix used to generate the Mandate reference. Must be at most 12 characters long. Must consist of only uppercase letters, numbers, spaces, or the following special characters: '/', '_', '-', '&', '.'. Cannot begin with 'STRIPE'.
                """

            mandate_options: Optional[MandateOptions]
            setup_future_usage: Optional[
                Literal["none", "off_session", "on_session"]
            ]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
            """
            target_date: Optional[str]
            """
            Controls when Stripe will attempt to debit the funds from the customer's account. The date must be a string in YYYY-MM-DD format. The date must be in the future and between 3 and 15 calendar days from now.
            """
            _inner_class_types = {"mandate_options": MandateOptions}

        class Sofort(StripeObject):
            setup_future_usage: Optional[Literal["none"]]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
            """

        class Swish(StripeObject):
            reference: Optional[str]
            """
            The order reference that will be displayed to customers in the Swish application. Defaults to the `id` of the Payment Intent.
            """

        class Twint(StripeObject):
            setup_future_usage: Optional[Literal["none"]]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
            """

        class UsBankAccount(StripeObject):
            class FinancialConnections(StripeObject):
                class Filters(StripeObject):
                    account_subcategories: Optional[
                        List[Literal["checking", "savings"]]
                    ]
                    """
                    The account subcategories to use to filter for possible accounts to link. Valid subcategories are `checking` and `savings`.
                    """

                filters: Optional[Filters]
                permissions: Optional[
                    List[
                        Literal[
                            "balances",
                            "ownership",
                            "payment_method",
                            "transactions",
                        ]
                    ]
                ]
                """
                The list of permissions to request. The `payment_method` permission must be included.
                """
                prefetch: Optional[
                    List[Literal["balances", "ownership", "transactions"]]
                ]
                """
                Data features requested to be retrieved upon account creation.
                """
                return_url: Optional[str]
                """
                For webview integrations only. Upon completing OAuth login in the native browser, the user will be redirected to this URL to return to your app.
                """
                _inner_class_types = {"filters": Filters}

            financial_connections: Optional[FinancialConnections]
            setup_future_usage: Optional[
                Literal["none", "off_session", "on_session"]
            ]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
            """
            target_date: Optional[str]
            """
            Controls when Stripe will attempt to debit the funds from the customer's account. The date must be a string in YYYY-MM-DD format. The date must be in the future and between 3 and 15 calendar days from now.
            """
            verification_method: Optional[Literal["automatic", "instant"]]
            """
            Bank account verification method.
            """
            _inner_class_types = {
                "financial_connections": FinancialConnections
            }

        acss_debit: Optional[AcssDebit]
        affirm: Optional[Affirm]
        afterpay_clearpay: Optional[AfterpayClearpay]
        alipay: Optional[Alipay]
        alma: Optional[Alma]
        amazon_pay: Optional[AmazonPay]
        au_becs_debit: Optional[AuBecsDebit]
        bacs_debit: Optional[BacsDebit]
        bancontact: Optional[Bancontact]
        billie: Optional[Billie]
        boleto: Optional[Boleto]
        card: Optional[Card]
        cashapp: Optional[Cashapp]
        customer_balance: Optional[CustomerBalance]
        eps: Optional[Eps]
        fpx: Optional[Fpx]
        giropay: Optional[Giropay]
        grabpay: Optional[Grabpay]
        ideal: Optional[Ideal]
        kakao_pay: Optional[KakaoPay]
        klarna: Optional[Klarna]
        konbini: Optional[Konbini]
        kr_card: Optional[KrCard]
        link: Optional[Link]
        mobilepay: Optional[Mobilepay]
        multibanco: Optional[Multibanco]
        naver_pay: Optional[NaverPay]
        oxxo: Optional[Oxxo]
        p24: Optional[P24]
        payco: Optional[Payco]
        paynow: Optional[Paynow]
        paypal: Optional[Paypal]
        payto: Optional[Payto]
        pix: Optional[Pix]
        revolut_pay: Optional[RevolutPay]
        samsung_pay: Optional[SamsungPay]
        satispay: Optional[Satispay]
        sepa_debit: Optional[SepaDebit]
        sofort: Optional[Sofort]
        swish: Optional[Swish]
        twint: Optional[Twint]
        us_bank_account: Optional[UsBankAccount]
        _inner_class_types = {
            "acss_debit": AcssDebit,
            "affirm": Affirm,
            "afterpay_clearpay": AfterpayClearpay,
            "alipay": Alipay,
            "alma": Alma,
            "amazon_pay": AmazonPay,
            "au_becs_debit": AuBecsDebit,
            "bacs_debit": BacsDebit,
            "bancontact": Bancontact,
            "billie": Billie,
            "boleto": Boleto,
            "card": Card,
            "cashapp": Cashapp,
            "customer_balance": CustomerBalance,
            "eps": Eps,
            "fpx": Fpx,
            "giropay": Giropay,
            "grabpay": Grabpay,
            "ideal": Ideal,
            "kakao_pay": KakaoPay,
            "klarna": Klarna,
            "konbini": Konbini,
            "kr_card": KrCard,
            "link": Link,
            "mobilepay": Mobilepay,
            "multibanco": Multibanco,
            "naver_pay": NaverPay,
            "oxxo": Oxxo,
            "p24": P24,
            "payco": Payco,
            "paynow": Paynow,
            "paypal": Paypal,
            "payto": Payto,
            "pix": Pix,
            "revolut_pay": RevolutPay,
            "samsung_pay": SamsungPay,
            "satispay": Satispay,
            "sepa_debit": SepaDebit,
            "sofort": Sofort,
            "swish": Swish,
            "twint": Twint,
            "us_bank_account": UsBankAccount,
        }

    class Permissions(StripeObject):
        update_shipping_details: Optional[
            Literal["client_only", "server_only"]
        ]
        """
        Determines which entity is allowed to update the shipping details.

        Default is `client_only`. Stripe Checkout client will automatically update the shipping details. If set to `server_only`, only your server is allowed to update the shipping details.

        When set to `server_only`, you must add the onShippingDetailsChange event handler when initializing the Stripe Checkout client and manually update the shipping details from your server using the Stripe API.
        """

    class PhoneNumberCollection(StripeObject):
        enabled: bool
        """
        Indicates whether phone number collection is enabled for the session
        """

    class PresentmentDetails(StripeObject):
        presentment_amount: int
        """
        Amount intended to be collected by this payment, denominated in `presentment_currency`.
        """
        presentment_currency: str
        """
        Currency presented to the customer during payment.
        """

    class SavedPaymentMethodOptions(StripeObject):
        allow_redisplay_filters: Optional[
            List[Literal["always", "limited", "unspecified"]]
        ]
        """
        Uses the `allow_redisplay` value of each saved payment method to filter the set presented to a returning customer. By default, only saved payment methods with 'allow_redisplay: ‘always' are shown in Checkout.
        """
        payment_method_remove: Optional[Literal["disabled", "enabled"]]
        """
        Enable customers to choose if they wish to remove their saved payment methods. Disabled by default.
        """
        payment_method_save: Optional[Literal["disabled", "enabled"]]
        """
        Enable customers to choose if they wish to save their payment method for future use. Disabled by default.
        """

    class ShippingAddressCollection(StripeObject):
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
        shipping locations. Unsupported country codes: `AS, CX, CC, CU, HM, IR, KP, MH, FM, NF, MP, PW, SY, UM, VI`.
        """

    class ShippingCost(StripeObject):
        class Tax(StripeObject):
            amount: int
            """
            Amount of tax applied for this rate.
            """
            rate: "TaxRate"
            """
            Tax rates can be applied to [invoices](https://docs.stripe.com/invoicing/taxes/tax-rates), [subscriptions](https://docs.stripe.com/billing/taxes/tax-rates) and [Checkout Sessions](https://docs.stripe.com/payments/checkout/use-manual-tax-rates) to collect tax.

            Related guide: [Tax rates](https://docs.stripe.com/billing/taxes/tax-rates)
            """
            taxability_reason: Optional[
                Literal[
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
            ]
            """
            The reasoning behind this tax, for example, if the product is tax exempt. The possible values for this field may be extended as new tax rules are supported.
            """
            taxable_amount: Optional[int]
            """
            The amount on which tax is calculated, in cents (or local equivalent).
            """

        amount_subtotal: int
        """
        Total shipping cost before any discounts or taxes are applied.
        """
        amount_tax: int
        """
        Total tax amount applied due to shipping costs. If no tax was applied, defaults to 0.
        """
        amount_total: int
        """
        Total shipping cost after discounts and taxes are applied.
        """
        shipping_rate: Optional[ExpandableField["ShippingRate"]]
        """
        The ID of the ShippingRate for this order.
        """
        taxes: Optional[List[Tax]]
        """
        The taxes applied to the shipping rate.
        """
        _inner_class_types = {"taxes": Tax}

    class ShippingOption(StripeObject):
        shipping_amount: int
        """
        A non-negative integer in cents representing how much to charge.
        """
        shipping_rate: ExpandableField["ShippingRate"]
        """
        The shipping rate.
        """

    class TaxIdCollection(StripeObject):
        enabled: bool
        """
        Indicates whether tax ID collection is enabled for the session
        """
        required: Literal["if_supported", "never"]
        """
        Indicates whether a tax ID is required on the payment page
        """

    class TotalDetails(StripeObject):
        class Breakdown(StripeObject):
            class Discount(StripeObject):
                amount: int
                """
                The amount discounted.
                """
                discount: "DiscountResource"
                """
                A discount represents the actual application of a [coupon](https://api.stripe.com#coupons) or [promotion code](https://api.stripe.com#promotion_codes).
                It contains information about when the discount began, when it will end, and what it is applied to.

                Related guide: [Applying discounts to subscriptions](https://docs.stripe.com/billing/subscriptions/discounts)
                """

            class Tax(StripeObject):
                amount: int
                """
                Amount of tax applied for this rate.
                """
                rate: "TaxRate"
                """
                Tax rates can be applied to [invoices](https://docs.stripe.com/invoicing/taxes/tax-rates), [subscriptions](https://docs.stripe.com/billing/taxes/tax-rates) and [Checkout Sessions](https://docs.stripe.com/payments/checkout/use-manual-tax-rates) to collect tax.

                Related guide: [Tax rates](https://docs.stripe.com/billing/taxes/tax-rates)
                """
                taxability_reason: Optional[
                    Literal[
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
                ]
                """
                The reasoning behind this tax, for example, if the product is tax exempt. The possible values for this field may be extended as new tax rules are supported.
                """
                taxable_amount: Optional[int]
                """
                The amount on which tax is calculated, in cents (or local equivalent).
                """

            discounts: List[Discount]
            """
            The aggregated discounts.
            """
            taxes: List[Tax]
            """
            The aggregated tax amounts by rate.
            """
            _inner_class_types = {"discounts": Discount, "taxes": Tax}

        amount_discount: int
        """
        This is the sum of all the discounts.
        """
        amount_shipping: Optional[int]
        """
        This is the sum of all the shipping amounts.
        """
        amount_tax: int
        """
        This is the sum of all the tax amounts.
        """
        breakdown: Optional[Breakdown]
        _inner_class_types = {"breakdown": Breakdown}

    class WalletOptions(StripeObject):
        class Link(StripeObject):
            display: Optional[Literal["auto", "never"]]
            """
            Describes whether Checkout should display Link. Defaults to `auto`.
            """

        link: Optional[Link]
        _inner_class_types = {"link": Link}

    adaptive_pricing: Optional[AdaptivePricing]
    """
    Settings for price localization with [Adaptive Pricing](https://docs.stripe.com/payments/checkout/adaptive-pricing).
    """
    after_expiration: Optional[AfterExpiration]
    """
    When set, provides configuration for actions to take if this Checkout Session expires.
    """
    allow_promotion_codes: Optional[bool]
    """
    Enables user redeemable promotion codes.
    """
    amount_subtotal: Optional[int]
    """
    Total of all items before discounts or taxes are applied.
    """
    amount_total: Optional[int]
    """
    Total of all items after discounts and taxes are applied.
    """
    automatic_tax: AutomaticTax
    billing_address_collection: Optional[Literal["auto", "required"]]
    """
    Describes whether Checkout should collect the customer's billing address. Defaults to `auto`.
    """
    branding_settings: Optional[BrandingSettings]
    cancel_url: Optional[str]
    """
    If set, Checkout displays a back button and customers will be directed to this URL if they decide to cancel payment and return to your website.
    """
    client_reference_id: Optional[str]
    """
    A unique string to reference the Checkout Session. This can be a
    customer ID, a cart ID, or similar, and can be used to reconcile the
    Session with your internal systems.
    """
    client_secret: Optional[str]
    """
    The client secret of your Checkout Session. Applies to Checkout Sessions with `ui_mode: embedded` or `ui_mode: custom`. For `ui_mode: embedded`, the client secret is to be used when initializing Stripe.js embedded checkout.
     For `ui_mode: custom`, use the client secret with [initCheckout](https://docs.stripe.com/js/custom_checkout/init) on your front end.
    """
    collected_information: Optional[CollectedInformation]
    """
    Information about the customer collected within the Checkout Session.
    """
    consent: Optional[Consent]
    """
    Results of `consent_collection` for this session.
    """
    consent_collection: Optional[ConsentCollection]
    """
    When set, provides configuration for the Checkout Session to gather active consent from customers.
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    currency: Optional[str]
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    currency_conversion: Optional[CurrencyConversion]
    """
    Currency conversion details for [Adaptive Pricing](https://docs.stripe.com/payments/checkout/adaptive-pricing) sessions created before 2025-03-31.
    """
    custom_fields: List[CustomField]
    """
    Collect additional information from your customer using custom fields. Up to 3 fields are supported. You can't set this parameter if `ui_mode` is `custom`.
    """
    custom_text: CustomText
    customer: Optional[ExpandableField["Customer"]]
    """
    The ID of the customer for this Session.
    For Checkout Sessions in `subscription` mode or Checkout Sessions with `customer_creation` set as `always` in `payment` mode, Checkout
    will create a new customer object based on information provided
    during the payment flow unless an existing customer was provided when
    the Session was created.
    """
    customer_account: Optional[str]
    """
    The ID of the account for this Session.
    """
    customer_creation: Optional[Literal["always", "if_required"]]
    """
    Configure whether a Checkout Session creates a Customer when the Checkout Session completes.
    """
    customer_details: Optional[CustomerDetails]
    """
    The customer details including the customer's tax exempt status and the customer's tax IDs. Customer's address details are not present on Sessions in `setup` mode.
    """
    customer_email: Optional[str]
    """
    If provided, this value will be used when the Customer object is created.
    If not provided, customers will be asked to enter their email address.
    Use this parameter to prefill customer data if you already have an email
    on file. To access information about the customer once the payment flow is
    complete, use the `customer` attribute.
    """
    discounts: Optional[List[Discount]]
    """
    List of coupons and promotion codes attached to the Checkout Session.
    """
    excluded_payment_method_types: Optional[List[str]]
    """
    A list of the types of payment methods (e.g., `card`) that should be excluded from this Checkout Session. This should only be used when payment methods for this Checkout Session are managed through the [Stripe Dashboard](https://dashboard.stripe.com/settings/payment_methods).
    """
    expires_at: int
    """
    The timestamp at which the Checkout Session will expire.
    """
    id: str
    """
    Unique identifier for the object.
    """
    invoice: Optional[ExpandableField["Invoice"]]
    """
    ID of the invoice created by the Checkout Session, if it exists.
    """
    invoice_creation: Optional[InvoiceCreation]
    """
    Details on the state of invoice creation for the Checkout Session.
    """
    line_items: Optional[ListObject["LineItem"]]
    """
    The line items purchased by the customer.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    locale: Optional[
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
    metadata: Optional[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    mode: Literal["payment", "setup", "subscription"]
    """
    The mode of the Checkout Session.
    """
    name_collection: Optional[NameCollection]
    object: Literal["checkout.session"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    optional_items: Optional[List[OptionalItem]]
    """
    The optional items presented to the customer at checkout.
    """
    origin_context: Optional[Literal["mobile_app", "web"]]
    """
    Where the user is coming from. This informs the optimizations that are applied to the session.
    """
    payment_intent: Optional[ExpandableField["PaymentIntent"]]
    """
    The ID of the PaymentIntent for Checkout Sessions in `payment` mode. You can't confirm or cancel the PaymentIntent for a Checkout Session. To cancel, [expire the Checkout Session](https://docs.stripe.com/api/checkout/sessions/expire) instead.
    """
    payment_link: Optional[ExpandableField["PaymentLink"]]
    """
    The ID of the Payment Link that created this Session.
    """
    payment_method_collection: Optional[Literal["always", "if_required"]]
    """
    Configure whether a Checkout Session should collect a payment method. Defaults to `always`.
    """
    payment_method_configuration_details: Optional[
        PaymentMethodConfigurationDetails
    ]
    """
    Information about the payment method configuration used for this Checkout session if using dynamic payment methods.
    """
    payment_method_options: Optional[PaymentMethodOptions]
    """
    Payment-method-specific configuration for the PaymentIntent or SetupIntent of this CheckoutSession.
    """
    payment_method_types: List[str]
    """
    A list of the types of payment methods (e.g. card) this Checkout
    Session is allowed to accept.
    """
    payment_status: Literal["no_payment_required", "paid", "unpaid"]
    """
    The payment status of the Checkout Session, one of `paid`, `unpaid`, or `no_payment_required`.
    You can use this value to decide when to fulfill your customer's order.
    """
    permissions: Optional[Permissions]
    """
    This property is used to set up permissions for various actions (e.g., update) on the CheckoutSession object.

    For specific permissions, please refer to their dedicated subsections, such as `permissions.update_shipping_details`.
    """
    phone_number_collection: Optional[PhoneNumberCollection]
    presentment_details: Optional[PresentmentDetails]
    recovered_from: Optional[str]
    """
    The ID of the original expired Checkout Session that triggered the recovery flow.
    """
    redirect_on_completion: Optional[Literal["always", "if_required", "never"]]
    """
    This parameter applies to `ui_mode: embedded`. Learn more about the [redirect behavior](https://docs.stripe.com/payments/checkout/custom-success-page?payment-ui=embedded-form) of embedded sessions. Defaults to `always`.
    """
    return_url: Optional[str]
    """
    Applies to Checkout Sessions with `ui_mode: embedded` or `ui_mode: custom`. The URL to redirect your customer back to after they authenticate or cancel their payment on the payment method's app or site.
    """
    saved_payment_method_options: Optional[SavedPaymentMethodOptions]
    """
    Controls saved payment method settings for the session. Only available in `payment` and `subscription` mode.
    """
    setup_intent: Optional[ExpandableField["SetupIntent"]]
    """
    The ID of the SetupIntent for Checkout Sessions in `setup` mode. You can't confirm or cancel the SetupIntent for a Checkout Session. To cancel, [expire the Checkout Session](https://docs.stripe.com/api/checkout/sessions/expire) instead.
    """
    shipping_address_collection: Optional[ShippingAddressCollection]
    """
    When set, provides configuration for Checkout to collect a shipping address from a customer.
    """
    shipping_cost: Optional[ShippingCost]
    """
    The details of the customer cost of shipping, including the customer chosen ShippingRate.
    """
    shipping_options: List[ShippingOption]
    """
    The shipping rate options applied to this Session.
    """
    status: Optional[Literal["complete", "expired", "open"]]
    """
    The status of the Checkout Session, one of `open`, `complete`, or `expired`.
    """
    submit_type: Optional[
        Literal["auto", "book", "donate", "pay", "subscribe"]
    ]
    """
    Describes the type of transaction being performed by Checkout in order to customize
    relevant text on the page, such as the submit button. `submit_type` can only be
    specified on Checkout Sessions in `payment` mode. If blank or `auto`, `pay` is used.
    """
    subscription: Optional[ExpandableField["Subscription"]]
    """
    The ID of the [Subscription](https://docs.stripe.com/api/subscriptions) for Checkout Sessions in `subscription` mode.
    """
    success_url: Optional[str]
    """
    The URL the customer will be directed to after the payment or
    subscription creation is successful.
    """
    tax_id_collection: Optional[TaxIdCollection]
    total_details: Optional[TotalDetails]
    """
    Tax and discount details for the computed total amount.
    """
    ui_mode: Optional[Literal["custom", "embedded", "hosted"]]
    """
    The UI mode of the Session. Defaults to `hosted`.
    """
    url: Optional[str]
    """
    The URL to the Checkout Session. Applies to Checkout Sessions with `ui_mode: hosted`. Redirect customers to this URL to take them to Checkout. If you're using [Custom Domains](https://docs.stripe.com/payments/checkout/custom-domains), the URL will use your subdomain. Otherwise, it'll use `checkout.stripe.com.`
    This value is only present when the session is active.
    """
    wallet_options: Optional[WalletOptions]
    """
    Wallet-specific configuration for this Checkout Session.
    """

    @classmethod
    def create(cls, **params: Unpack["SessionCreateParams"]) -> "Session":
        """
        Creates a Checkout Session object.
        """
        return cast(
            "Session",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["SessionCreateParams"]
    ) -> "Session":
        """
        Creates a Checkout Session object.
        """
        return cast(
            "Session",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def _cls_expire(
        cls, session: str, **params: Unpack["SessionExpireParams"]
    ) -> "Session":
        """
        A Checkout Session can be expired when it is in one of these statuses: open

        After it expires, a customer can't complete a Checkout Session and customers loading the Checkout Session see a message saying the Checkout Session is expired.
        """
        return cast(
            "Session",
            cls._static_request(
                "post",
                "/v1/checkout/sessions/{session}/expire".format(
                    session=sanitize_id(session)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def expire(
        session: str, **params: Unpack["SessionExpireParams"]
    ) -> "Session":
        """
        A Checkout Session can be expired when it is in one of these statuses: open

        After it expires, a customer can't complete a Checkout Session and customers loading the Checkout Session see a message saying the Checkout Session is expired.
        """
        ...

    @overload
    def expire(self, **params: Unpack["SessionExpireParams"]) -> "Session":
        """
        A Checkout Session can be expired when it is in one of these statuses: open

        After it expires, a customer can't complete a Checkout Session and customers loading the Checkout Session see a message saying the Checkout Session is expired.
        """
        ...

    @class_method_variant("_cls_expire")
    def expire(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["SessionExpireParams"]
    ) -> "Session":
        """
        A Checkout Session can be expired when it is in one of these statuses: open

        After it expires, a customer can't complete a Checkout Session and customers loading the Checkout Session see a message saying the Checkout Session is expired.
        """
        return cast(
            "Session",
            self._request(
                "post",
                "/v1/checkout/sessions/{session}/expire".format(
                    session=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_expire_async(
        cls, session: str, **params: Unpack["SessionExpireParams"]
    ) -> "Session":
        """
        A Checkout Session can be expired when it is in one of these statuses: open

        After it expires, a customer can't complete a Checkout Session and customers loading the Checkout Session see a message saying the Checkout Session is expired.
        """
        return cast(
            "Session",
            await cls._static_request_async(
                "post",
                "/v1/checkout/sessions/{session}/expire".format(
                    session=sanitize_id(session)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def expire_async(
        session: str, **params: Unpack["SessionExpireParams"]
    ) -> "Session":
        """
        A Checkout Session can be expired when it is in one of these statuses: open

        After it expires, a customer can't complete a Checkout Session and customers loading the Checkout Session see a message saying the Checkout Session is expired.
        """
        ...

    @overload
    async def expire_async(
        self, **params: Unpack["SessionExpireParams"]
    ) -> "Session":
        """
        A Checkout Session can be expired when it is in one of these statuses: open

        After it expires, a customer can't complete a Checkout Session and customers loading the Checkout Session see a message saying the Checkout Session is expired.
        """
        ...

    @class_method_variant("_cls_expire_async")
    async def expire_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["SessionExpireParams"]
    ) -> "Session":
        """
        A Checkout Session can be expired when it is in one of these statuses: open

        After it expires, a customer can't complete a Checkout Session and customers loading the Checkout Session see a message saying the Checkout Session is expired.
        """
        return cast(
            "Session",
            await self._request_async(
                "post",
                "/v1/checkout/sessions/{session}/expire".format(
                    session=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def list(
        cls, **params: Unpack["SessionListParams"]
    ) -> ListObject["Session"]:
        """
        Returns a list of Checkout Sessions.
        """
        result = cls._static_request(
            "get",
            cls.class_url(),
            params=params,
        )
        if not isinstance(result, ListObject):
            raise TypeError(
                "Expected list object from API, got %s"
                % (type(result).__name__)
            )

        return result

    @classmethod
    async def list_async(
        cls, **params: Unpack["SessionListParams"]
    ) -> ListObject["Session"]:
        """
        Returns a list of Checkout Sessions.
        """
        result = await cls._static_request_async(
            "get",
            cls.class_url(),
            params=params,
        )
        if not isinstance(result, ListObject):
            raise TypeError(
                "Expected list object from API, got %s"
                % (type(result).__name__)
            )

        return result

    @classmethod
    def _cls_list_line_items(
        cls, session: str, **params: Unpack["SessionListLineItemsParams"]
    ) -> ListObject["LineItem"]:
        """
        When retrieving a Checkout Session, there is an includable line_items property containing the first handful of those items. There is also a URL where you can retrieve the full (paginated) list of line items.
        """
        return cast(
            ListObject["LineItem"],
            cls._static_request(
                "get",
                "/v1/checkout/sessions/{session}/line_items".format(
                    session=sanitize_id(session)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def list_line_items(
        session: str, **params: Unpack["SessionListLineItemsParams"]
    ) -> ListObject["LineItem"]:
        """
        When retrieving a Checkout Session, there is an includable line_items property containing the first handful of those items. There is also a URL where you can retrieve the full (paginated) list of line items.
        """
        ...

    @overload
    def list_line_items(
        self, **params: Unpack["SessionListLineItemsParams"]
    ) -> ListObject["LineItem"]:
        """
        When retrieving a Checkout Session, there is an includable line_items property containing the first handful of those items. There is also a URL where you can retrieve the full (paginated) list of line items.
        """
        ...

    @class_method_variant("_cls_list_line_items")
    def list_line_items(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["SessionListLineItemsParams"]
    ) -> ListObject["LineItem"]:
        """
        When retrieving a Checkout Session, there is an includable line_items property containing the first handful of those items. There is also a URL where you can retrieve the full (paginated) list of line items.
        """
        return cast(
            ListObject["LineItem"],
            self._request(
                "get",
                "/v1/checkout/sessions/{session}/line_items".format(
                    session=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_list_line_items_async(
        cls, session: str, **params: Unpack["SessionListLineItemsParams"]
    ) -> ListObject["LineItem"]:
        """
        When retrieving a Checkout Session, there is an includable line_items property containing the first handful of those items. There is also a URL where you can retrieve the full (paginated) list of line items.
        """
        return cast(
            ListObject["LineItem"],
            await cls._static_request_async(
                "get",
                "/v1/checkout/sessions/{session}/line_items".format(
                    session=sanitize_id(session)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def list_line_items_async(
        session: str, **params: Unpack["SessionListLineItemsParams"]
    ) -> ListObject["LineItem"]:
        """
        When retrieving a Checkout Session, there is an includable line_items property containing the first handful of those items. There is also a URL where you can retrieve the full (paginated) list of line items.
        """
        ...

    @overload
    async def list_line_items_async(
        self, **params: Unpack["SessionListLineItemsParams"]
    ) -> ListObject["LineItem"]:
        """
        When retrieving a Checkout Session, there is an includable line_items property containing the first handful of those items. There is also a URL where you can retrieve the full (paginated) list of line items.
        """
        ...

    @class_method_variant("_cls_list_line_items_async")
    async def list_line_items_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["SessionListLineItemsParams"]
    ) -> ListObject["LineItem"]:
        """
        When retrieving a Checkout Session, there is an includable line_items property containing the first handful of those items. There is also a URL where you can retrieve the full (paginated) list of line items.
        """
        return cast(
            ListObject["LineItem"],
            await self._request_async(
                "get",
                "/v1/checkout/sessions/{session}/line_items".format(
                    session=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def modify(
        cls, id: str, **params: Unpack["SessionModifyParams"]
    ) -> "Session":
        """
        Updates a Checkout Session object.

        Related guide: [Dynamically update a Checkout Session](https://docs.stripe.com/payments/advanced/dynamic-updates)
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Session",
            cls._static_request(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    async def modify_async(
        cls, id: str, **params: Unpack["SessionModifyParams"]
    ) -> "Session":
        """
        Updates a Checkout Session object.

        Related guide: [Dynamically update a Checkout Session](https://docs.stripe.com/payments/advanced/dynamic-updates)
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Session",
            await cls._static_request_async(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["SessionRetrieveParams"]
    ) -> "Session":
        """
        Retrieves a Checkout Session object.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["SessionRetrieveParams"]
    ) -> "Session":
        """
        Retrieves a Checkout Session object.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    _inner_class_types = {
        "adaptive_pricing": AdaptivePricing,
        "after_expiration": AfterExpiration,
        "automatic_tax": AutomaticTax,
        "branding_settings": BrandingSettings,
        "collected_information": CollectedInformation,
        "consent": Consent,
        "consent_collection": ConsentCollection,
        "currency_conversion": CurrencyConversion,
        "custom_fields": CustomField,
        "custom_text": CustomText,
        "customer_details": CustomerDetails,
        "discounts": Discount,
        "invoice_creation": InvoiceCreation,
        "name_collection": NameCollection,
        "optional_items": OptionalItem,
        "payment_method_configuration_details": PaymentMethodConfigurationDetails,
        "payment_method_options": PaymentMethodOptions,
        "permissions": Permissions,
        "phone_number_collection": PhoneNumberCollection,
        "presentment_details": PresentmentDetails,
        "saved_payment_method_options": SavedPaymentMethodOptions,
        "shipping_address_collection": ShippingAddressCollection,
        "shipping_cost": ShippingCost,
        "shipping_options": ShippingOption,
        "tax_id_collection": TaxIdCollection,
        "total_details": TotalDetails,
        "wallet_options": WalletOptions,
    }
