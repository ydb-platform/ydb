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
    from stripe._application import Application
    from stripe._line_item import LineItem
    from stripe._shipping_rate import ShippingRate
    from stripe._tax_id import TaxId
    from stripe.params._payment_link_create_params import (
        PaymentLinkCreateParams,
    )
    from stripe.params._payment_link_list_line_items_params import (
        PaymentLinkListLineItemsParams,
    )
    from stripe.params._payment_link_list_params import PaymentLinkListParams
    from stripe.params._payment_link_modify_params import (
        PaymentLinkModifyParams,
    )
    from stripe.params._payment_link_retrieve_params import (
        PaymentLinkRetrieveParams,
    )


class PaymentLink(
    CreateableAPIResource["PaymentLink"],
    ListableAPIResource["PaymentLink"],
    UpdateableAPIResource["PaymentLink"],
):
    """
    A payment link is a shareable URL that will take your customers to a hosted payment page. A payment link can be shared and used multiple times.

    When a customer opens a payment link it will open a new [checkout session](https://docs.stripe.com/api/checkout/sessions) to render the payment page. You can use [checkout session events](https://docs.stripe.com/api/events/types#event_types-checkout.session.completed) to track payments through payment links.

    Related guide: [Payment Links API](https://docs.stripe.com/payment-links)
    """

    OBJECT_NAME: ClassVar[Literal["payment_link"]] = "payment_link"

    class AfterCompletion(StripeObject):
        class HostedConfirmation(StripeObject):
            custom_message: Optional[str]
            """
            The custom message that is displayed to the customer after the purchase is complete.
            """

        class Redirect(StripeObject):
            url: str
            """
            The URL the customer will be redirected to after the purchase is complete.
            """

        hosted_confirmation: Optional[HostedConfirmation]
        redirect: Optional[Redirect]
        type: Literal["hosted_confirmation", "redirect"]
        """
        The specified behavior after the purchase is complete.
        """
        _inner_class_types = {
            "hosted_confirmation": HostedConfirmation,
            "redirect": Redirect,
        }

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
        If `true`, tax will be calculated automatically using the customer's location.
        """
        liability: Optional[Liability]
        """
        The account that's liable for tax. If set, the business address and tax registrations required to perform the tax calculation are loaded from this account. The tax transaction is returned in the report of the connected account.
        """
        _inner_class_types = {"liability": Liability}

    class ConsentCollection(StripeObject):
        class PaymentMethodReuseAgreement(StripeObject):
            position: Literal["auto", "hidden"]
            """
            Determines the position and visibility of the payment method reuse agreement in the UI. When set to `auto`, Stripe's defaults will be used.

            When set to `hidden`, the payment method reuse agreement text will always be hidden in the UI.
            """

        payment_method_reuse_agreement: Optional[PaymentMethodReuseAgreement]
        """
        Settings related to the payment method reuse text shown in the Checkout UI.
        """
        promotions: Optional[Literal["auto", "none"]]
        """
        If set to `auto`, enables the collection of customer consent for promotional communications.
        """
        terms_of_service: Optional[Literal["none", "required"]]
        """
        If set to `required`, it requires cutomers to accept the terms of service before being able to pay. If set to `none`, customers won't be shown a checkbox to accept the terms of service.
        """
        _inner_class_types = {
            "payment_method_reuse_agreement": PaymentMethodReuseAgreement,
        }

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

            account_tax_ids: Optional[List[ExpandableField["TaxId"]]]
            """
            The account tax IDs associated with the invoice.
            """
            custom_fields: Optional[List[CustomField]]
            """
            A list of up to 4 custom fields to be displayed on the invoice.
            """
            description: Optional[str]
            """
            An arbitrary string attached to the object. Often useful for displaying to users.
            """
            footer: Optional[str]
            """
            Footer to be displayed on the invoice.
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
        Enable creating an invoice on successful payment.
        """
        invoice_data: Optional[InvoiceData]
        """
        Configuration for the invoice. Default invoice values will be used if unspecified.
        """
        _inner_class_types = {"invoice_data": InvoiceData}

    class NameCollection(StripeObject):
        class Business(StripeObject):
            enabled: bool
            """
            Indicates whether business name collection is enabled for the payment link.
            """
            optional: bool
            """
            Whether the customer is required to complete the field before checking out. Defaults to `false`.
            """

        class Individual(StripeObject):
            enabled: bool
            """
            Indicates whether individual name collection is enabled for the payment link.
            """
            optional: bool
            """
            Whether the customer is required to complete the field before checking out. Defaults to `false`.
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
            The maximum quantity of this item the customer can purchase. By default this value is 99.
            """
            minimum: Optional[int]
            """
            The minimum quantity of this item the customer must purchase, if they choose to purchase it. Because this item is optional, the customer will always be able to remove it from their order, even if the `minimum` configured here is greater than 0. By default this value is 0.
            """

        adjustable_quantity: Optional[AdjustableQuantity]
        price: str
        quantity: int
        _inner_class_types = {"adjustable_quantity": AdjustableQuantity}

    class PaymentIntentData(StripeObject):
        capture_method: Optional[
            Literal["automatic", "automatic_async", "manual"]
        ]
        """
        Indicates when the funds will be captured from the customer's account.
        """
        description: Optional[str]
        """
        An arbitrary string attached to the object. Often useful for displaying to users.
        """
        metadata: Dict[str, str]
        """
        Set of [key-value pairs](https://docs.stripe.com/api/metadata) that will set metadata on [Payment Intents](https://docs.stripe.com/api/payment_intents) generated from this payment link.
        """
        setup_future_usage: Optional[Literal["off_session", "on_session"]]
        """
        Indicates that you intend to make future payments with the payment method collected during checkout.
        """
        statement_descriptor: Optional[str]
        """
        For a non-card payment, information about the charge that appears on the customer's statement when this payment succeeds in creating a charge.
        """
        statement_descriptor_suffix: Optional[str]
        """
        For a card payment, information about the charge that appears on the customer's statement when this payment succeeds in creating a charge. Concatenated with the account's statement descriptor prefix to form the complete statement descriptor.
        """
        transfer_group: Optional[str]
        """
        A string that identifies the resulting payment as part of a group. See the PaymentIntents [use case for connected accounts](https://docs.stripe.com/connect/separate-charges-and-transfers) for details.
        """

    class PhoneNumberCollection(StripeObject):
        enabled: bool
        """
        If `true`, a phone number will be collected during checkout.
        """

    class Restrictions(StripeObject):
        class CompletedSessions(StripeObject):
            count: int
            """
            The current number of checkout sessions that have been completed on the payment link which count towards the `completed_sessions` restriction to be met.
            """
            limit: int
            """
            The maximum number of checkout sessions that can be completed for the `completed_sessions` restriction to be met.
            """

        completed_sessions: CompletedSessions
        _inner_class_types = {"completed_sessions": CompletedSessions}

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
        An array of two-letter ISO country codes representing which countries Checkout should provide as options for shipping locations. Unsupported country codes: `AS, CX, CC, CU, HM, IR, KP, MH, FM, NF, MP, PW, SD, SY, UM, VI`.
        """

    class ShippingOption(StripeObject):
        shipping_amount: int
        """
        A non-negative integer in cents representing how much to charge.
        """
        shipping_rate: ExpandableField["ShippingRate"]
        """
        The ID of the Shipping Rate to use for this shipping option.
        """

    class SubscriptionData(StripeObject):
        class InvoiceSettings(StripeObject):
            class Issuer(StripeObject):
                account: Optional[ExpandableField["Account"]]
                """
                The connected account being referenced when `type` is `account`.
                """
                type: Literal["account", "self"]
                """
                Type of the account referenced.
                """

            issuer: Issuer
            _inner_class_types = {"issuer": Issuer}

        class TrialSettings(StripeObject):
            class EndBehavior(StripeObject):
                missing_payment_method: Literal[
                    "cancel", "create_invoice", "pause"
                ]
                """
                Indicates how the subscription should change when the trial ends if the user did not provide a payment method.
                """

            end_behavior: EndBehavior
            """
            Defines how a subscription behaves when a free trial ends.
            """
            _inner_class_types = {"end_behavior": EndBehavior}

        description: Optional[str]
        """
        The subscription's description, meant to be displayable to the customer. Use this field to optionally store an explanation of the subscription for rendering in Stripe surfaces and certain local payment methods UIs.
        """
        invoice_settings: InvoiceSettings
        metadata: Dict[str, str]
        """
        Set of [key-value pairs](https://docs.stripe.com/api/metadata) that will set metadata on [Subscriptions](https://docs.stripe.com/api/subscriptions) generated from this payment link.
        """
        trial_period_days: Optional[int]
        """
        Integer representing the number of trial period days before the customer is charged for the first time.
        """
        trial_settings: Optional[TrialSettings]
        """
        Settings related to subscription trials.
        """
        _inner_class_types = {
            "invoice_settings": InvoiceSettings,
            "trial_settings": TrialSettings,
        }

    class TaxIdCollection(StripeObject):
        enabled: bool
        """
        Indicates whether tax ID collection is enabled for the session.
        """
        required: Literal["if_supported", "never"]

    class TransferData(StripeObject):
        amount: Optional[int]
        """
        The amount in cents (or local equivalent) that will be transferred to the destination account. By default, the entire amount is transferred to the destination.
        """
        destination: ExpandableField["Account"]
        """
        The connected account receiving the transfer.
        """

    active: bool
    """
    Whether the payment link's `url` is active. If `false`, customers visiting the URL will be shown a page saying that the link has been deactivated.
    """
    after_completion: AfterCompletion
    allow_promotion_codes: bool
    """
    Whether user redeemable promotion codes are enabled.
    """
    application: Optional[ExpandableField["Application"]]
    """
    The ID of the Connect application that created the Payment Link.
    """
    application_fee_amount: Optional[int]
    """
    The amount of the application fee (if any) that will be requested to be applied to the payment and transferred to the application owner's Stripe account.
    """
    application_fee_percent: Optional[float]
    """
    This represents the percentage of the subscription invoice total that will be transferred to the application owner's Stripe account.
    """
    automatic_tax: AutomaticTax
    billing_address_collection: Literal["auto", "required"]
    """
    Configuration for collecting the customer's billing address. Defaults to `auto`.
    """
    consent_collection: Optional[ConsentCollection]
    """
    When set, provides configuration to gather active consent from customers.
    """
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    custom_fields: List[CustomField]
    """
    Collect additional information from your customer using custom fields. Up to 3 fields are supported. You can't set this parameter if `ui_mode` is `custom`.
    """
    custom_text: CustomText
    customer_creation: Literal["always", "if_required"]
    """
    Configuration for Customer creation during checkout.
    """
    id: str
    """
    Unique identifier for the object.
    """
    inactive_message: Optional[str]
    """
    The custom message to be displayed to a customer when a payment link is no longer active.
    """
    invoice_creation: Optional[InvoiceCreation]
    """
    Configuration for creating invoice for payment mode payment links.
    """
    line_items: Optional[ListObject["LineItem"]]
    """
    The line items representing what is being sold.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    metadata: Dict[str, str]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    name_collection: Optional[NameCollection]
    object: Literal["payment_link"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    on_behalf_of: Optional[ExpandableField["Account"]]
    """
    The account on behalf of which to charge. See the [Connect documentation](https://support.stripe.com/questions/sending-invoices-on-behalf-of-connected-accounts) for details.
    """
    optional_items: Optional[List[OptionalItem]]
    """
    The optional items presented to the customer at checkout.
    """
    payment_intent_data: Optional[PaymentIntentData]
    """
    Indicates the parameters to be passed to PaymentIntent creation during checkout.
    """
    payment_method_collection: Literal["always", "if_required"]
    """
    Configuration for collecting a payment method during checkout. Defaults to `always`.
    """
    payment_method_types: Optional[
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
    The list of payment method types that customers can use. When `null`, Stripe will dynamically show relevant payment methods you've enabled in your [payment method settings](https://dashboard.stripe.com/settings/payment_methods).
    """
    phone_number_collection: PhoneNumberCollection
    restrictions: Optional[Restrictions]
    """
    Settings that restrict the usage of a payment link.
    """
    shipping_address_collection: Optional[ShippingAddressCollection]
    """
    Configuration for collecting the customer's shipping address.
    """
    shipping_options: List[ShippingOption]
    """
    The shipping rate options applied to the session.
    """
    submit_type: Literal["auto", "book", "donate", "pay", "subscribe"]
    """
    Indicates the type of transaction being performed which customizes relevant text on the page, such as the submit button.
    """
    subscription_data: Optional[SubscriptionData]
    """
    When creating a subscription, the specified configuration data will be used. There must be at least one line item with a recurring price to use `subscription_data`.
    """
    tax_id_collection: TaxIdCollection
    transfer_data: Optional[TransferData]
    """
    The account (if any) the payments will be attributed to for tax reporting, and where funds from each payment will be transferred to.
    """
    url: str
    """
    The public URL that can be shared with customers.
    """

    @classmethod
    def create(
        cls, **params: Unpack["PaymentLinkCreateParams"]
    ) -> "PaymentLink":
        """
        Creates a payment link.
        """
        return cast(
            "PaymentLink",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["PaymentLinkCreateParams"]
    ) -> "PaymentLink":
        """
        Creates a payment link.
        """
        return cast(
            "PaymentLink",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def list(
        cls, **params: Unpack["PaymentLinkListParams"]
    ) -> ListObject["PaymentLink"]:
        """
        Returns a list of your payment links.
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
        cls, **params: Unpack["PaymentLinkListParams"]
    ) -> ListObject["PaymentLink"]:
        """
        Returns a list of your payment links.
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
        cls,
        payment_link: str,
        **params: Unpack["PaymentLinkListLineItemsParams"],
    ) -> ListObject["LineItem"]:
        """
        When retrieving a payment link, there is an includable line_items property containing the first handful of those items. There is also a URL where you can retrieve the full (paginated) list of line items.
        """
        return cast(
            ListObject["LineItem"],
            cls._static_request(
                "get",
                "/v1/payment_links/{payment_link}/line_items".format(
                    payment_link=sanitize_id(payment_link)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def list_line_items(
        payment_link: str, **params: Unpack["PaymentLinkListLineItemsParams"]
    ) -> ListObject["LineItem"]:
        """
        When retrieving a payment link, there is an includable line_items property containing the first handful of those items. There is also a URL where you can retrieve the full (paginated) list of line items.
        """
        ...

    @overload
    def list_line_items(
        self, **params: Unpack["PaymentLinkListLineItemsParams"]
    ) -> ListObject["LineItem"]:
        """
        When retrieving a payment link, there is an includable line_items property containing the first handful of those items. There is also a URL where you can retrieve the full (paginated) list of line items.
        """
        ...

    @class_method_variant("_cls_list_line_items")
    def list_line_items(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["PaymentLinkListLineItemsParams"]
    ) -> ListObject["LineItem"]:
        """
        When retrieving a payment link, there is an includable line_items property containing the first handful of those items. There is also a URL where you can retrieve the full (paginated) list of line items.
        """
        return cast(
            ListObject["LineItem"],
            self._request(
                "get",
                "/v1/payment_links/{payment_link}/line_items".format(
                    payment_link=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_list_line_items_async(
        cls,
        payment_link: str,
        **params: Unpack["PaymentLinkListLineItemsParams"],
    ) -> ListObject["LineItem"]:
        """
        When retrieving a payment link, there is an includable line_items property containing the first handful of those items. There is also a URL where you can retrieve the full (paginated) list of line items.
        """
        return cast(
            ListObject["LineItem"],
            await cls._static_request_async(
                "get",
                "/v1/payment_links/{payment_link}/line_items".format(
                    payment_link=sanitize_id(payment_link)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def list_line_items_async(
        payment_link: str, **params: Unpack["PaymentLinkListLineItemsParams"]
    ) -> ListObject["LineItem"]:
        """
        When retrieving a payment link, there is an includable line_items property containing the first handful of those items. There is also a URL where you can retrieve the full (paginated) list of line items.
        """
        ...

    @overload
    async def list_line_items_async(
        self, **params: Unpack["PaymentLinkListLineItemsParams"]
    ) -> ListObject["LineItem"]:
        """
        When retrieving a payment link, there is an includable line_items property containing the first handful of those items. There is also a URL where you can retrieve the full (paginated) list of line items.
        """
        ...

    @class_method_variant("_cls_list_line_items_async")
    async def list_line_items_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["PaymentLinkListLineItemsParams"]
    ) -> ListObject["LineItem"]:
        """
        When retrieving a payment link, there is an includable line_items property containing the first handful of those items. There is also a URL where you can retrieve the full (paginated) list of line items.
        """
        return cast(
            ListObject["LineItem"],
            await self._request_async(
                "get",
                "/v1/payment_links/{payment_link}/line_items".format(
                    payment_link=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def modify(
        cls, id: str, **params: Unpack["PaymentLinkModifyParams"]
    ) -> "PaymentLink":
        """
        Updates a payment link.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "PaymentLink",
            cls._static_request(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    async def modify_async(
        cls, id: str, **params: Unpack["PaymentLinkModifyParams"]
    ) -> "PaymentLink":
        """
        Updates a payment link.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "PaymentLink",
            await cls._static_request_async(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["PaymentLinkRetrieveParams"]
    ) -> "PaymentLink":
        """
        Retrieve a payment link.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["PaymentLinkRetrieveParams"]
    ) -> "PaymentLink":
        """
        Retrieve a payment link.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    _inner_class_types = {
        "after_completion": AfterCompletion,
        "automatic_tax": AutomaticTax,
        "consent_collection": ConsentCollection,
        "custom_fields": CustomField,
        "custom_text": CustomText,
        "invoice_creation": InvoiceCreation,
        "name_collection": NameCollection,
        "optional_items": OptionalItem,
        "payment_intent_data": PaymentIntentData,
        "phone_number_collection": PhoneNumberCollection,
        "restrictions": Restrictions,
        "shipping_address_collection": ShippingAddressCollection,
        "shipping_options": ShippingOption,
        "subscription_data": SubscriptionData,
        "tax_id_collection": TaxIdCollection,
        "transfer_data": TransferData,
    }
