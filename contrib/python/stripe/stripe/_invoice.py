# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._deletable_api_resource import DeletableAPIResource
from stripe._expandable_field import ExpandableField
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._nested_resource_class_methods import nested_resource_class_methods
from stripe._search_result_object import SearchResultObject
from stripe._searchable_api_resource import SearchableAPIResource
from stripe._stripe_object import StripeObject
from stripe._updateable_api_resource import UpdateableAPIResource
from stripe._util import class_method_variant, sanitize_id
from typing import (
    AsyncIterator,
    ClassVar,
    Dict,
    Iterator,
    List,
    Optional,
    Union,
    cast,
    overload,
)
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._account import Account
    from stripe._application import Application
    from stripe._bank_account import BankAccount
    from stripe._card import Card as CardResource
    from stripe._customer import Customer
    from stripe._discount import Discount
    from stripe._invoice_line_item import InvoiceLineItem
    from stripe._invoice_payment import InvoicePayment
    from stripe._payment_intent import PaymentIntent
    from stripe._payment_method import PaymentMethod
    from stripe._setup_intent import SetupIntent
    from stripe._shipping_rate import ShippingRate
    from stripe._source import Source
    from stripe._subscription import Subscription
    from stripe._tax_id import TaxId
    from stripe._tax_rate import TaxRate
    from stripe.billing._credit_balance_transaction import (
        CreditBalanceTransaction,
    )
    from stripe.params._invoice_add_lines_params import InvoiceAddLinesParams
    from stripe.params._invoice_attach_payment_params import (
        InvoiceAttachPaymentParams,
    )
    from stripe.params._invoice_create_params import InvoiceCreateParams
    from stripe.params._invoice_create_preview_params import (
        InvoiceCreatePreviewParams,
    )
    from stripe.params._invoice_delete_params import InvoiceDeleteParams
    from stripe.params._invoice_finalize_invoice_params import (
        InvoiceFinalizeInvoiceParams,
    )
    from stripe.params._invoice_list_lines_params import InvoiceListLinesParams
    from stripe.params._invoice_list_params import InvoiceListParams
    from stripe.params._invoice_mark_uncollectible_params import (
        InvoiceMarkUncollectibleParams,
    )
    from stripe.params._invoice_modify_params import InvoiceModifyParams
    from stripe.params._invoice_pay_params import InvoicePayParams
    from stripe.params._invoice_remove_lines_params import (
        InvoiceRemoveLinesParams,
    )
    from stripe.params._invoice_retrieve_params import InvoiceRetrieveParams
    from stripe.params._invoice_search_params import InvoiceSearchParams
    from stripe.params._invoice_send_invoice_params import (
        InvoiceSendInvoiceParams,
    )
    from stripe.params._invoice_update_lines_params import (
        InvoiceUpdateLinesParams,
    )
    from stripe.params._invoice_void_invoice_params import (
        InvoiceVoidInvoiceParams,
    )
    from stripe.test_helpers._test_clock import TestClock


@nested_resource_class_methods("line")
class Invoice(
    CreateableAPIResource["Invoice"],
    DeletableAPIResource["Invoice"],
    ListableAPIResource["Invoice"],
    SearchableAPIResource["Invoice"],
    UpdateableAPIResource["Invoice"],
):
    """
    Invoices are statements of amounts owed by a customer, and are either
    generated one-off, or generated periodically from a subscription.

    They contain [invoice items](https://api.stripe.com#invoiceitems), and proration adjustments
    that may be caused by subscription upgrades/downgrades (if necessary).

    If your invoice is configured to be billed through automatic charges,
    Stripe automatically finalizes your invoice and attempts payment. Note
    that finalizing the invoice,
    [when automatic](https://docs.stripe.com/invoicing/integration/automatic-advancement-collection), does
    not happen immediately as the invoice is created. Stripe waits
    until one hour after the last webhook was successfully sent (or the last
    webhook timed out after failing). If you (and the platforms you may have
    connected to) have no webhooks configured, Stripe waits one hour after
    creation to finalize the invoice.

    If your invoice is configured to be billed by sending an email, then based on your
    [email settings](https://dashboard.stripe.com/account/billing/automatic),
    Stripe will email the invoice to your customer and await payment. These
    emails can contain a link to a hosted page to pay the invoice.

    Stripe applies any customer credit on the account before determining the
    amount due for the invoice (i.e., the amount that will be actually
    charged). If the amount due for the invoice is less than Stripe's [minimum allowed charge
    per currency](https://docs.stripe.com/docs/currencies#minimum-and-maximum-charge-amounts), the
    invoice is automatically marked paid, and we add the amount due to the
    customer's credit balance which is applied to the next invoice.

    More details on the customer's credit balance are
    [here](https://docs.stripe.com/billing/customer/balance).

    Related guide: [Send invoices to customers](https://docs.stripe.com/billing/invoices/sending)
    """

    OBJECT_NAME: ClassVar[Literal["invoice"]] = "invoice"

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

        disabled_reason: Optional[
            Literal[
                "finalization_requires_location_inputs",
                "finalization_system_error",
            ]
        ]
        """
        If Stripe disabled automatic tax, this enum describes why.
        """
        enabled: bool
        """
        Whether Stripe automatically computes tax on this invoice. Note that incompatible invoice items (invoice items with manually specified [tax rates](https://docs.stripe.com/api/tax_rates), negative amounts, or `tax_behavior=unspecified`) cannot be added to automatic tax invoices.
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
        The status of the most recent automated tax calculation for this invoice.
        """
        _inner_class_types = {"liability": Liability}

    class ConfirmationSecret(StripeObject):
        client_secret: str
        """
        The client_secret of the payment that Stripe creates for the invoice after finalization.
        """
        type: str
        """
        The type of client_secret. Currently this is always payment_intent, referencing the default payment_intent that Stripe creates during invoice finalization
        """

    class CustomField(StripeObject):
        name: str
        """
        The name of the custom field.
        """
        value: str
        """
        The value of the custom field.
        """

    class CustomerAddress(StripeObject):
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

    class CustomerShipping(StripeObject):
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

        address: Optional[Address]
        carrier: Optional[str]
        """
        The delivery service that shipped a physical product, such as Fedex, UPS, USPS, etc.
        """
        name: Optional[str]
        """
        Recipient name.
        """
        phone: Optional[str]
        """
        Recipient phone (including extension).
        """
        tracking_number: Optional[str]
        """
        The tracking number for a physical product, obtained from the delivery service. If multiple tracking numbers were generated for this purchase, please separate them with commas.
        """
        _inner_class_types = {"address": Address}

    class CustomerTaxId(StripeObject):
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

    class FromInvoice(StripeObject):
        action: str
        """
        The relation between this invoice and the cloned invoice
        """
        invoice: ExpandableField["Invoice"]
        """
        The invoice that was cloned.
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

    class LastFinalizationError(StripeObject):
        advice_code: Optional[str]
        """
        For card errors resulting from a card issuer decline, a short string indicating [how to proceed with an error](https://docs.stripe.com/declines#retrying-issuer-declines) if they provide one.
        """
        charge: Optional[str]
        """
        For card errors, the ID of the failed charge.
        """
        code: Optional[
            Literal[
                "account_closed",
                "account_country_invalid_address",
                "account_error_country_change_requires_additional_steps",
                "account_information_mismatch",
                "account_invalid",
                "account_number_invalid",
                "account_token_required_for_v2_account",
                "acss_debit_session_incomplete",
                "alipay_upgrade_required",
                "amount_too_large",
                "amount_too_small",
                "api_key_expired",
                "application_fees_not_allowed",
                "authentication_required",
                "balance_insufficient",
                "balance_invalid_parameter",
                "bank_account_bad_routing_numbers",
                "bank_account_declined",
                "bank_account_exists",
                "bank_account_restricted",
                "bank_account_unusable",
                "bank_account_unverified",
                "bank_account_verification_failed",
                "billing_invalid_mandate",
                "bitcoin_upgrade_required",
                "capture_charge_authorization_expired",
                "capture_unauthorized_payment",
                "card_decline_rate_limit_exceeded",
                "card_declined",
                "cardholder_phone_number_required",
                "charge_already_captured",
                "charge_already_refunded",
                "charge_disputed",
                "charge_exceeds_source_limit",
                "charge_exceeds_transaction_limit",
                "charge_expired_for_capture",
                "charge_invalid_parameter",
                "charge_not_refundable",
                "clearing_code_unsupported",
                "country_code_invalid",
                "country_unsupported",
                "coupon_expired",
                "customer_max_payment_methods",
                "customer_max_subscriptions",
                "customer_session_expired",
                "customer_tax_location_invalid",
                "debit_not_authorized",
                "email_invalid",
                "expired_card",
                "financial_connections_account_inactive",
                "financial_connections_account_pending_account_numbers",
                "financial_connections_account_unavailable_account_numbers",
                "financial_connections_no_successful_transaction_refresh",
                "forwarding_api_inactive",
                "forwarding_api_invalid_parameter",
                "forwarding_api_retryable_upstream_error",
                "forwarding_api_upstream_connection_error",
                "forwarding_api_upstream_connection_timeout",
                "forwarding_api_upstream_error",
                "idempotency_key_in_use",
                "incorrect_address",
                "incorrect_cvc",
                "incorrect_number",
                "incorrect_zip",
                "india_recurring_payment_mandate_canceled",
                "instant_payouts_config_disabled",
                "instant_payouts_currency_disabled",
                "instant_payouts_limit_exceeded",
                "instant_payouts_unsupported",
                "insufficient_funds",
                "intent_invalid_state",
                "intent_verification_method_missing",
                "invalid_card_type",
                "invalid_characters",
                "invalid_charge_amount",
                "invalid_cvc",
                "invalid_expiry_month",
                "invalid_expiry_year",
                "invalid_mandate_reference_prefix_format",
                "invalid_number",
                "invalid_source_usage",
                "invalid_tax_location",
                "invoice_no_customer_line_items",
                "invoice_no_payment_method_types",
                "invoice_no_subscription_line_items",
                "invoice_not_editable",
                "invoice_on_behalf_of_not_editable",
                "invoice_payment_intent_requires_action",
                "invoice_upcoming_none",
                "livemode_mismatch",
                "lock_timeout",
                "missing",
                "no_account",
                "not_allowed_on_standard_account",
                "out_of_inventory",
                "ownership_declaration_not_allowed",
                "parameter_invalid_empty",
                "parameter_invalid_integer",
                "parameter_invalid_string_blank",
                "parameter_invalid_string_empty",
                "parameter_missing",
                "parameter_unknown",
                "parameters_exclusive",
                "payment_intent_action_required",
                "payment_intent_authentication_failure",
                "payment_intent_incompatible_payment_method",
                "payment_intent_invalid_parameter",
                "payment_intent_konbini_rejected_confirmation_number",
                "payment_intent_mandate_invalid",
                "payment_intent_payment_attempt_expired",
                "payment_intent_payment_attempt_failed",
                "payment_intent_rate_limit_exceeded",
                "payment_intent_unexpected_state",
                "payment_method_bank_account_already_verified",
                "payment_method_bank_account_blocked",
                "payment_method_billing_details_address_missing",
                "payment_method_configuration_failures",
                "payment_method_currency_mismatch",
                "payment_method_customer_decline",
                "payment_method_invalid_parameter",
                "payment_method_invalid_parameter_testmode",
                "payment_method_microdeposit_failed",
                "payment_method_microdeposit_verification_amounts_invalid",
                "payment_method_microdeposit_verification_amounts_mismatch",
                "payment_method_microdeposit_verification_attempts_exceeded",
                "payment_method_microdeposit_verification_descriptor_code_mismatch",
                "payment_method_microdeposit_verification_timeout",
                "payment_method_not_available",
                "payment_method_provider_decline",
                "payment_method_provider_timeout",
                "payment_method_unactivated",
                "payment_method_unexpected_state",
                "payment_method_unsupported_type",
                "payout_reconciliation_not_ready",
                "payouts_limit_exceeded",
                "payouts_not_allowed",
                "platform_account_required",
                "platform_api_key_expired",
                "postal_code_invalid",
                "processing_error",
                "product_inactive",
                "progressive_onboarding_limit_exceeded",
                "rate_limit",
                "refer_to_customer",
                "refund_disputed_payment",
                "request_blocked",
                "resource_already_exists",
                "resource_missing",
                "return_intent_already_processed",
                "routing_number_invalid",
                "secret_key_required",
                "sepa_unsupported_account",
                "setup_attempt_failed",
                "setup_intent_authentication_failure",
                "setup_intent_invalid_parameter",
                "setup_intent_mandate_invalid",
                "setup_intent_mobile_wallet_unsupported",
                "setup_intent_setup_attempt_expired",
                "setup_intent_unexpected_state",
                "shipping_address_invalid",
                "shipping_calculation_failed",
                "sku_inactive",
                "state_unsupported",
                "status_transition_invalid",
                "storer_capability_missing",
                "storer_capability_not_active",
                "stripe_tax_inactive",
                "tax_id_invalid",
                "tax_id_prohibited",
                "taxes_calculation_failed",
                "terminal_location_country_unsupported",
                "terminal_reader_busy",
                "terminal_reader_hardware_fault",
                "terminal_reader_invalid_location_for_activation",
                "terminal_reader_invalid_location_for_payment",
                "terminal_reader_offline",
                "terminal_reader_timeout",
                "testmode_charges_only",
                "tls_version_unsupported",
                "token_already_used",
                "token_card_network_invalid",
                "token_in_use",
                "transfer_source_balance_parameters_mismatch",
                "transfers_not_allowed",
                "url_invalid",
            ]
        ]
        """
        For some errors that could be handled programmatically, a short string indicating the [error code](https://docs.stripe.com/error-codes) reported.
        """
        decline_code: Optional[str]
        """
        For card errors resulting from a card issuer decline, a short string indicating the [card issuer's reason for the decline](https://docs.stripe.com/declines#issuer-declines) if they provide one.
        """
        doc_url: Optional[str]
        """
        A URL to more information about the [error code](https://docs.stripe.com/error-codes) reported.
        """
        message: Optional[str]
        """
        A human-readable message providing more details about the error. For card errors, these messages can be shown to your users.
        """
        network_advice_code: Optional[str]
        """
        For card errors resulting from a card issuer decline, a 2 digit code which indicates the advice given to merchant by the card network on how to proceed with an error.
        """
        network_decline_code: Optional[str]
        """
        For payments declined by the network, an alphanumeric code which indicates the reason the payment failed.
        """
        param: Optional[str]
        """
        If the error is parameter-specific, the parameter related to the error. For example, you can use this to display a message near the correct form field.
        """
        payment_intent: Optional["PaymentIntent"]
        """
        A PaymentIntent guides you through the process of collecting a payment from your customer.
        We recommend that you create exactly one PaymentIntent for each order or
        customer session in your system. You can reference the PaymentIntent later to
        see the history of payment attempts for a particular session.

        A PaymentIntent transitions through
        [multiple statuses](https://docs.stripe.com/payments/paymentintents/lifecycle)
        throughout its lifetime as it interfaces with Stripe.js to perform
        authentication flows and ultimately creates at most one successful charge.

        Related guide: [Payment Intents API](https://docs.stripe.com/payments/payment-intents)
        """
        payment_method: Optional["PaymentMethod"]
        """
        PaymentMethod objects represent your customer's payment instruments.
        You can use them with [PaymentIntents](https://docs.stripe.com/payments/payment-intents) to collect payments or save them to
        Customer objects to store instrument details for future payments.

        Related guides: [Payment Methods](https://docs.stripe.com/payments/payment-methods) and [More Payment Scenarios](https://docs.stripe.com/payments/more-payment-scenarios).
        """
        payment_method_type: Optional[str]
        """
        If the error is specific to the type of payment method, the payment method type that had a problem. This field is only populated for invoice-related errors.
        """
        request_log_url: Optional[str]
        """
        A URL to the request log entry in your dashboard.
        """
        setup_intent: Optional["SetupIntent"]
        """
        A SetupIntent guides you through the process of setting up and saving a customer's payment credentials for future payments.
        For example, you can use a SetupIntent to set up and save your customer's card without immediately collecting a payment.
        Later, you can use [PaymentIntents](https://api.stripe.com#payment_intents) to drive the payment flow.

        Create a SetupIntent when you're ready to collect your customer's payment credentials.
        Don't maintain long-lived, unconfirmed SetupIntents because they might not be valid.
        The SetupIntent transitions through multiple [statuses](https://docs.stripe.com/payments/intents#intent-statuses) as it guides
        you through the setup process.

        Successful SetupIntents result in payment credentials that are optimized for future payments.
        For example, cardholders in [certain regions](https://stripe.com/guides/strong-customer-authentication) might need to be run through
        [Strong Customer Authentication](https://docs.stripe.com/strong-customer-authentication) during payment method collection
        to streamline later [off-session payments](https://docs.stripe.com/payments/setup-intents).
        If you use the SetupIntent with a [Customer](https://api.stripe.com#setup_intent_object-customer),
        it automatically attaches the resulting payment method to that Customer after successful setup.
        We recommend using SetupIntents or [setup_future_usage](https://api.stripe.com#payment_intent_object-setup_future_usage) on
        PaymentIntents to save payment methods to prevent saving invalid or unoptimized payment methods.

        By using SetupIntents, you can reduce friction for your customers, even as regulations change over time.

        Related guide: [Setup Intents API](https://docs.stripe.com/payments/setup-intents)
        """
        source: Optional[
            Union["Account", "BankAccount", "CardResource", "Source"]
        ]
        type: Literal[
            "api_error",
            "card_error",
            "idempotency_error",
            "invalid_request_error",
        ]
        """
        The type of error returned. One of `api_error`, `card_error`, `idempotency_error`, or `invalid_request_error`
        """

    class Parent(StripeObject):
        class QuoteDetails(StripeObject):
            quote: str
            """
            The quote that generated this invoice
            """

        class SubscriptionDetails(StripeObject):
            metadata: Optional[Dict[str, str]]
            """
            Set of [key-value pairs](https://docs.stripe.com/api/metadata) defined as subscription metadata when an invoice is created. Becomes an immutable snapshot of the subscription metadata at the time of invoice finalization.
             *Note: This attribute is populated only for invoices created on or after June 29, 2023.*
            """
            subscription: ExpandableField["Subscription"]
            """
            The subscription that generated this invoice
            """
            subscription_proration_date: Optional[int]
            """
            Only set for upcoming invoices that preview prorations. The time used to calculate prorations.
            """

        quote_details: Optional[QuoteDetails]
        """
        Details about the quote that generated this invoice
        """
        subscription_details: Optional[SubscriptionDetails]
        """
        Details about the subscription that generated this invoice
        """
        type: Literal["quote_details", "subscription_details"]
        """
        The type of parent that generated this invoice
        """
        _inner_class_types = {
            "quote_details": QuoteDetails,
            "subscription_details": SubscriptionDetails,
        }

    class PaymentSettings(StripeObject):
        class PaymentMethodOptions(StripeObject):
            class AcssDebit(StripeObject):
                class MandateOptions(StripeObject):
                    transaction_type: Optional[Literal["business", "personal"]]
                    """
                    Transaction type of the mandate.
                    """

                mandate_options: Optional[MandateOptions]
                verification_method: Optional[
                    Literal["automatic", "instant", "microdeposits"]
                ]
                """
                Bank account verification method.
                """
                _inner_class_types = {"mandate_options": MandateOptions}

            class Bancontact(StripeObject):
                preferred_language: Literal["de", "en", "fr", "nl"]
                """
                Preferred language of the Bancontact authorization page that the customer is redirected to.
                """

            class Card(StripeObject):
                class Installments(StripeObject):
                    enabled: Optional[bool]
                    """
                    Whether Installments are enabled for this Invoice.
                    """

                installments: Optional[Installments]
                request_three_d_secure: Optional[
                    Literal["any", "automatic", "challenge"]
                ]
                """
                We strongly recommend that you rely on our SCA Engine to automatically prompt your customers for authentication based on risk level and [other requirements](https://docs.stripe.com/strong-customer-authentication). However, if you wish to request 3D Secure based on logic from your own fraud engine, provide this option. Read our guide on [manually requesting 3D Secure](https://docs.stripe.com/payments/3d-secure/authentication-flow#manual-three-ds) for more information on how this configuration interacts with Radar and our SCA Engine.
                """
                _inner_class_types = {"installments": Installments}

            class CustomerBalance(StripeObject):
                class BankTransfer(StripeObject):
                    class EuBankTransfer(StripeObject):
                        country: Literal["BE", "DE", "ES", "FR", "IE", "NL"]
                        """
                        The desired country code of the bank account information. Permitted values include: `DE`, `FR`, `IE`, or `NL`.
                        """

                    eu_bank_transfer: Optional[EuBankTransfer]
                    type: Optional[str]
                    """
                    The bank transfer type that can be used for funding. Permitted values include: `eu_bank_transfer`, `gb_bank_transfer`, `jp_bank_transfer`, `mx_bank_transfer`, or `us_bank_transfer`.
                    """
                    _inner_class_types = {"eu_bank_transfer": EuBankTransfer}

                bank_transfer: Optional[BankTransfer]
                funding_type: Optional[Literal["bank_transfer"]]
                """
                The funding method type to be used when there are not enough funds in the customer balance. Permitted values include: `bank_transfer`.
                """
                _inner_class_types = {"bank_transfer": BankTransfer}

            class Konbini(StripeObject):
                pass

            class Payto(StripeObject):
                class MandateOptions(StripeObject):
                    amount: Optional[int]
                    """
                    The maximum amount that can be collected in a single invoice. If you don't specify a maximum, then there is no limit.
                    """
                    amount_type: Optional[Literal["fixed", "maximum"]]
                    """
                    Only `maximum` is supported.
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

                mandate_options: Optional[MandateOptions]
                _inner_class_types = {"mandate_options": MandateOptions}

            class SepaDebit(StripeObject):
                pass

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
                    _inner_class_types = {"filters": Filters}

                financial_connections: Optional[FinancialConnections]
                verification_method: Optional[
                    Literal["automatic", "instant", "microdeposits"]
                ]
                """
                Bank account verification method.
                """
                _inner_class_types = {
                    "financial_connections": FinancialConnections,
                }

            acss_debit: Optional[AcssDebit]
            """
            If paying by `acss_debit`, this sub-hash contains details about the Canadian pre-authorized debit payment method options to pass to the invoice's PaymentIntent.
            """
            bancontact: Optional[Bancontact]
            """
            If paying by `bancontact`, this sub-hash contains details about the Bancontact payment method options to pass to the invoice's PaymentIntent.
            """
            card: Optional[Card]
            """
            If paying by `card`, this sub-hash contains details about the Card payment method options to pass to the invoice's PaymentIntent.
            """
            customer_balance: Optional[CustomerBalance]
            """
            If paying by `customer_balance`, this sub-hash contains details about the Bank transfer payment method options to pass to the invoice's PaymentIntent.
            """
            konbini: Optional[Konbini]
            """
            If paying by `konbini`, this sub-hash contains details about the Konbini payment method options to pass to the invoice's PaymentIntent.
            """
            payto: Optional[Payto]
            """
            If paying by `payto`, this sub-hash contains details about the PayTo payment method options to pass to the invoice's PaymentIntent.
            """
            sepa_debit: Optional[SepaDebit]
            """
            If paying by `sepa_debit`, this sub-hash contains details about the SEPA Direct Debit payment method options to pass to the invoice's PaymentIntent.
            """
            us_bank_account: Optional[UsBankAccount]
            """
            If paying by `us_bank_account`, this sub-hash contains details about the ACH direct debit payment method options to pass to the invoice's PaymentIntent.
            """
            _inner_class_types = {
                "acss_debit": AcssDebit,
                "bancontact": Bancontact,
                "card": Card,
                "customer_balance": CustomerBalance,
                "konbini": Konbini,
                "payto": Payto,
                "sepa_debit": SepaDebit,
                "us_bank_account": UsBankAccount,
            }

        default_mandate: Optional[str]
        """
        ID of the mandate to be used for this invoice. It must correspond to the payment method used to pay the invoice, including the invoice's default_payment_method or default_source, if set.
        """
        payment_method_options: Optional[PaymentMethodOptions]
        """
        Payment-method-specific configuration to provide to the invoice's PaymentIntent.
        """
        payment_method_types: Optional[
            List[
                Literal[
                    "ach_credit_transfer",
                    "ach_debit",
                    "acss_debit",
                    "affirm",
                    "amazon_pay",
                    "au_becs_debit",
                    "bacs_debit",
                    "bancontact",
                    "boleto",
                    "card",
                    "cashapp",
                    "crypto",
                    "custom",
                    "customer_balance",
                    "eps",
                    "fpx",
                    "giropay",
                    "grabpay",
                    "ideal",
                    "jp_credit_transfer",
                    "kakao_pay",
                    "klarna",
                    "konbini",
                    "kr_card",
                    "link",
                    "multibanco",
                    "naver_pay",
                    "nz_bank_account",
                    "p24",
                    "pay_by_bank",
                    "payco",
                    "paynow",
                    "paypal",
                    "payto",
                    "promptpay",
                    "revolut_pay",
                    "sepa_credit_transfer",
                    "sepa_debit",
                    "sofort",
                    "swish",
                    "us_bank_account",
                    "wechat_pay",
                ]
            ]
        ]
        """
        The list of payment method types (e.g. card) to provide to the invoice's PaymentIntent. If not set, Stripe attempts to automatically determine the types to use by looking at the invoice's default payment method, the subscription's default payment method, the customer's default payment method, and your [invoice template settings](https://dashboard.stripe.com/settings/billing/invoice).
        """
        _inner_class_types = {"payment_method_options": PaymentMethodOptions}

    class Rendering(StripeObject):
        class Pdf(StripeObject):
            page_size: Optional[Literal["a4", "auto", "letter"]]
            """
            Page size of invoice pdf. Options include a4, letter, and auto. If set to auto, page size will be switched to a4 or letter based on customer locale.
            """

        amount_tax_display: Optional[str]
        """
        How line-item prices and amounts will be displayed with respect to tax on invoice PDFs.
        """
        pdf: Optional[Pdf]
        """
        Invoice pdf rendering options
        """
        template: Optional[str]
        """
        ID of the rendering template that the invoice is formatted by.
        """
        template_version: Optional[int]
        """
        Version of the rendering template that the invoice is using.
        """
        _inner_class_types = {"pdf": Pdf}

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
        Total shipping cost before any taxes are applied.
        """
        amount_tax: int
        """
        Total tax amount applied due to shipping costs. If no tax was applied, defaults to 0.
        """
        amount_total: int
        """
        Total shipping cost after taxes are applied.
        """
        shipping_rate: Optional[ExpandableField["ShippingRate"]]
        """
        The ID of the ShippingRate for this invoice.
        """
        taxes: Optional[List[Tax]]
        """
        The taxes applied to the shipping rate.
        """
        _inner_class_types = {"taxes": Tax}

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

        address: Optional[Address]
        carrier: Optional[str]
        """
        The delivery service that shipped a physical product, such as Fedex, UPS, USPS, etc.
        """
        name: Optional[str]
        """
        Recipient name.
        """
        phone: Optional[str]
        """
        Recipient phone (including extension).
        """
        tracking_number: Optional[str]
        """
        The tracking number for a physical product, obtained from the delivery service. If multiple tracking numbers were generated for this purchase, please separate them with commas.
        """
        _inner_class_types = {"address": Address}

    class StatusTransitions(StripeObject):
        finalized_at: Optional[int]
        """
        The time that the invoice draft was finalized.
        """
        marked_uncollectible_at: Optional[int]
        """
        The time that the invoice was marked uncollectible.
        """
        paid_at: Optional[int]
        """
        The time that the invoice was paid.
        """
        voided_at: Optional[int]
        """
        The time that the invoice was voided.
        """

    class ThresholdReason(StripeObject):
        class ItemReason(StripeObject):
            line_item_ids: List[str]
            """
            The IDs of the line items that triggered the threshold invoice.
            """
            usage_gte: int
            """
            The quantity threshold boundary that applied to the given line item.
            """

        amount_gte: Optional[int]
        """
        The total invoice amount threshold boundary if it triggered the threshold invoice.
        """
        item_reasons: List[ItemReason]
        """
        Indicates which line items triggered a threshold invoice.
        """
        _inner_class_types = {"item_reasons": ItemReason}

    class TotalDiscountAmount(StripeObject):
        amount: int
        """
        The amount, in cents (or local equivalent), of the discount.
        """
        discount: ExpandableField["Discount"]
        """
        The discount that was applied to get this discount amount.
        """

    class TotalPretaxCreditAmount(StripeObject):
        amount: int
        """
        The amount, in cents (or local equivalent), of the pretax credit amount.
        """
        credit_balance_transaction: Optional[
            ExpandableField["CreditBalanceTransaction"]
        ]
        """
        The credit balance transaction that was applied to get this pretax credit amount.
        """
        discount: Optional[ExpandableField["Discount"]]
        """
        The discount that was applied to get this pretax credit amount.
        """
        type: Literal["credit_balance_transaction", "discount"]
        """
        Type of the pretax credit amount referenced.
        """

    class TotalTax(StripeObject):
        class TaxRateDetails(StripeObject):
            tax_rate: str
            """
            ID of the tax rate
            """

        amount: int
        """
        The amount of the tax, in cents (or local equivalent).
        """
        tax_behavior: Literal["exclusive", "inclusive"]
        """
        Whether this tax is inclusive or exclusive.
        """
        tax_rate_details: Optional[TaxRateDetails]
        """
        Additional details about the tax rate. Only present when `type` is `tax_rate_details`.
        """
        taxability_reason: Literal[
            "customer_exempt",
            "not_available",
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
        taxable_amount: Optional[int]
        """
        The amount on which tax is calculated, in cents (or local equivalent).
        """
        type: Literal["tax_rate_details"]
        """
        The type of tax information.
        """
        _inner_class_types = {"tax_rate_details": TaxRateDetails}

    account_country: Optional[str]
    """
    The country of the business associated with this invoice, most often the business creating the invoice.
    """
    account_name: Optional[str]
    """
    The public name of the business associated with this invoice, most often the business creating the invoice.
    """
    account_tax_ids: Optional[List[ExpandableField["TaxId"]]]
    """
    The account tax IDs associated with the invoice. Only editable when the invoice is a draft.
    """
    amount_due: int
    """
    Final amount due at this time for this invoice. If the invoice's total is smaller than the minimum charge amount, for example, or if there is account credit that can be applied to the invoice, the `amount_due` may be 0. If there is a positive `starting_balance` for the invoice (the customer owes money), the `amount_due` will also take that into account. The charge that gets generated for the invoice will be for the amount specified in `amount_due`.
    """
    amount_overpaid: int
    """
    Amount that was overpaid on the invoice. The amount overpaid is credited to the customer's credit balance.
    """
    amount_paid: int
    """
    The amount, in cents (or local equivalent), that was paid.
    """
    amount_remaining: int
    """
    The difference between amount_due and amount_paid, in cents (or local equivalent).
    """
    amount_shipping: int
    """
    This is the sum of all the shipping amounts.
    """
    application: Optional[ExpandableField["Application"]]
    """
    ID of the Connect Application that created the invoice.
    """
    attempt_count: int
    """
    Number of payment attempts made for this invoice, from the perspective of the payment retry schedule. Any payment attempt counts as the first attempt, and subsequently only automatic retries increment the attempt count. In other words, manual payment attempts after the first attempt do not affect the retry schedule. If a failure is returned with a non-retryable return code, the invoice can no longer be retried unless a new payment method is obtained. Retries will continue to be scheduled, and attempt_count will continue to increment, but retries will only be executed if a new payment method is obtained.
    """
    attempted: bool
    """
    Whether an attempt has been made to pay the invoice. An invoice is not attempted until 1 hour after the `invoice.created` webhook, for example, so you might not want to display that invoice as unpaid to your users.
    """
    auto_advance: Optional[bool]
    """
    Controls whether Stripe performs [automatic collection](https://docs.stripe.com/invoicing/integration/automatic-advancement-collection) of the invoice. If `false`, the invoice's state doesn't automatically advance without an explicit action.
    """
    automatic_tax: AutomaticTax
    automatically_finalizes_at: Optional[int]
    """
    The time when this invoice is currently scheduled to be automatically finalized. The field will be `null` if the invoice is not scheduled to finalize in the future. If the invoice is not in the draft state, this field will always be `null` - see `finalized_at` for the time when an already-finalized invoice was finalized.
    """
    billing_reason: Optional[
        Literal[
            "automatic_pending_invoice_item_invoice",
            "manual",
            "quote_accept",
            "subscription",
            "subscription_create",
            "subscription_cycle",
            "subscription_threshold",
            "subscription_update",
            "upcoming",
        ]
    ]
    """
    Indicates the reason why the invoice was created.

    * `manual`: Unrelated to a subscription, for example, created via the invoice editor.
    * `subscription`: No longer in use. Applies to subscriptions from before May 2018 where no distinction was made between updates, cycles, and thresholds.
    * `subscription_create`: A new subscription was created.
    * `subscription_cycle`: A subscription advanced into a new period.
    * `subscription_threshold`: A subscription reached a billing threshold.
    * `subscription_update`: A subscription was updated.
    * `upcoming`: Reserved for upcoming invoices created through the Create Preview Invoice API or when an `invoice.upcoming` event is generated for an upcoming invoice on a subscription.
    """
    collection_method: Literal["charge_automatically", "send_invoice"]
    """
    Either `charge_automatically`, or `send_invoice`. When charging automatically, Stripe will attempt to pay this invoice using the default source attached to the customer. When sending an invoice, Stripe will email this invoice to the customer with payment instructions.
    """
    confirmation_secret: Optional[ConfirmationSecret]
    """
    The confirmation secret associated with this invoice. Currently, this contains the client_secret of the PaymentIntent that Stripe creates during invoice finalization.
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    custom_fields: Optional[List[CustomField]]
    """
    Custom fields displayed on the invoice.
    """
    customer: Optional[ExpandableField["Customer"]]
    """
    The ID of the customer to bill.
    """
    customer_account: Optional[str]
    """
    The ID of the account representing the customer to bill.
    """
    customer_address: Optional[CustomerAddress]
    """
    The customer's address. Until the invoice is finalized, this field will equal `customer.address`. Once the invoice is finalized, this field will no longer be updated.
    """
    customer_email: Optional[str]
    """
    The customer's email. Until the invoice is finalized, this field will equal `customer.email`. Once the invoice is finalized, this field will no longer be updated.
    """
    customer_name: Optional[str]
    """
    The customer's name. Until the invoice is finalized, this field will equal `customer.name`. Once the invoice is finalized, this field will no longer be updated.
    """
    customer_phone: Optional[str]
    """
    The customer's phone number. Until the invoice is finalized, this field will equal `customer.phone`. Once the invoice is finalized, this field will no longer be updated.
    """
    customer_shipping: Optional[CustomerShipping]
    """
    The customer's shipping information. Until the invoice is finalized, this field will equal `customer.shipping`. Once the invoice is finalized, this field will no longer be updated.
    """
    customer_tax_exempt: Optional[Literal["exempt", "none", "reverse"]]
    """
    The customer's tax exempt status. Until the invoice is finalized, this field will equal `customer.tax_exempt`. Once the invoice is finalized, this field will no longer be updated.
    """
    customer_tax_ids: Optional[List[CustomerTaxId]]
    """
    The customer's tax IDs. Until the invoice is finalized, this field will contain the same tax IDs as `customer.tax_ids`. Once the invoice is finalized, this field will no longer be updated.
    """
    default_payment_method: Optional[ExpandableField["PaymentMethod"]]
    """
    ID of the default payment method for the invoice. It must belong to the customer associated with the invoice. If not set, defaults to the subscription's default payment method, if any, or to the default payment method in the customer's invoice settings.
    """
    default_source: Optional[
        ExpandableField[
            Union["Account", "BankAccount", "CardResource", "Source"]
        ]
    ]
    """
    ID of the default payment source for the invoice. It must belong to the customer associated with the invoice and be in a chargeable state. If not set, defaults to the subscription's default source, if any, or to the customer's default source.
    """
    default_tax_rates: List["TaxRate"]
    """
    The tax rates applied to this invoice, if any.
    """
    deleted: Optional[Literal[True]]
    """
    Always true for a deleted object
    """
    description: Optional[str]
    """
    An arbitrary string attached to the object. Often useful for displaying to users. Referenced as 'memo' in the Dashboard.
    """
    discounts: List[ExpandableField["Discount"]]
    """
    The discounts applied to the invoice. Line item discounts are applied before invoice discounts. Use `expand[]=discounts` to expand each discount.
    """
    due_date: Optional[int]
    """
    The date on which payment for this invoice is due. This value will be `null` for invoices where `collection_method=charge_automatically`.
    """
    effective_at: Optional[int]
    """
    The date when this invoice is in effect. Same as `finalized_at` unless overwritten. When defined, this value replaces the system-generated 'Date of issue' printed on the invoice PDF and receipt.
    """
    ending_balance: Optional[int]
    """
    Ending customer balance after the invoice is finalized. Invoices are finalized approximately an hour after successful webhook delivery or when payment collection is attempted for the invoice. If the invoice has not been finalized yet, this will be null.
    """
    footer: Optional[str]
    """
    Footer displayed on the invoice.
    """
    from_invoice: Optional[FromInvoice]
    """
    Details of the invoice that was cloned. See the [revision documentation](https://docs.stripe.com/invoicing/invoice-revisions) for more details.
    """
    hosted_invoice_url: Optional[str]
    """
    The URL for the hosted invoice page, which allows customers to view and pay an invoice. If the invoice has not been finalized yet, this will be null.
    """
    id: str
    """
    Unique identifier for the object. For preview invoices created using the [create preview](https://stripe.com/docs/api/invoices/create_preview) endpoint, this id will be prefixed with `upcoming_in`.
    """
    invoice_pdf: Optional[str]
    """
    The link to download the PDF for the invoice. If the invoice has not been finalized yet, this will be null.
    """
    issuer: Issuer
    last_finalization_error: Optional[LastFinalizationError]
    """
    The error encountered during the previous attempt to finalize the invoice. This field is cleared when the invoice is successfully finalized.
    """
    latest_revision: Optional[ExpandableField["Invoice"]]
    """
    The ID of the most recent non-draft revision of this invoice
    """
    lines: ListObject["InvoiceLineItem"]
    """
    The individual line items that make up the invoice. `lines` is sorted as follows: (1) pending invoice items (including prorations) in reverse chronological order, (2) subscription items in reverse chronological order, and (3) invoice items added after invoice creation in chronological order.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    metadata: Optional[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    next_payment_attempt: Optional[int]
    """
    The time at which payment will next be attempted. This value will be `null` for invoices where `collection_method=send_invoice`.
    """
    number: Optional[str]
    """
    A unique, identifying string that appears on emails sent to the customer for this invoice. This starts with the customer's unique invoice_prefix if it is specified.
    """
    object: Literal["invoice"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    on_behalf_of: Optional[ExpandableField["Account"]]
    """
    The account (if any) for which the funds of the invoice payment are intended. If set, the invoice will be presented with the branding and support information of the specified account. See the [Invoices with Connect](https://docs.stripe.com/billing/invoices/connect) documentation for details.
    """
    parent: Optional[Parent]
    """
    The parent that generated this invoice
    """
    payment_settings: PaymentSettings
    payments: Optional[ListObject["InvoicePayment"]]
    """
    Payments for this invoice. Use [invoice payment](https://docs.stripe.com/api/invoice-payment) to get more details.
    """
    period_end: int
    """
    End of the usage period during which invoice items were added to this invoice. This looks back one period for a subscription invoice. Use the [line item period](https://docs.stripe.com/api/invoices/line_item#invoice_line_item_object-period) to get the service period for each price.
    """
    period_start: int
    """
    Start of the usage period during which invoice items were added to this invoice. This looks back one period for a subscription invoice. Use the [line item period](https://docs.stripe.com/api/invoices/line_item#invoice_line_item_object-period) to get the service period for each price.
    """
    post_payment_credit_notes_amount: int
    """
    Total amount of all post-payment credit notes issued for this invoice.
    """
    pre_payment_credit_notes_amount: int
    """
    Total amount of all pre-payment credit notes issued for this invoice.
    """
    receipt_number: Optional[str]
    """
    This is the transaction number that appears on email receipts sent for this invoice.
    """
    rendering: Optional[Rendering]
    """
    The rendering-related settings that control how the invoice is displayed on customer-facing surfaces such as PDF and Hosted Invoice Page.
    """
    shipping_cost: Optional[ShippingCost]
    """
    The details of the cost of shipping, including the ShippingRate applied on the invoice.
    """
    shipping_details: Optional[ShippingDetails]
    """
    Shipping details for the invoice. The Invoice PDF will use the `shipping_details` value if it is set, otherwise the PDF will render the shipping address from the customer.
    """
    starting_balance: int
    """
    Starting customer balance before the invoice is finalized. If the invoice has not been finalized yet, this will be the current customer balance. For revision invoices, this also includes any customer balance that was applied to the original invoice.
    """
    statement_descriptor: Optional[str]
    """
    Extra information about an invoice for the customer's credit card statement.
    """
    status: Optional[Literal["draft", "open", "paid", "uncollectible", "void"]]
    """
    The status of the invoice, one of `draft`, `open`, `paid`, `uncollectible`, or `void`. [Learn more](https://docs.stripe.com/billing/invoices/workflow#workflow-overview)
    """
    status_transitions: StatusTransitions
    subtotal: int
    """
    Total of all subscriptions, invoice items, and prorations on the invoice before any invoice level discount or exclusive tax is applied. Item discounts are already incorporated
    """
    subtotal_excluding_tax: Optional[int]
    """
    The integer amount in cents (or local equivalent) representing the subtotal of the invoice before any invoice level discount or tax is applied. Item discounts are already incorporated
    """
    test_clock: Optional[ExpandableField["TestClock"]]
    """
    ID of the test clock this invoice belongs to.
    """
    threshold_reason: Optional[ThresholdReason]
    total: int
    """
    Total after discounts and taxes.
    """
    total_discount_amounts: Optional[List[TotalDiscountAmount]]
    """
    The aggregate amounts calculated per discount across all line items.
    """
    total_excluding_tax: Optional[int]
    """
    The integer amount in cents (or local equivalent) representing the total amount of the invoice including all discounts but excluding all tax.
    """
    total_pretax_credit_amounts: Optional[List[TotalPretaxCreditAmount]]
    """
    Contains pretax credit amounts (ex: discount, credit grants, etc) that apply to this invoice. This is a combined list of total_pretax_credit_amounts across all invoice line items.
    """
    total_taxes: Optional[List[TotalTax]]
    """
    The aggregate tax information of all line items.
    """
    webhooks_delivered_at: Optional[int]
    """
    Invoices are automatically paid or sent 1 hour after webhooks are delivered, or until all webhook delivery attempts have [been exhausted](https://docs.stripe.com/billing/webhooks#understand). This field tracks the time when webhooks for this invoice were successfully delivered. If the invoice had no webhooks to deliver, this will be set while the invoice is being created.
    """

    @classmethod
    def _cls_add_lines(
        cls, invoice: str, **params: Unpack["InvoiceAddLinesParams"]
    ) -> "Invoice":
        """
        Adds multiple line items to an invoice. This is only possible when an invoice is still a draft.
        """
        return cast(
            "Invoice",
            cls._static_request(
                "post",
                "/v1/invoices/{invoice}/add_lines".format(
                    invoice=sanitize_id(invoice)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def add_lines(
        invoice: str, **params: Unpack["InvoiceAddLinesParams"]
    ) -> "Invoice":
        """
        Adds multiple line items to an invoice. This is only possible when an invoice is still a draft.
        """
        ...

    @overload
    def add_lines(
        self, **params: Unpack["InvoiceAddLinesParams"]
    ) -> "Invoice":
        """
        Adds multiple line items to an invoice. This is only possible when an invoice is still a draft.
        """
        ...

    @class_method_variant("_cls_add_lines")
    def add_lines(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["InvoiceAddLinesParams"]
    ) -> "Invoice":
        """
        Adds multiple line items to an invoice. This is only possible when an invoice is still a draft.
        """
        return cast(
            "Invoice",
            self._request(
                "post",
                "/v1/invoices/{invoice}/add_lines".format(
                    invoice=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_add_lines_async(
        cls, invoice: str, **params: Unpack["InvoiceAddLinesParams"]
    ) -> "Invoice":
        """
        Adds multiple line items to an invoice. This is only possible when an invoice is still a draft.
        """
        return cast(
            "Invoice",
            await cls._static_request_async(
                "post",
                "/v1/invoices/{invoice}/add_lines".format(
                    invoice=sanitize_id(invoice)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def add_lines_async(
        invoice: str, **params: Unpack["InvoiceAddLinesParams"]
    ) -> "Invoice":
        """
        Adds multiple line items to an invoice. This is only possible when an invoice is still a draft.
        """
        ...

    @overload
    async def add_lines_async(
        self, **params: Unpack["InvoiceAddLinesParams"]
    ) -> "Invoice":
        """
        Adds multiple line items to an invoice. This is only possible when an invoice is still a draft.
        """
        ...

    @class_method_variant("_cls_add_lines_async")
    async def add_lines_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["InvoiceAddLinesParams"]
    ) -> "Invoice":
        """
        Adds multiple line items to an invoice. This is only possible when an invoice is still a draft.
        """
        return cast(
            "Invoice",
            await self._request_async(
                "post",
                "/v1/invoices/{invoice}/add_lines".format(
                    invoice=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def _cls_attach_payment(
        cls, invoice: str, **params: Unpack["InvoiceAttachPaymentParams"]
    ) -> "Invoice":
        """
        Attaches a PaymentIntent or an Out of Band Payment to the invoice, adding it to the list of payments.

        For the PaymentIntent, when the PaymentIntent's status changes to succeeded, the payment is credited
        to the invoice, increasing its amount_paid. When the invoice is fully paid, the
        invoice's status becomes paid.

        If the PaymentIntent's status is already succeeded when it's attached, it's
        credited to the invoice immediately.

        See: [Partial payments](https://docs.stripe.com/docs/invoicing/partial-payments) to learn more.
        """
        return cast(
            "Invoice",
            cls._static_request(
                "post",
                "/v1/invoices/{invoice}/attach_payment".format(
                    invoice=sanitize_id(invoice)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def attach_payment(
        invoice: str, **params: Unpack["InvoiceAttachPaymentParams"]
    ) -> "Invoice":
        """
        Attaches a PaymentIntent or an Out of Band Payment to the invoice, adding it to the list of payments.

        For the PaymentIntent, when the PaymentIntent's status changes to succeeded, the payment is credited
        to the invoice, increasing its amount_paid. When the invoice is fully paid, the
        invoice's status becomes paid.

        If the PaymentIntent's status is already succeeded when it's attached, it's
        credited to the invoice immediately.

        See: [Partial payments](https://docs.stripe.com/docs/invoicing/partial-payments) to learn more.
        """
        ...

    @overload
    def attach_payment(
        self, **params: Unpack["InvoiceAttachPaymentParams"]
    ) -> "Invoice":
        """
        Attaches a PaymentIntent or an Out of Band Payment to the invoice, adding it to the list of payments.

        For the PaymentIntent, when the PaymentIntent's status changes to succeeded, the payment is credited
        to the invoice, increasing its amount_paid. When the invoice is fully paid, the
        invoice's status becomes paid.

        If the PaymentIntent's status is already succeeded when it's attached, it's
        credited to the invoice immediately.

        See: [Partial payments](https://docs.stripe.com/docs/invoicing/partial-payments) to learn more.
        """
        ...

    @class_method_variant("_cls_attach_payment")
    def attach_payment(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["InvoiceAttachPaymentParams"]
    ) -> "Invoice":
        """
        Attaches a PaymentIntent or an Out of Band Payment to the invoice, adding it to the list of payments.

        For the PaymentIntent, when the PaymentIntent's status changes to succeeded, the payment is credited
        to the invoice, increasing its amount_paid. When the invoice is fully paid, the
        invoice's status becomes paid.

        If the PaymentIntent's status is already succeeded when it's attached, it's
        credited to the invoice immediately.

        See: [Partial payments](https://docs.stripe.com/docs/invoicing/partial-payments) to learn more.
        """
        return cast(
            "Invoice",
            self._request(
                "post",
                "/v1/invoices/{invoice}/attach_payment".format(
                    invoice=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_attach_payment_async(
        cls, invoice: str, **params: Unpack["InvoiceAttachPaymentParams"]
    ) -> "Invoice":
        """
        Attaches a PaymentIntent or an Out of Band Payment to the invoice, adding it to the list of payments.

        For the PaymentIntent, when the PaymentIntent's status changes to succeeded, the payment is credited
        to the invoice, increasing its amount_paid. When the invoice is fully paid, the
        invoice's status becomes paid.

        If the PaymentIntent's status is already succeeded when it's attached, it's
        credited to the invoice immediately.

        See: [Partial payments](https://docs.stripe.com/docs/invoicing/partial-payments) to learn more.
        """
        return cast(
            "Invoice",
            await cls._static_request_async(
                "post",
                "/v1/invoices/{invoice}/attach_payment".format(
                    invoice=sanitize_id(invoice)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def attach_payment_async(
        invoice: str, **params: Unpack["InvoiceAttachPaymentParams"]
    ) -> "Invoice":
        """
        Attaches a PaymentIntent or an Out of Band Payment to the invoice, adding it to the list of payments.

        For the PaymentIntent, when the PaymentIntent's status changes to succeeded, the payment is credited
        to the invoice, increasing its amount_paid. When the invoice is fully paid, the
        invoice's status becomes paid.

        If the PaymentIntent's status is already succeeded when it's attached, it's
        credited to the invoice immediately.

        See: [Partial payments](https://docs.stripe.com/docs/invoicing/partial-payments) to learn more.
        """
        ...

    @overload
    async def attach_payment_async(
        self, **params: Unpack["InvoiceAttachPaymentParams"]
    ) -> "Invoice":
        """
        Attaches a PaymentIntent or an Out of Band Payment to the invoice, adding it to the list of payments.

        For the PaymentIntent, when the PaymentIntent's status changes to succeeded, the payment is credited
        to the invoice, increasing its amount_paid. When the invoice is fully paid, the
        invoice's status becomes paid.

        If the PaymentIntent's status is already succeeded when it's attached, it's
        credited to the invoice immediately.

        See: [Partial payments](https://docs.stripe.com/docs/invoicing/partial-payments) to learn more.
        """
        ...

    @class_method_variant("_cls_attach_payment_async")
    async def attach_payment_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["InvoiceAttachPaymentParams"]
    ) -> "Invoice":
        """
        Attaches a PaymentIntent or an Out of Band Payment to the invoice, adding it to the list of payments.

        For the PaymentIntent, when the PaymentIntent's status changes to succeeded, the payment is credited
        to the invoice, increasing its amount_paid. When the invoice is fully paid, the
        invoice's status becomes paid.

        If the PaymentIntent's status is already succeeded when it's attached, it's
        credited to the invoice immediately.

        See: [Partial payments](https://docs.stripe.com/docs/invoicing/partial-payments) to learn more.
        """
        return cast(
            "Invoice",
            await self._request_async(
                "post",
                "/v1/invoices/{invoice}/attach_payment".format(
                    invoice=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def create(cls, **params: Unpack["InvoiceCreateParams"]) -> "Invoice":
        """
        This endpoint creates a draft invoice for a given customer. The invoice remains a draft until you [finalize the invoice, which allows you to [pay](https://docs.stripe.com/api#finalize_invoice) or <a href="/api/invoices/send">send](https://docs.stripe.com/api/invoices/pay) the invoice to your customers.
        """
        return cast(
            "Invoice",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["InvoiceCreateParams"]
    ) -> "Invoice":
        """
        This endpoint creates a draft invoice for a given customer. The invoice remains a draft until you [finalize the invoice, which allows you to [pay](https://docs.stripe.com/api#finalize_invoice) or <a href="/api/invoices/send">send](https://docs.stripe.com/api/invoices/pay) the invoice to your customers.
        """
        return cast(
            "Invoice",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def create_preview(
        cls, **params: Unpack["InvoiceCreatePreviewParams"]
    ) -> "Invoice":
        """
        At any time, you can preview the upcoming invoice for a subscription or subscription schedule. This will show you all the charges that are pending, including subscription renewal charges, invoice item charges, etc. It will also show you any discounts that are applicable to the invoice.

        You can also preview the effects of creating or updating a subscription or subscription schedule, including a preview of any prorations that will take place. To ensure that the actual proration is calculated exactly the same as the previewed proration, you should pass the subscription_details.proration_date parameter when doing the actual subscription update.

        The recommended way to get only the prorations being previewed on the invoice is to consider line items where parent.subscription_item_details.proration is true.

        Note that when you are viewing an upcoming invoice, you are simply viewing a preview – the invoice has not yet been created. As such, the upcoming invoice will not show up in invoice listing calls, and you cannot use the API to pay or edit the invoice. If you want to change the amount that your customer will be billed, you can add, remove, or update pending invoice items, or update the customer's discount.

        Note: Currency conversion calculations use the latest exchange rates. Exchange rates may vary between the time of the preview and the time of the actual invoice creation. [Learn more](https://docs.stripe.com/currencies/conversions)
        """
        return cast(
            "Invoice",
            cls._static_request(
                "post",
                "/v1/invoices/create_preview",
                params=params,
            ),
        )

    @classmethod
    async def create_preview_async(
        cls, **params: Unpack["InvoiceCreatePreviewParams"]
    ) -> "Invoice":
        """
        At any time, you can preview the upcoming invoice for a subscription or subscription schedule. This will show you all the charges that are pending, including subscription renewal charges, invoice item charges, etc. It will also show you any discounts that are applicable to the invoice.

        You can also preview the effects of creating or updating a subscription or subscription schedule, including a preview of any prorations that will take place. To ensure that the actual proration is calculated exactly the same as the previewed proration, you should pass the subscription_details.proration_date parameter when doing the actual subscription update.

        The recommended way to get only the prorations being previewed on the invoice is to consider line items where parent.subscription_item_details.proration is true.

        Note that when you are viewing an upcoming invoice, you are simply viewing a preview – the invoice has not yet been created. As such, the upcoming invoice will not show up in invoice listing calls, and you cannot use the API to pay or edit the invoice. If you want to change the amount that your customer will be billed, you can add, remove, or update pending invoice items, or update the customer's discount.

        Note: Currency conversion calculations use the latest exchange rates. Exchange rates may vary between the time of the preview and the time of the actual invoice creation. [Learn more](https://docs.stripe.com/currencies/conversions)
        """
        return cast(
            "Invoice",
            await cls._static_request_async(
                "post",
                "/v1/invoices/create_preview",
                params=params,
            ),
        )

    @classmethod
    def _cls_delete(
        cls, sid: str, **params: Unpack["InvoiceDeleteParams"]
    ) -> "Invoice":
        """
        Permanently deletes a one-off invoice draft. This cannot be undone. Attempts to delete invoices that are no longer in a draft state will fail; once an invoice has been finalized or if an invoice is for a subscription, it must be [voided](https://docs.stripe.com/api#void_invoice).
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(sid))
        return cast(
            "Invoice",
            cls._static_request(
                "delete",
                url,
                params=params,
            ),
        )

    @overload
    @staticmethod
    def delete(sid: str, **params: Unpack["InvoiceDeleteParams"]) -> "Invoice":
        """
        Permanently deletes a one-off invoice draft. This cannot be undone. Attempts to delete invoices that are no longer in a draft state will fail; once an invoice has been finalized or if an invoice is for a subscription, it must be [voided](https://docs.stripe.com/api#void_invoice).
        """
        ...

    @overload
    def delete(self, **params: Unpack["InvoiceDeleteParams"]) -> "Invoice":
        """
        Permanently deletes a one-off invoice draft. This cannot be undone. Attempts to delete invoices that are no longer in a draft state will fail; once an invoice has been finalized or if an invoice is for a subscription, it must be [voided](https://docs.stripe.com/api#void_invoice).
        """
        ...

    @class_method_variant("_cls_delete")
    def delete(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["InvoiceDeleteParams"]
    ) -> "Invoice":
        """
        Permanently deletes a one-off invoice draft. This cannot be undone. Attempts to delete invoices that are no longer in a draft state will fail; once an invoice has been finalized or if an invoice is for a subscription, it must be [voided](https://docs.stripe.com/api#void_invoice).
        """
        return self._request_and_refresh(
            "delete",
            self.instance_url(),
            params=params,
        )

    @classmethod
    async def _cls_delete_async(
        cls, sid: str, **params: Unpack["InvoiceDeleteParams"]
    ) -> "Invoice":
        """
        Permanently deletes a one-off invoice draft. This cannot be undone. Attempts to delete invoices that are no longer in a draft state will fail; once an invoice has been finalized or if an invoice is for a subscription, it must be [voided](https://docs.stripe.com/api#void_invoice).
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(sid))
        return cast(
            "Invoice",
            await cls._static_request_async(
                "delete",
                url,
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def delete_async(
        sid: str, **params: Unpack["InvoiceDeleteParams"]
    ) -> "Invoice":
        """
        Permanently deletes a one-off invoice draft. This cannot be undone. Attempts to delete invoices that are no longer in a draft state will fail; once an invoice has been finalized or if an invoice is for a subscription, it must be [voided](https://docs.stripe.com/api#void_invoice).
        """
        ...

    @overload
    async def delete_async(
        self, **params: Unpack["InvoiceDeleteParams"]
    ) -> "Invoice":
        """
        Permanently deletes a one-off invoice draft. This cannot be undone. Attempts to delete invoices that are no longer in a draft state will fail; once an invoice has been finalized or if an invoice is for a subscription, it must be [voided](https://docs.stripe.com/api#void_invoice).
        """
        ...

    @class_method_variant("_cls_delete_async")
    async def delete_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["InvoiceDeleteParams"]
    ) -> "Invoice":
        """
        Permanently deletes a one-off invoice draft. This cannot be undone. Attempts to delete invoices that are no longer in a draft state will fail; once an invoice has been finalized or if an invoice is for a subscription, it must be [voided](https://docs.stripe.com/api#void_invoice).
        """
        return await self._request_and_refresh_async(
            "delete",
            self.instance_url(),
            params=params,
        )

    @classmethod
    def _cls_finalize_invoice(
        cls, invoice: str, **params: Unpack["InvoiceFinalizeInvoiceParams"]
    ) -> "Invoice":
        """
        Stripe automatically finalizes drafts before sending and attempting payment on invoices. However, if you'd like to finalize a draft invoice manually, you can do so using this method.
        """
        return cast(
            "Invoice",
            cls._static_request(
                "post",
                "/v1/invoices/{invoice}/finalize".format(
                    invoice=sanitize_id(invoice)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def finalize_invoice(
        invoice: str, **params: Unpack["InvoiceFinalizeInvoiceParams"]
    ) -> "Invoice":
        """
        Stripe automatically finalizes drafts before sending and attempting payment on invoices. However, if you'd like to finalize a draft invoice manually, you can do so using this method.
        """
        ...

    @overload
    def finalize_invoice(
        self, **params: Unpack["InvoiceFinalizeInvoiceParams"]
    ) -> "Invoice":
        """
        Stripe automatically finalizes drafts before sending and attempting payment on invoices. However, if you'd like to finalize a draft invoice manually, you can do so using this method.
        """
        ...

    @class_method_variant("_cls_finalize_invoice")
    def finalize_invoice(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["InvoiceFinalizeInvoiceParams"]
    ) -> "Invoice":
        """
        Stripe automatically finalizes drafts before sending and attempting payment on invoices. However, if you'd like to finalize a draft invoice manually, you can do so using this method.
        """
        return cast(
            "Invoice",
            self._request(
                "post",
                "/v1/invoices/{invoice}/finalize".format(
                    invoice=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_finalize_invoice_async(
        cls, invoice: str, **params: Unpack["InvoiceFinalizeInvoiceParams"]
    ) -> "Invoice":
        """
        Stripe automatically finalizes drafts before sending and attempting payment on invoices. However, if you'd like to finalize a draft invoice manually, you can do so using this method.
        """
        return cast(
            "Invoice",
            await cls._static_request_async(
                "post",
                "/v1/invoices/{invoice}/finalize".format(
                    invoice=sanitize_id(invoice)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def finalize_invoice_async(
        invoice: str, **params: Unpack["InvoiceFinalizeInvoiceParams"]
    ) -> "Invoice":
        """
        Stripe automatically finalizes drafts before sending and attempting payment on invoices. However, if you'd like to finalize a draft invoice manually, you can do so using this method.
        """
        ...

    @overload
    async def finalize_invoice_async(
        self, **params: Unpack["InvoiceFinalizeInvoiceParams"]
    ) -> "Invoice":
        """
        Stripe automatically finalizes drafts before sending and attempting payment on invoices. However, if you'd like to finalize a draft invoice manually, you can do so using this method.
        """
        ...

    @class_method_variant("_cls_finalize_invoice_async")
    async def finalize_invoice_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["InvoiceFinalizeInvoiceParams"]
    ) -> "Invoice":
        """
        Stripe automatically finalizes drafts before sending and attempting payment on invoices. However, if you'd like to finalize a draft invoice manually, you can do so using this method.
        """
        return cast(
            "Invoice",
            await self._request_async(
                "post",
                "/v1/invoices/{invoice}/finalize".format(
                    invoice=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def list(
        cls, **params: Unpack["InvoiceListParams"]
    ) -> ListObject["Invoice"]:
        """
        You can list all invoices, or list the invoices for a specific customer. The invoices are returned sorted by creation date, with the most recently created invoices appearing first.
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
        cls, **params: Unpack["InvoiceListParams"]
    ) -> ListObject["Invoice"]:
        """
        You can list all invoices, or list the invoices for a specific customer. The invoices are returned sorted by creation date, with the most recently created invoices appearing first.
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
    def _cls_mark_uncollectible(
        cls, invoice: str, **params: Unpack["InvoiceMarkUncollectibleParams"]
    ) -> "Invoice":
        """
        Marking an invoice as uncollectible is useful for keeping track of bad debts that can be written off for accounting purposes.
        """
        return cast(
            "Invoice",
            cls._static_request(
                "post",
                "/v1/invoices/{invoice}/mark_uncollectible".format(
                    invoice=sanitize_id(invoice)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def mark_uncollectible(
        invoice: str, **params: Unpack["InvoiceMarkUncollectibleParams"]
    ) -> "Invoice":
        """
        Marking an invoice as uncollectible is useful for keeping track of bad debts that can be written off for accounting purposes.
        """
        ...

    @overload
    def mark_uncollectible(
        self, **params: Unpack["InvoiceMarkUncollectibleParams"]
    ) -> "Invoice":
        """
        Marking an invoice as uncollectible is useful for keeping track of bad debts that can be written off for accounting purposes.
        """
        ...

    @class_method_variant("_cls_mark_uncollectible")
    def mark_uncollectible(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["InvoiceMarkUncollectibleParams"]
    ) -> "Invoice":
        """
        Marking an invoice as uncollectible is useful for keeping track of bad debts that can be written off for accounting purposes.
        """
        return cast(
            "Invoice",
            self._request(
                "post",
                "/v1/invoices/{invoice}/mark_uncollectible".format(
                    invoice=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_mark_uncollectible_async(
        cls, invoice: str, **params: Unpack["InvoiceMarkUncollectibleParams"]
    ) -> "Invoice":
        """
        Marking an invoice as uncollectible is useful for keeping track of bad debts that can be written off for accounting purposes.
        """
        return cast(
            "Invoice",
            await cls._static_request_async(
                "post",
                "/v1/invoices/{invoice}/mark_uncollectible".format(
                    invoice=sanitize_id(invoice)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def mark_uncollectible_async(
        invoice: str, **params: Unpack["InvoiceMarkUncollectibleParams"]
    ) -> "Invoice":
        """
        Marking an invoice as uncollectible is useful for keeping track of bad debts that can be written off for accounting purposes.
        """
        ...

    @overload
    async def mark_uncollectible_async(
        self, **params: Unpack["InvoiceMarkUncollectibleParams"]
    ) -> "Invoice":
        """
        Marking an invoice as uncollectible is useful for keeping track of bad debts that can be written off for accounting purposes.
        """
        ...

    @class_method_variant("_cls_mark_uncollectible_async")
    async def mark_uncollectible_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["InvoiceMarkUncollectibleParams"]
    ) -> "Invoice":
        """
        Marking an invoice as uncollectible is useful for keeping track of bad debts that can be written off for accounting purposes.
        """
        return cast(
            "Invoice",
            await self._request_async(
                "post",
                "/v1/invoices/{invoice}/mark_uncollectible".format(
                    invoice=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def modify(
        cls, id: str, **params: Unpack["InvoiceModifyParams"]
    ) -> "Invoice":
        """
        Draft invoices are fully editable. Once an invoice is [finalized](https://docs.stripe.com/docs/billing/invoices/workflow#finalized),
        monetary values, as well as collection_method, become uneditable.

        If you would like to stop the Stripe Billing engine from automatically finalizing, reattempting payments on,
        sending reminders for, or [automatically reconciling](https://docs.stripe.com/docs/billing/invoices/reconciliation) invoices, pass
        auto_advance=false.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Invoice",
            cls._static_request(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    async def modify_async(
        cls, id: str, **params: Unpack["InvoiceModifyParams"]
    ) -> "Invoice":
        """
        Draft invoices are fully editable. Once an invoice is [finalized](https://docs.stripe.com/docs/billing/invoices/workflow#finalized),
        monetary values, as well as collection_method, become uneditable.

        If you would like to stop the Stripe Billing engine from automatically finalizing, reattempting payments on,
        sending reminders for, or [automatically reconciling](https://docs.stripe.com/docs/billing/invoices/reconciliation) invoices, pass
        auto_advance=false.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Invoice",
            await cls._static_request_async(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    def _cls_pay(
        cls, invoice: str, **params: Unpack["InvoicePayParams"]
    ) -> "Invoice":
        """
        Stripe automatically creates and then attempts to collect payment on invoices for customers on subscriptions according to your [subscriptions settings](https://dashboard.stripe.com/account/billing/automatic). However, if you'd like to attempt payment on an invoice out of the normal collection schedule or for some other reason, you can do so.
        """
        return cast(
            "Invoice",
            cls._static_request(
                "post",
                "/v1/invoices/{invoice}/pay".format(
                    invoice=sanitize_id(invoice)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def pay(invoice: str, **params: Unpack["InvoicePayParams"]) -> "Invoice":
        """
        Stripe automatically creates and then attempts to collect payment on invoices for customers on subscriptions according to your [subscriptions settings](https://dashboard.stripe.com/account/billing/automatic). However, if you'd like to attempt payment on an invoice out of the normal collection schedule or for some other reason, you can do so.
        """
        ...

    @overload
    def pay(self, **params: Unpack["InvoicePayParams"]) -> "Invoice":
        """
        Stripe automatically creates and then attempts to collect payment on invoices for customers on subscriptions according to your [subscriptions settings](https://dashboard.stripe.com/account/billing/automatic). However, if you'd like to attempt payment on an invoice out of the normal collection schedule or for some other reason, you can do so.
        """
        ...

    @class_method_variant("_cls_pay")
    def pay(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["InvoicePayParams"]
    ) -> "Invoice":
        """
        Stripe automatically creates and then attempts to collect payment on invoices for customers on subscriptions according to your [subscriptions settings](https://dashboard.stripe.com/account/billing/automatic). However, if you'd like to attempt payment on an invoice out of the normal collection schedule or for some other reason, you can do so.
        """
        return cast(
            "Invoice",
            self._request(
                "post",
                "/v1/invoices/{invoice}/pay".format(
                    invoice=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_pay_async(
        cls, invoice: str, **params: Unpack["InvoicePayParams"]
    ) -> "Invoice":
        """
        Stripe automatically creates and then attempts to collect payment on invoices for customers on subscriptions according to your [subscriptions settings](https://dashboard.stripe.com/account/billing/automatic). However, if you'd like to attempt payment on an invoice out of the normal collection schedule or for some other reason, you can do so.
        """
        return cast(
            "Invoice",
            await cls._static_request_async(
                "post",
                "/v1/invoices/{invoice}/pay".format(
                    invoice=sanitize_id(invoice)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def pay_async(
        invoice: str, **params: Unpack["InvoicePayParams"]
    ) -> "Invoice":
        """
        Stripe automatically creates and then attempts to collect payment on invoices for customers on subscriptions according to your [subscriptions settings](https://dashboard.stripe.com/account/billing/automatic). However, if you'd like to attempt payment on an invoice out of the normal collection schedule or for some other reason, you can do so.
        """
        ...

    @overload
    async def pay_async(
        self, **params: Unpack["InvoicePayParams"]
    ) -> "Invoice":
        """
        Stripe automatically creates and then attempts to collect payment on invoices for customers on subscriptions according to your [subscriptions settings](https://dashboard.stripe.com/account/billing/automatic). However, if you'd like to attempt payment on an invoice out of the normal collection schedule or for some other reason, you can do so.
        """
        ...

    @class_method_variant("_cls_pay_async")
    async def pay_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["InvoicePayParams"]
    ) -> "Invoice":
        """
        Stripe automatically creates and then attempts to collect payment on invoices for customers on subscriptions according to your [subscriptions settings](https://dashboard.stripe.com/account/billing/automatic). However, if you'd like to attempt payment on an invoice out of the normal collection schedule or for some other reason, you can do so.
        """
        return cast(
            "Invoice",
            await self._request_async(
                "post",
                "/v1/invoices/{invoice}/pay".format(
                    invoice=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def _cls_remove_lines(
        cls, invoice: str, **params: Unpack["InvoiceRemoveLinesParams"]
    ) -> "Invoice":
        """
        Removes multiple line items from an invoice. This is only possible when an invoice is still a draft.
        """
        return cast(
            "Invoice",
            cls._static_request(
                "post",
                "/v1/invoices/{invoice}/remove_lines".format(
                    invoice=sanitize_id(invoice)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def remove_lines(
        invoice: str, **params: Unpack["InvoiceRemoveLinesParams"]
    ) -> "Invoice":
        """
        Removes multiple line items from an invoice. This is only possible when an invoice is still a draft.
        """
        ...

    @overload
    def remove_lines(
        self, **params: Unpack["InvoiceRemoveLinesParams"]
    ) -> "Invoice":
        """
        Removes multiple line items from an invoice. This is only possible when an invoice is still a draft.
        """
        ...

    @class_method_variant("_cls_remove_lines")
    def remove_lines(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["InvoiceRemoveLinesParams"]
    ) -> "Invoice":
        """
        Removes multiple line items from an invoice. This is only possible when an invoice is still a draft.
        """
        return cast(
            "Invoice",
            self._request(
                "post",
                "/v1/invoices/{invoice}/remove_lines".format(
                    invoice=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_remove_lines_async(
        cls, invoice: str, **params: Unpack["InvoiceRemoveLinesParams"]
    ) -> "Invoice":
        """
        Removes multiple line items from an invoice. This is only possible when an invoice is still a draft.
        """
        return cast(
            "Invoice",
            await cls._static_request_async(
                "post",
                "/v1/invoices/{invoice}/remove_lines".format(
                    invoice=sanitize_id(invoice)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def remove_lines_async(
        invoice: str, **params: Unpack["InvoiceRemoveLinesParams"]
    ) -> "Invoice":
        """
        Removes multiple line items from an invoice. This is only possible when an invoice is still a draft.
        """
        ...

    @overload
    async def remove_lines_async(
        self, **params: Unpack["InvoiceRemoveLinesParams"]
    ) -> "Invoice":
        """
        Removes multiple line items from an invoice. This is only possible when an invoice is still a draft.
        """
        ...

    @class_method_variant("_cls_remove_lines_async")
    async def remove_lines_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["InvoiceRemoveLinesParams"]
    ) -> "Invoice":
        """
        Removes multiple line items from an invoice. This is only possible when an invoice is still a draft.
        """
        return cast(
            "Invoice",
            await self._request_async(
                "post",
                "/v1/invoices/{invoice}/remove_lines".format(
                    invoice=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["InvoiceRetrieveParams"]
    ) -> "Invoice":
        """
        Retrieves the invoice with the given ID.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["InvoiceRetrieveParams"]
    ) -> "Invoice":
        """
        Retrieves the invoice with the given ID.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    @classmethod
    def _cls_send_invoice(
        cls, invoice: str, **params: Unpack["InvoiceSendInvoiceParams"]
    ) -> "Invoice":
        """
        Stripe will automatically send invoices to customers according to your [subscriptions settings](https://dashboard.stripe.com/account/billing/automatic). However, if you'd like to manually send an invoice to your customer out of the normal schedule, you can do so. When sending invoices that have already been paid, there will be no reference to the payment in the email.

        Requests made in test-mode result in no emails being sent, despite sending an invoice.sent event.
        """
        return cast(
            "Invoice",
            cls._static_request(
                "post",
                "/v1/invoices/{invoice}/send".format(
                    invoice=sanitize_id(invoice)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def send_invoice(
        invoice: str, **params: Unpack["InvoiceSendInvoiceParams"]
    ) -> "Invoice":
        """
        Stripe will automatically send invoices to customers according to your [subscriptions settings](https://dashboard.stripe.com/account/billing/automatic). However, if you'd like to manually send an invoice to your customer out of the normal schedule, you can do so. When sending invoices that have already been paid, there will be no reference to the payment in the email.

        Requests made in test-mode result in no emails being sent, despite sending an invoice.sent event.
        """
        ...

    @overload
    def send_invoice(
        self, **params: Unpack["InvoiceSendInvoiceParams"]
    ) -> "Invoice":
        """
        Stripe will automatically send invoices to customers according to your [subscriptions settings](https://dashboard.stripe.com/account/billing/automatic). However, if you'd like to manually send an invoice to your customer out of the normal schedule, you can do so. When sending invoices that have already been paid, there will be no reference to the payment in the email.

        Requests made in test-mode result in no emails being sent, despite sending an invoice.sent event.
        """
        ...

    @class_method_variant("_cls_send_invoice")
    def send_invoice(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["InvoiceSendInvoiceParams"]
    ) -> "Invoice":
        """
        Stripe will automatically send invoices to customers according to your [subscriptions settings](https://dashboard.stripe.com/account/billing/automatic). However, if you'd like to manually send an invoice to your customer out of the normal schedule, you can do so. When sending invoices that have already been paid, there will be no reference to the payment in the email.

        Requests made in test-mode result in no emails being sent, despite sending an invoice.sent event.
        """
        return cast(
            "Invoice",
            self._request(
                "post",
                "/v1/invoices/{invoice}/send".format(
                    invoice=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_send_invoice_async(
        cls, invoice: str, **params: Unpack["InvoiceSendInvoiceParams"]
    ) -> "Invoice":
        """
        Stripe will automatically send invoices to customers according to your [subscriptions settings](https://dashboard.stripe.com/account/billing/automatic). However, if you'd like to manually send an invoice to your customer out of the normal schedule, you can do so. When sending invoices that have already been paid, there will be no reference to the payment in the email.

        Requests made in test-mode result in no emails being sent, despite sending an invoice.sent event.
        """
        return cast(
            "Invoice",
            await cls._static_request_async(
                "post",
                "/v1/invoices/{invoice}/send".format(
                    invoice=sanitize_id(invoice)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def send_invoice_async(
        invoice: str, **params: Unpack["InvoiceSendInvoiceParams"]
    ) -> "Invoice":
        """
        Stripe will automatically send invoices to customers according to your [subscriptions settings](https://dashboard.stripe.com/account/billing/automatic). However, if you'd like to manually send an invoice to your customer out of the normal schedule, you can do so. When sending invoices that have already been paid, there will be no reference to the payment in the email.

        Requests made in test-mode result in no emails being sent, despite sending an invoice.sent event.
        """
        ...

    @overload
    async def send_invoice_async(
        self, **params: Unpack["InvoiceSendInvoiceParams"]
    ) -> "Invoice":
        """
        Stripe will automatically send invoices to customers according to your [subscriptions settings](https://dashboard.stripe.com/account/billing/automatic). However, if you'd like to manually send an invoice to your customer out of the normal schedule, you can do so. When sending invoices that have already been paid, there will be no reference to the payment in the email.

        Requests made in test-mode result in no emails being sent, despite sending an invoice.sent event.
        """
        ...

    @class_method_variant("_cls_send_invoice_async")
    async def send_invoice_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["InvoiceSendInvoiceParams"]
    ) -> "Invoice":
        """
        Stripe will automatically send invoices to customers according to your [subscriptions settings](https://dashboard.stripe.com/account/billing/automatic). However, if you'd like to manually send an invoice to your customer out of the normal schedule, you can do so. When sending invoices that have already been paid, there will be no reference to the payment in the email.

        Requests made in test-mode result in no emails being sent, despite sending an invoice.sent event.
        """
        return cast(
            "Invoice",
            await self._request_async(
                "post",
                "/v1/invoices/{invoice}/send".format(
                    invoice=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def _cls_update_lines(
        cls, invoice: str, **params: Unpack["InvoiceUpdateLinesParams"]
    ) -> "Invoice":
        """
        Updates multiple line items on an invoice. This is only possible when an invoice is still a draft.
        """
        return cast(
            "Invoice",
            cls._static_request(
                "post",
                "/v1/invoices/{invoice}/update_lines".format(
                    invoice=sanitize_id(invoice)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def update_lines(
        invoice: str, **params: Unpack["InvoiceUpdateLinesParams"]
    ) -> "Invoice":
        """
        Updates multiple line items on an invoice. This is only possible when an invoice is still a draft.
        """
        ...

    @overload
    def update_lines(
        self, **params: Unpack["InvoiceUpdateLinesParams"]
    ) -> "Invoice":
        """
        Updates multiple line items on an invoice. This is only possible when an invoice is still a draft.
        """
        ...

    @class_method_variant("_cls_update_lines")
    def update_lines(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["InvoiceUpdateLinesParams"]
    ) -> "Invoice":
        """
        Updates multiple line items on an invoice. This is only possible when an invoice is still a draft.
        """
        return cast(
            "Invoice",
            self._request(
                "post",
                "/v1/invoices/{invoice}/update_lines".format(
                    invoice=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_update_lines_async(
        cls, invoice: str, **params: Unpack["InvoiceUpdateLinesParams"]
    ) -> "Invoice":
        """
        Updates multiple line items on an invoice. This is only possible when an invoice is still a draft.
        """
        return cast(
            "Invoice",
            await cls._static_request_async(
                "post",
                "/v1/invoices/{invoice}/update_lines".format(
                    invoice=sanitize_id(invoice)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def update_lines_async(
        invoice: str, **params: Unpack["InvoiceUpdateLinesParams"]
    ) -> "Invoice":
        """
        Updates multiple line items on an invoice. This is only possible when an invoice is still a draft.
        """
        ...

    @overload
    async def update_lines_async(
        self, **params: Unpack["InvoiceUpdateLinesParams"]
    ) -> "Invoice":
        """
        Updates multiple line items on an invoice. This is only possible when an invoice is still a draft.
        """
        ...

    @class_method_variant("_cls_update_lines_async")
    async def update_lines_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["InvoiceUpdateLinesParams"]
    ) -> "Invoice":
        """
        Updates multiple line items on an invoice. This is only possible when an invoice is still a draft.
        """
        return cast(
            "Invoice",
            await self._request_async(
                "post",
                "/v1/invoices/{invoice}/update_lines".format(
                    invoice=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def _cls_void_invoice(
        cls, invoice: str, **params: Unpack["InvoiceVoidInvoiceParams"]
    ) -> "Invoice":
        """
        Mark a finalized invoice as void. This cannot be undone. Voiding an invoice is similar to [deletion](https://docs.stripe.com/api#delete_invoice), however it only applies to finalized invoices and maintains a papertrail where the invoice can still be found.

        Consult with local regulations to determine whether and how an invoice might be amended, canceled, or voided in the jurisdiction you're doing business in. You might need to [issue another invoice or <a href="#create_credit_note">credit note](https://docs.stripe.com/api#create_invoice) instead. Stripe recommends that you consult with your legal counsel for advice specific to your business.
        """
        return cast(
            "Invoice",
            cls._static_request(
                "post",
                "/v1/invoices/{invoice}/void".format(
                    invoice=sanitize_id(invoice)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def void_invoice(
        invoice: str, **params: Unpack["InvoiceVoidInvoiceParams"]
    ) -> "Invoice":
        """
        Mark a finalized invoice as void. This cannot be undone. Voiding an invoice is similar to [deletion](https://docs.stripe.com/api#delete_invoice), however it only applies to finalized invoices and maintains a papertrail where the invoice can still be found.

        Consult with local regulations to determine whether and how an invoice might be amended, canceled, or voided in the jurisdiction you're doing business in. You might need to [issue another invoice or <a href="#create_credit_note">credit note](https://docs.stripe.com/api#create_invoice) instead. Stripe recommends that you consult with your legal counsel for advice specific to your business.
        """
        ...

    @overload
    def void_invoice(
        self, **params: Unpack["InvoiceVoidInvoiceParams"]
    ) -> "Invoice":
        """
        Mark a finalized invoice as void. This cannot be undone. Voiding an invoice is similar to [deletion](https://docs.stripe.com/api#delete_invoice), however it only applies to finalized invoices and maintains a papertrail where the invoice can still be found.

        Consult with local regulations to determine whether and how an invoice might be amended, canceled, or voided in the jurisdiction you're doing business in. You might need to [issue another invoice or <a href="#create_credit_note">credit note](https://docs.stripe.com/api#create_invoice) instead. Stripe recommends that you consult with your legal counsel for advice specific to your business.
        """
        ...

    @class_method_variant("_cls_void_invoice")
    def void_invoice(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["InvoiceVoidInvoiceParams"]
    ) -> "Invoice":
        """
        Mark a finalized invoice as void. This cannot be undone. Voiding an invoice is similar to [deletion](https://docs.stripe.com/api#delete_invoice), however it only applies to finalized invoices and maintains a papertrail where the invoice can still be found.

        Consult with local regulations to determine whether and how an invoice might be amended, canceled, or voided in the jurisdiction you're doing business in. You might need to [issue another invoice or <a href="#create_credit_note">credit note](https://docs.stripe.com/api#create_invoice) instead. Stripe recommends that you consult with your legal counsel for advice specific to your business.
        """
        return cast(
            "Invoice",
            self._request(
                "post",
                "/v1/invoices/{invoice}/void".format(
                    invoice=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_void_invoice_async(
        cls, invoice: str, **params: Unpack["InvoiceVoidInvoiceParams"]
    ) -> "Invoice":
        """
        Mark a finalized invoice as void. This cannot be undone. Voiding an invoice is similar to [deletion](https://docs.stripe.com/api#delete_invoice), however it only applies to finalized invoices and maintains a papertrail where the invoice can still be found.

        Consult with local regulations to determine whether and how an invoice might be amended, canceled, or voided in the jurisdiction you're doing business in. You might need to [issue another invoice or <a href="#create_credit_note">credit note](https://docs.stripe.com/api#create_invoice) instead. Stripe recommends that you consult with your legal counsel for advice specific to your business.
        """
        return cast(
            "Invoice",
            await cls._static_request_async(
                "post",
                "/v1/invoices/{invoice}/void".format(
                    invoice=sanitize_id(invoice)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def void_invoice_async(
        invoice: str, **params: Unpack["InvoiceVoidInvoiceParams"]
    ) -> "Invoice":
        """
        Mark a finalized invoice as void. This cannot be undone. Voiding an invoice is similar to [deletion](https://docs.stripe.com/api#delete_invoice), however it only applies to finalized invoices and maintains a papertrail where the invoice can still be found.

        Consult with local regulations to determine whether and how an invoice might be amended, canceled, or voided in the jurisdiction you're doing business in. You might need to [issue another invoice or <a href="#create_credit_note">credit note](https://docs.stripe.com/api#create_invoice) instead. Stripe recommends that you consult with your legal counsel for advice specific to your business.
        """
        ...

    @overload
    async def void_invoice_async(
        self, **params: Unpack["InvoiceVoidInvoiceParams"]
    ) -> "Invoice":
        """
        Mark a finalized invoice as void. This cannot be undone. Voiding an invoice is similar to [deletion](https://docs.stripe.com/api#delete_invoice), however it only applies to finalized invoices and maintains a papertrail where the invoice can still be found.

        Consult with local regulations to determine whether and how an invoice might be amended, canceled, or voided in the jurisdiction you're doing business in. You might need to [issue another invoice or <a href="#create_credit_note">credit note](https://docs.stripe.com/api#create_invoice) instead. Stripe recommends that you consult with your legal counsel for advice specific to your business.
        """
        ...

    @class_method_variant("_cls_void_invoice_async")
    async def void_invoice_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["InvoiceVoidInvoiceParams"]
    ) -> "Invoice":
        """
        Mark a finalized invoice as void. This cannot be undone. Voiding an invoice is similar to [deletion](https://docs.stripe.com/api#delete_invoice), however it only applies to finalized invoices and maintains a papertrail where the invoice can still be found.

        Consult with local regulations to determine whether and how an invoice might be amended, canceled, or voided in the jurisdiction you're doing business in. You might need to [issue another invoice or <a href="#create_credit_note">credit note](https://docs.stripe.com/api#create_invoice) instead. Stripe recommends that you consult with your legal counsel for advice specific to your business.
        """
        return cast(
            "Invoice",
            await self._request_async(
                "post",
                "/v1/invoices/{invoice}/void".format(
                    invoice=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def search(
        cls, *args, **kwargs: Unpack["InvoiceSearchParams"]
    ) -> SearchResultObject["Invoice"]:
        """
        Search for invoices you've previously created using Stripe's [Search Query Language](https://docs.stripe.com/docs/search#search-query-language).
        Don't use search in read-after-write flows where strict consistency is necessary. Under normal operating
        conditions, data is searchable in less than a minute. Occasionally, propagation of new or updated data can be up
        to an hour behind during outages. Search functionality is not available to merchants in India.
        """
        return cls._search(search_url="/v1/invoices/search", *args, **kwargs)

    @classmethod
    async def search_async(
        cls, *args, **kwargs: Unpack["InvoiceSearchParams"]
    ) -> SearchResultObject["Invoice"]:
        """
        Search for invoices you've previously created using Stripe's [Search Query Language](https://docs.stripe.com/docs/search#search-query-language).
        Don't use search in read-after-write flows where strict consistency is necessary. Under normal operating
        conditions, data is searchable in less than a minute. Occasionally, propagation of new or updated data can be up
        to an hour behind during outages. Search functionality is not available to merchants in India.
        """
        return await cls._search_async(
            search_url="/v1/invoices/search", *args, **kwargs
        )

    @classmethod
    def search_auto_paging_iter(
        cls, *args, **kwargs: Unpack["InvoiceSearchParams"]
    ) -> Iterator["Invoice"]:
        return cls.search(*args, **kwargs).auto_paging_iter()

    @classmethod
    async def search_auto_paging_iter_async(
        cls, *args, **kwargs: Unpack["InvoiceSearchParams"]
    ) -> AsyncIterator["Invoice"]:
        return (await cls.search_async(*args, **kwargs)).auto_paging_iter()

    @classmethod
    def list_lines(
        cls, invoice: str, **params: Unpack["InvoiceListLinesParams"]
    ) -> ListObject["InvoiceLineItem"]:
        """
        When retrieving an invoice, you'll get a lines property containing the total count of line items and the first handful of those items. There is also a URL where you can retrieve the full (paginated) list of line items.
        """
        return cast(
            ListObject["InvoiceLineItem"],
            cls._static_request(
                "get",
                "/v1/invoices/{invoice}/lines".format(
                    invoice=sanitize_id(invoice)
                ),
                params=params,
            ),
        )

    @classmethod
    async def list_lines_async(
        cls, invoice: str, **params: Unpack["InvoiceListLinesParams"]
    ) -> ListObject["InvoiceLineItem"]:
        """
        When retrieving an invoice, you'll get a lines property containing the total count of line items and the first handful of those items. There is also a URL where you can retrieve the full (paginated) list of line items.
        """
        return cast(
            ListObject["InvoiceLineItem"],
            await cls._static_request_async(
                "get",
                "/v1/invoices/{invoice}/lines".format(
                    invoice=sanitize_id(invoice)
                ),
                params=params,
            ),
        )

    _inner_class_types = {
        "automatic_tax": AutomaticTax,
        "confirmation_secret": ConfirmationSecret,
        "custom_fields": CustomField,
        "customer_address": CustomerAddress,
        "customer_shipping": CustomerShipping,
        "customer_tax_ids": CustomerTaxId,
        "from_invoice": FromInvoice,
        "issuer": Issuer,
        "last_finalization_error": LastFinalizationError,
        "parent": Parent,
        "payment_settings": PaymentSettings,
        "rendering": Rendering,
        "shipping_cost": ShippingCost,
        "shipping_details": ShippingDetails,
        "status_transitions": StatusTransitions,
        "threshold_reason": ThresholdReason,
        "total_discount_amounts": TotalDiscountAmount,
        "total_pretax_credit_amounts": TotalPretaxCreditAmount,
        "total_taxes": TotalTax,
    }
