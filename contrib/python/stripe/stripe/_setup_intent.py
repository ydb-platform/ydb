# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._expandable_field import ExpandableField
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._stripe_object import StripeObject
from stripe._updateable_api_resource import UpdateableAPIResource
from stripe._util import class_method_variant, sanitize_id
from typing import Any, ClassVar, Dict, List, Optional, Union, cast, overload
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._account import Account
    from stripe._application import Application
    from stripe._bank_account import BankAccount
    from stripe._card import Card as CardResource
    from stripe._customer import Customer
    from stripe._mandate import Mandate
    from stripe._payment_intent import PaymentIntent
    from stripe._payment_method import PaymentMethod
    from stripe._setup_attempt import SetupAttempt
    from stripe._source import Source
    from stripe.params._setup_intent_cancel_params import (
        SetupIntentCancelParams,
    )
    from stripe.params._setup_intent_confirm_params import (
        SetupIntentConfirmParams,
    )
    from stripe.params._setup_intent_create_params import (
        SetupIntentCreateParams,
    )
    from stripe.params._setup_intent_list_params import SetupIntentListParams
    from stripe.params._setup_intent_modify_params import (
        SetupIntentModifyParams,
    )
    from stripe.params._setup_intent_retrieve_params import (
        SetupIntentRetrieveParams,
    )
    from stripe.params._setup_intent_verify_microdeposits_params import (
        SetupIntentVerifyMicrodepositsParams,
    )


class SetupIntent(
    CreateableAPIResource["SetupIntent"],
    ListableAPIResource["SetupIntent"],
    UpdateableAPIResource["SetupIntent"],
):
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

    OBJECT_NAME: ClassVar[Literal["setup_intent"]] = "setup_intent"

    class AutomaticPaymentMethods(StripeObject):
        allow_redirects: Optional[Literal["always", "never"]]
        """
        Controls whether this SetupIntent will accept redirect-based payment methods.

        Redirect-based payment methods may require your customer to be redirected to a payment method's app or site for authentication or additional steps. To [confirm](https://docs.stripe.com/api/setup_intents/confirm) this SetupIntent, you may be required to provide a `return_url` to redirect customers back to your site after they authenticate or complete the setup.
        """
        enabled: Optional[bool]
        """
        Automatically calculates compatible payment methods
        """

    class LastSetupError(StripeObject):
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

    class NextAction(StripeObject):
        class CashappHandleRedirectOrDisplayQrCode(StripeObject):
            class QrCode(StripeObject):
                expires_at: int
                """
                The date (unix timestamp) when the QR code expires.
                """
                image_url_png: str
                """
                The image_url_png string used to render QR code
                """
                image_url_svg: str
                """
                The image_url_svg string used to render QR code
                """

            hosted_instructions_url: str
            """
            The URL to the hosted Cash App Pay instructions page, which allows customers to view the QR code, and supports QR code refreshing on expiration.
            """
            mobile_auth_url: str
            """
            The url for mobile redirect based auth
            """
            qr_code: QrCode
            _inner_class_types = {"qr_code": QrCode}

        class RedirectToUrl(StripeObject):
            return_url: Optional[str]
            """
            If the customer does not exit their browser while authenticating, they will be redirected to this specified URL after completion.
            """
            url: Optional[str]
            """
            The URL you must redirect your customer to in order to authenticate.
            """

        class VerifyWithMicrodeposits(StripeObject):
            arrival_date: int
            """
            The timestamp when the microdeposits are expected to land.
            """
            hosted_verification_url: str
            """
            The URL for the hosted verification page, which allows customers to verify their bank account.
            """
            microdeposit_type: Optional[Literal["amounts", "descriptor_code"]]
            """
            The type of the microdeposit sent to the customer. Used to distinguish between different verification methods.
            """

        cashapp_handle_redirect_or_display_qr_code: Optional[
            CashappHandleRedirectOrDisplayQrCode
        ]
        redirect_to_url: Optional[RedirectToUrl]
        type: str
        """
        Type of the next action to perform. Refer to the other child attributes under `next_action` for available values. Examples include: `redirect_to_url`, `use_stripe_sdk`, `alipay_handle_redirect`, `oxxo_display_details`, or `verify_with_microdeposits`.
        """
        use_stripe_sdk: Optional[Dict[str, Any]]
        """
        When confirming a SetupIntent with Stripe.js, Stripe.js depends on the contents of this dictionary to invoke authentication flows. The shape of the contents is subject to change and is only intended to be used by Stripe.js.
        """
        verify_with_microdeposits: Optional[VerifyWithMicrodeposits]
        _inner_class_types = {
            "cashapp_handle_redirect_or_display_qr_code": CashappHandleRedirectOrDisplayQrCode,
            "redirect_to_url": RedirectToUrl,
            "verify_with_microdeposits": VerifyWithMicrodeposits,
        }

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
                List of Stripe products where this mandate can be selected automatically.
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
            Currency supported by the bank account
            """
            mandate_options: Optional[MandateOptions]
            verification_method: Optional[
                Literal["automatic", "instant", "microdeposits"]
            ]
            """
            Bank account verification method.
            """
            _inner_class_types = {"mandate_options": MandateOptions}

        class AmazonPay(StripeObject):
            pass

        class BacsDebit(StripeObject):
            class MandateOptions(StripeObject):
                reference_prefix: Optional[str]
                """
                Prefix used to generate the Mandate reference. Must be at most 12 characters long. Must consist of only uppercase letters, numbers, spaces, or the following special characters: '/', '_', '-', '&', '.'. Cannot begin with 'DDIC' or 'STRIPE'.
                """

            mandate_options: Optional[MandateOptions]
            _inner_class_types = {"mandate_options": MandateOptions}

        class Card(StripeObject):
            class MandateOptions(StripeObject):
                amount: int
                """
                Amount to be charged for future payments.
                """
                amount_type: Literal["fixed", "maximum"]
                """
                One of `fixed` or `maximum`. If `fixed`, the `amount` param refers to the exact amount to be charged in future payments. If `maximum`, the amount charged can be up to the value passed for the `amount` param.
                """
                currency: str
                """
                Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
                """
                description: Optional[str]
                """
                A description of the mandate or subscription that is meant to be displayed to the customer.
                """
                end_date: Optional[int]
                """
                End date of the mandate or subscription. If not provided, the mandate will be active until canceled. If provided, end date should be after start date.
                """
                interval: Literal["day", "month", "sporadic", "week", "year"]
                """
                Specifies payment frequency. One of `day`, `week`, `month`, `year`, or `sporadic`.
                """
                interval_count: Optional[int]
                """
                The number of intervals between payments. For example, `interval=month` and `interval_count=3` indicates one payment every three months. Maximum of one year interval allowed (1 year, 12 months, or 52 weeks). This parameter is optional when `interval=sporadic`.
                """
                reference: str
                """
                Unique identifier for the mandate or subscription.
                """
                start_date: int
                """
                Start date of the mandate or subscription. Start date should not be lesser than yesterday.
                """
                supported_types: Optional[List[Literal["india"]]]
                """
                Specifies the type of mandates supported. Possible values are `india`.
                """

            mandate_options: Optional[MandateOptions]
            """
            Configuration options for setting up an eMandate for cards issued in India.
            """
            network: Optional[
                Literal[
                    "amex",
                    "cartes_bancaires",
                    "diners",
                    "discover",
                    "eftpos_au",
                    "girocard",
                    "interac",
                    "jcb",
                    "link",
                    "mastercard",
                    "unionpay",
                    "unknown",
                    "visa",
                ]
            ]
            """
            Selected network to process this SetupIntent on. Depends on the available networks of the card attached to the setup intent. Can be only set confirm-time.
            """
            request_three_d_secure: Optional[
                Literal["any", "automatic", "challenge"]
            ]
            """
            We strongly recommend that you rely on our SCA Engine to automatically prompt your customers for authentication based on risk level and [other requirements](https://docs.stripe.com/strong-customer-authentication). However, if you wish to request 3D Secure based on logic from your own fraud engine, provide this option. If not provided, this value defaults to `automatic`. Read our guide on [manually requesting 3D Secure](https://docs.stripe.com/payments/3d-secure/authentication-flow#manual-three-ds) for more information on how this configuration interacts with Radar and our SCA Engine.
            """
            _inner_class_types = {"mandate_options": MandateOptions}

        class CardPresent(StripeObject):
            pass

        class Klarna(StripeObject):
            currency: Optional[str]
            """
            The currency of the setup intent. Three letter ISO currency code.
            """
            preferred_locale: Optional[str]
            """
            Preferred locale of the Klarna checkout page that the customer is redirected to.
            """

        class Link(StripeObject):
            persistent_token: Optional[str]
            """
            [Deprecated] This is a legacy parameter that no longer has any function.
            """

        class Paypal(StripeObject):
            billing_agreement_id: Optional[str]
            """
            The PayPal Billing Agreement ID (BAID). This is an ID generated by PayPal which represents the mandate between the merchant and the customer.
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
            _inner_class_types = {"mandate_options": MandateOptions}

        class SepaDebit(StripeObject):
            class MandateOptions(StripeObject):
                reference_prefix: Optional[str]
                """
                Prefix used to generate the Mandate reference. Must be at most 12 characters long. Must consist of only uppercase letters, numbers, spaces, or the following special characters: '/', '_', '-', '&', '.'. Cannot begin with 'STRIPE'.
                """

            mandate_options: Optional[MandateOptions]
            _inner_class_types = {"mandate_options": MandateOptions}

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

            class MandateOptions(StripeObject):
                collection_method: Optional[Literal["paper"]]
                """
                Mandate collection method
                """

            financial_connections: Optional[FinancialConnections]
            mandate_options: Optional[MandateOptions]
            verification_method: Optional[
                Literal["automatic", "instant", "microdeposits"]
            ]
            """
            Bank account verification method.
            """
            _inner_class_types = {
                "financial_connections": FinancialConnections,
                "mandate_options": MandateOptions,
            }

        acss_debit: Optional[AcssDebit]
        amazon_pay: Optional[AmazonPay]
        bacs_debit: Optional[BacsDebit]
        card: Optional[Card]
        card_present: Optional[CardPresent]
        klarna: Optional[Klarna]
        link: Optional[Link]
        paypal: Optional[Paypal]
        payto: Optional[Payto]
        sepa_debit: Optional[SepaDebit]
        us_bank_account: Optional[UsBankAccount]
        _inner_class_types = {
            "acss_debit": AcssDebit,
            "amazon_pay": AmazonPay,
            "bacs_debit": BacsDebit,
            "card": Card,
            "card_present": CardPresent,
            "klarna": Klarna,
            "link": Link,
            "paypal": Paypal,
            "payto": Payto,
            "sepa_debit": SepaDebit,
            "us_bank_account": UsBankAccount,
        }

    application: Optional[ExpandableField["Application"]]
    """
    ID of the Connect application that created the SetupIntent.
    """
    attach_to_self: Optional[bool]
    """
    If present, the SetupIntent's payment method will be attached to the in-context Stripe Account.

    It can only be used for this Stripe Account's own money movement flows like InboundTransfer and OutboundTransfers. It cannot be set to true when setting up a PaymentMethod for a Customer, and defaults to false when attaching a PaymentMethod to a Customer.
    """
    automatic_payment_methods: Optional[AutomaticPaymentMethods]
    """
    Settings for dynamic payment methods compatible with this Setup Intent
    """
    cancellation_reason: Optional[
        Literal["abandoned", "duplicate", "requested_by_customer"]
    ]
    """
    Reason for cancellation of this SetupIntent, one of `abandoned`, `requested_by_customer`, or `duplicate`.
    """
    client_secret: Optional[str]
    """
    The client secret of this SetupIntent. Used for client-side retrieval using a publishable key.

    The client secret can be used to complete payment setup from your frontend. It should not be stored, logged, or exposed to anyone other than the customer. Make sure that you have TLS enabled on any page that includes the client secret.
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    customer: Optional[ExpandableField["Customer"]]
    """
    ID of the Customer this SetupIntent belongs to, if one exists.

    If present, the SetupIntent's payment method will be attached to the Customer on successful setup. Payment methods attached to other Customers cannot be used with this SetupIntent.
    """
    customer_account: Optional[str]
    """
    ID of the Account this SetupIntent belongs to, if one exists.

    If present, the SetupIntent's payment method will be attached to the Account on successful setup. Payment methods attached to other Accounts cannot be used with this SetupIntent.
    """
    description: Optional[str]
    """
    An arbitrary string attached to the object. Often useful for displaying to users.
    """
    excluded_payment_method_types: Optional[
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
    Payment method types that are excluded from this SetupIntent.
    """
    flow_directions: Optional[List[Literal["inbound", "outbound"]]]
    """
    Indicates the directions of money movement for which this payment method is intended to be used.

    Include `inbound` if you intend to use the payment method as the origin to pull funds from. Include `outbound` if you intend to use the payment method as the destination to send funds to. You can include both if you intend to use the payment method for both purposes.
    """
    id: str
    """
    Unique identifier for the object.
    """
    last_setup_error: Optional[LastSetupError]
    """
    The error encountered in the previous SetupIntent confirmation.
    """
    latest_attempt: Optional[ExpandableField["SetupAttempt"]]
    """
    The most recent SetupAttempt for this SetupIntent.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    mandate: Optional[ExpandableField["Mandate"]]
    """
    ID of the multi use Mandate generated by the SetupIntent.
    """
    metadata: Optional[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    next_action: Optional[NextAction]
    """
    If present, this property tells you what actions you need to take in order for your customer to continue payment setup.
    """
    object: Literal["setup_intent"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    on_behalf_of: Optional[ExpandableField["Account"]]
    """
    The account (if any) for which the setup is intended.
    """
    payment_method: Optional[ExpandableField["PaymentMethod"]]
    """
    ID of the payment method used with this SetupIntent. If the payment method is `card_present` and isn't a digital wallet, then the [generated_card](https://docs.stripe.com/api/setup_attempts/object#setup_attempt_object-payment_method_details-card_present-generated_card) associated with the `latest_attempt` is attached to the Customer instead.
    """
    payment_method_configuration_details: Optional[
        PaymentMethodConfigurationDetails
    ]
    """
    Information about the [payment method configuration](https://docs.stripe.com/api/payment_method_configurations) used for this Setup Intent.
    """
    payment_method_options: Optional[PaymentMethodOptions]
    """
    Payment method-specific configuration for this SetupIntent.
    """
    payment_method_types: List[str]
    """
    The list of payment method types (e.g. card) that this SetupIntent is allowed to set up. A list of valid payment method types can be found [here](https://docs.stripe.com/api/payment_methods/object#payment_method_object-type).
    """
    single_use_mandate: Optional[ExpandableField["Mandate"]]
    """
    ID of the single_use Mandate generated by the SetupIntent.
    """
    status: Literal[
        "canceled",
        "processing",
        "requires_action",
        "requires_confirmation",
        "requires_payment_method",
        "succeeded",
    ]
    """
    [Status](https://docs.stripe.com/payments/intents#intent-statuses) of this SetupIntent, one of `requires_payment_method`, `requires_confirmation`, `requires_action`, `processing`, `canceled`, or `succeeded`.
    """
    usage: str
    """
    Indicates how the payment method is intended to be used in the future.

    Use `on_session` if you intend to only reuse the payment method when the customer is in your checkout flow. Use `off_session` if your customer may or may not be in your checkout flow. If not provided, this value defaults to `off_session`.
    """

    @classmethod
    def _cls_cancel(
        cls, intent: str, **params: Unpack["SetupIntentCancelParams"]
    ) -> "SetupIntent":
        """
        You can cancel a SetupIntent object when it's in one of these statuses: requires_payment_method, requires_confirmation, or requires_action.

        After you cancel it, setup is abandoned and any operations on the SetupIntent fail with an error. You can't cancel the SetupIntent for a Checkout Session. [Expire the Checkout Session](https://docs.stripe.com/docs/api/checkout/sessions/expire) instead.
        """
        return cast(
            "SetupIntent",
            cls._static_request(
                "post",
                "/v1/setup_intents/{intent}/cancel".format(
                    intent=sanitize_id(intent)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def cancel(
        intent: str, **params: Unpack["SetupIntentCancelParams"]
    ) -> "SetupIntent":
        """
        You can cancel a SetupIntent object when it's in one of these statuses: requires_payment_method, requires_confirmation, or requires_action.

        After you cancel it, setup is abandoned and any operations on the SetupIntent fail with an error. You can't cancel the SetupIntent for a Checkout Session. [Expire the Checkout Session](https://docs.stripe.com/docs/api/checkout/sessions/expire) instead.
        """
        ...

    @overload
    def cancel(
        self, **params: Unpack["SetupIntentCancelParams"]
    ) -> "SetupIntent":
        """
        You can cancel a SetupIntent object when it's in one of these statuses: requires_payment_method, requires_confirmation, or requires_action.

        After you cancel it, setup is abandoned and any operations on the SetupIntent fail with an error. You can't cancel the SetupIntent for a Checkout Session. [Expire the Checkout Session](https://docs.stripe.com/docs/api/checkout/sessions/expire) instead.
        """
        ...

    @class_method_variant("_cls_cancel")
    def cancel(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["SetupIntentCancelParams"]
    ) -> "SetupIntent":
        """
        You can cancel a SetupIntent object when it's in one of these statuses: requires_payment_method, requires_confirmation, or requires_action.

        After you cancel it, setup is abandoned and any operations on the SetupIntent fail with an error. You can't cancel the SetupIntent for a Checkout Session. [Expire the Checkout Session](https://docs.stripe.com/docs/api/checkout/sessions/expire) instead.
        """
        return cast(
            "SetupIntent",
            self._request(
                "post",
                "/v1/setup_intents/{intent}/cancel".format(
                    intent=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_cancel_async(
        cls, intent: str, **params: Unpack["SetupIntentCancelParams"]
    ) -> "SetupIntent":
        """
        You can cancel a SetupIntent object when it's in one of these statuses: requires_payment_method, requires_confirmation, or requires_action.

        After you cancel it, setup is abandoned and any operations on the SetupIntent fail with an error. You can't cancel the SetupIntent for a Checkout Session. [Expire the Checkout Session](https://docs.stripe.com/docs/api/checkout/sessions/expire) instead.
        """
        return cast(
            "SetupIntent",
            await cls._static_request_async(
                "post",
                "/v1/setup_intents/{intent}/cancel".format(
                    intent=sanitize_id(intent)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def cancel_async(
        intent: str, **params: Unpack["SetupIntentCancelParams"]
    ) -> "SetupIntent":
        """
        You can cancel a SetupIntent object when it's in one of these statuses: requires_payment_method, requires_confirmation, or requires_action.

        After you cancel it, setup is abandoned and any operations on the SetupIntent fail with an error. You can't cancel the SetupIntent for a Checkout Session. [Expire the Checkout Session](https://docs.stripe.com/docs/api/checkout/sessions/expire) instead.
        """
        ...

    @overload
    async def cancel_async(
        self, **params: Unpack["SetupIntentCancelParams"]
    ) -> "SetupIntent":
        """
        You can cancel a SetupIntent object when it's in one of these statuses: requires_payment_method, requires_confirmation, or requires_action.

        After you cancel it, setup is abandoned and any operations on the SetupIntent fail with an error. You can't cancel the SetupIntent for a Checkout Session. [Expire the Checkout Session](https://docs.stripe.com/docs/api/checkout/sessions/expire) instead.
        """
        ...

    @class_method_variant("_cls_cancel_async")
    async def cancel_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["SetupIntentCancelParams"]
    ) -> "SetupIntent":
        """
        You can cancel a SetupIntent object when it's in one of these statuses: requires_payment_method, requires_confirmation, or requires_action.

        After you cancel it, setup is abandoned and any operations on the SetupIntent fail with an error. You can't cancel the SetupIntent for a Checkout Session. [Expire the Checkout Session](https://docs.stripe.com/docs/api/checkout/sessions/expire) instead.
        """
        return cast(
            "SetupIntent",
            await self._request_async(
                "post",
                "/v1/setup_intents/{intent}/cancel".format(
                    intent=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def _cls_confirm(
        cls, intent: str, **params: Unpack["SetupIntentConfirmParams"]
    ) -> "SetupIntent":
        """
        Confirm that your customer intends to set up the current or
        provided payment method. For example, you would confirm a SetupIntent
        when a customer hits the “Save” button on a payment method management
        page on your website.

        If the selected payment method does not require any additional
        steps from the customer, the SetupIntent will transition to the
        succeeded status.

        Otherwise, it will transition to the requires_action status and
        suggest additional actions via next_action. If setup fails,
        the SetupIntent will transition to the
        requires_payment_method status or the canceled status if the
        confirmation limit is reached.
        """
        return cast(
            "SetupIntent",
            cls._static_request(
                "post",
                "/v1/setup_intents/{intent}/confirm".format(
                    intent=sanitize_id(intent)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def confirm(
        intent: str, **params: Unpack["SetupIntentConfirmParams"]
    ) -> "SetupIntent":
        """
        Confirm that your customer intends to set up the current or
        provided payment method. For example, you would confirm a SetupIntent
        when a customer hits the “Save” button on a payment method management
        page on your website.

        If the selected payment method does not require any additional
        steps from the customer, the SetupIntent will transition to the
        succeeded status.

        Otherwise, it will transition to the requires_action status and
        suggest additional actions via next_action. If setup fails,
        the SetupIntent will transition to the
        requires_payment_method status or the canceled status if the
        confirmation limit is reached.
        """
        ...

    @overload
    def confirm(
        self, **params: Unpack["SetupIntentConfirmParams"]
    ) -> "SetupIntent":
        """
        Confirm that your customer intends to set up the current or
        provided payment method. For example, you would confirm a SetupIntent
        when a customer hits the “Save” button on a payment method management
        page on your website.

        If the selected payment method does not require any additional
        steps from the customer, the SetupIntent will transition to the
        succeeded status.

        Otherwise, it will transition to the requires_action status and
        suggest additional actions via next_action. If setup fails,
        the SetupIntent will transition to the
        requires_payment_method status or the canceled status if the
        confirmation limit is reached.
        """
        ...

    @class_method_variant("_cls_confirm")
    def confirm(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["SetupIntentConfirmParams"]
    ) -> "SetupIntent":
        """
        Confirm that your customer intends to set up the current or
        provided payment method. For example, you would confirm a SetupIntent
        when a customer hits the “Save” button on a payment method management
        page on your website.

        If the selected payment method does not require any additional
        steps from the customer, the SetupIntent will transition to the
        succeeded status.

        Otherwise, it will transition to the requires_action status and
        suggest additional actions via next_action. If setup fails,
        the SetupIntent will transition to the
        requires_payment_method status or the canceled status if the
        confirmation limit is reached.
        """
        return cast(
            "SetupIntent",
            self._request(
                "post",
                "/v1/setup_intents/{intent}/confirm".format(
                    intent=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_confirm_async(
        cls, intent: str, **params: Unpack["SetupIntentConfirmParams"]
    ) -> "SetupIntent":
        """
        Confirm that your customer intends to set up the current or
        provided payment method. For example, you would confirm a SetupIntent
        when a customer hits the “Save” button on a payment method management
        page on your website.

        If the selected payment method does not require any additional
        steps from the customer, the SetupIntent will transition to the
        succeeded status.

        Otherwise, it will transition to the requires_action status and
        suggest additional actions via next_action. If setup fails,
        the SetupIntent will transition to the
        requires_payment_method status or the canceled status if the
        confirmation limit is reached.
        """
        return cast(
            "SetupIntent",
            await cls._static_request_async(
                "post",
                "/v1/setup_intents/{intent}/confirm".format(
                    intent=sanitize_id(intent)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def confirm_async(
        intent: str, **params: Unpack["SetupIntentConfirmParams"]
    ) -> "SetupIntent":
        """
        Confirm that your customer intends to set up the current or
        provided payment method. For example, you would confirm a SetupIntent
        when a customer hits the “Save” button on a payment method management
        page on your website.

        If the selected payment method does not require any additional
        steps from the customer, the SetupIntent will transition to the
        succeeded status.

        Otherwise, it will transition to the requires_action status and
        suggest additional actions via next_action. If setup fails,
        the SetupIntent will transition to the
        requires_payment_method status or the canceled status if the
        confirmation limit is reached.
        """
        ...

    @overload
    async def confirm_async(
        self, **params: Unpack["SetupIntentConfirmParams"]
    ) -> "SetupIntent":
        """
        Confirm that your customer intends to set up the current or
        provided payment method. For example, you would confirm a SetupIntent
        when a customer hits the “Save” button on a payment method management
        page on your website.

        If the selected payment method does not require any additional
        steps from the customer, the SetupIntent will transition to the
        succeeded status.

        Otherwise, it will transition to the requires_action status and
        suggest additional actions via next_action. If setup fails,
        the SetupIntent will transition to the
        requires_payment_method status or the canceled status if the
        confirmation limit is reached.
        """
        ...

    @class_method_variant("_cls_confirm_async")
    async def confirm_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["SetupIntentConfirmParams"]
    ) -> "SetupIntent":
        """
        Confirm that your customer intends to set up the current or
        provided payment method. For example, you would confirm a SetupIntent
        when a customer hits the “Save” button on a payment method management
        page on your website.

        If the selected payment method does not require any additional
        steps from the customer, the SetupIntent will transition to the
        succeeded status.

        Otherwise, it will transition to the requires_action status and
        suggest additional actions via next_action. If setup fails,
        the SetupIntent will transition to the
        requires_payment_method status or the canceled status if the
        confirmation limit is reached.
        """
        return cast(
            "SetupIntent",
            await self._request_async(
                "post",
                "/v1/setup_intents/{intent}/confirm".format(
                    intent=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def create(
        cls, **params: Unpack["SetupIntentCreateParams"]
    ) -> "SetupIntent":
        """
        Creates a SetupIntent object.

        After you create the SetupIntent, attach a payment method and [confirm](https://docs.stripe.com/docs/api/setup_intents/confirm)
        it to collect any required permissions to charge the payment method later.
        """
        return cast(
            "SetupIntent",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["SetupIntentCreateParams"]
    ) -> "SetupIntent":
        """
        Creates a SetupIntent object.

        After you create the SetupIntent, attach a payment method and [confirm](https://docs.stripe.com/docs/api/setup_intents/confirm)
        it to collect any required permissions to charge the payment method later.
        """
        return cast(
            "SetupIntent",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def list(
        cls, **params: Unpack["SetupIntentListParams"]
    ) -> ListObject["SetupIntent"]:
        """
        Returns a list of SetupIntents.
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
        cls, **params: Unpack["SetupIntentListParams"]
    ) -> ListObject["SetupIntent"]:
        """
        Returns a list of SetupIntents.
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
    def modify(
        cls, id: str, **params: Unpack["SetupIntentModifyParams"]
    ) -> "SetupIntent":
        """
        Updates a SetupIntent object.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "SetupIntent",
            cls._static_request(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    async def modify_async(
        cls, id: str, **params: Unpack["SetupIntentModifyParams"]
    ) -> "SetupIntent":
        """
        Updates a SetupIntent object.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "SetupIntent",
            await cls._static_request_async(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["SetupIntentRetrieveParams"]
    ) -> "SetupIntent":
        """
        Retrieves the details of a SetupIntent that has previously been created.

        Client-side retrieval using a publishable key is allowed when the client_secret is provided in the query string.

        When retrieved with a publishable key, only a subset of properties will be returned. Please refer to the [SetupIntent](https://docs.stripe.com/api#setup_intent_object) object reference for more details.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["SetupIntentRetrieveParams"]
    ) -> "SetupIntent":
        """
        Retrieves the details of a SetupIntent that has previously been created.

        Client-side retrieval using a publishable key is allowed when the client_secret is provided in the query string.

        When retrieved with a publishable key, only a subset of properties will be returned. Please refer to the [SetupIntent](https://docs.stripe.com/api#setup_intent_object) object reference for more details.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    @classmethod
    def _cls_verify_microdeposits(
        cls,
        intent: str,
        **params: Unpack["SetupIntentVerifyMicrodepositsParams"],
    ) -> "SetupIntent":
        """
        Verifies microdeposits on a SetupIntent object.
        """
        return cast(
            "SetupIntent",
            cls._static_request(
                "post",
                "/v1/setup_intents/{intent}/verify_microdeposits".format(
                    intent=sanitize_id(intent)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def verify_microdeposits(
        intent: str, **params: Unpack["SetupIntentVerifyMicrodepositsParams"]
    ) -> "SetupIntent":
        """
        Verifies microdeposits on a SetupIntent object.
        """
        ...

    @overload
    def verify_microdeposits(
        self, **params: Unpack["SetupIntentVerifyMicrodepositsParams"]
    ) -> "SetupIntent":
        """
        Verifies microdeposits on a SetupIntent object.
        """
        ...

    @class_method_variant("_cls_verify_microdeposits")
    def verify_microdeposits(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["SetupIntentVerifyMicrodepositsParams"]
    ) -> "SetupIntent":
        """
        Verifies microdeposits on a SetupIntent object.
        """
        return cast(
            "SetupIntent",
            self._request(
                "post",
                "/v1/setup_intents/{intent}/verify_microdeposits".format(
                    intent=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_verify_microdeposits_async(
        cls,
        intent: str,
        **params: Unpack["SetupIntentVerifyMicrodepositsParams"],
    ) -> "SetupIntent":
        """
        Verifies microdeposits on a SetupIntent object.
        """
        return cast(
            "SetupIntent",
            await cls._static_request_async(
                "post",
                "/v1/setup_intents/{intent}/verify_microdeposits".format(
                    intent=sanitize_id(intent)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def verify_microdeposits_async(
        intent: str, **params: Unpack["SetupIntentVerifyMicrodepositsParams"]
    ) -> "SetupIntent":
        """
        Verifies microdeposits on a SetupIntent object.
        """
        ...

    @overload
    async def verify_microdeposits_async(
        self, **params: Unpack["SetupIntentVerifyMicrodepositsParams"]
    ) -> "SetupIntent":
        """
        Verifies microdeposits on a SetupIntent object.
        """
        ...

    @class_method_variant("_cls_verify_microdeposits_async")
    async def verify_microdeposits_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["SetupIntentVerifyMicrodepositsParams"]
    ) -> "SetupIntent":
        """
        Verifies microdeposits on a SetupIntent object.
        """
        return cast(
            "SetupIntent",
            await self._request_async(
                "post",
                "/v1/setup_intents/{intent}/verify_microdeposits".format(
                    intent=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    _inner_class_types = {
        "automatic_payment_methods": AutomaticPaymentMethods,
        "last_setup_error": LastSetupError,
        "next_action": NextAction,
        "payment_method_configuration_details": PaymentMethodConfigurationDetails,
        "payment_method_options": PaymentMethodOptions,
    }
