# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
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
    Any,
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
    from stripe._charge import Charge
    from stripe._customer import Customer
    from stripe._payment_intent_amount_details_line_item import (
        PaymentIntentAmountDetailsLineItem,
    )
    from stripe._payment_method import PaymentMethod
    from stripe._review import Review
    from stripe._setup_intent import SetupIntent
    from stripe._source import Source
    from stripe.params._payment_intent_apply_customer_balance_params import (
        PaymentIntentApplyCustomerBalanceParams,
    )
    from stripe.params._payment_intent_cancel_params import (
        PaymentIntentCancelParams,
    )
    from stripe.params._payment_intent_capture_params import (
        PaymentIntentCaptureParams,
    )
    from stripe.params._payment_intent_confirm_params import (
        PaymentIntentConfirmParams,
    )
    from stripe.params._payment_intent_create_params import (
        PaymentIntentCreateParams,
    )
    from stripe.params._payment_intent_increment_authorization_params import (
        PaymentIntentIncrementAuthorizationParams,
    )
    from stripe.params._payment_intent_list_amount_details_line_items_params import (
        PaymentIntentListAmountDetailsLineItemsParams,
    )
    from stripe.params._payment_intent_list_params import (
        PaymentIntentListParams,
    )
    from stripe.params._payment_intent_modify_params import (
        PaymentIntentModifyParams,
    )
    from stripe.params._payment_intent_retrieve_params import (
        PaymentIntentRetrieveParams,
    )
    from stripe.params._payment_intent_search_params import (
        PaymentIntentSearchParams,
    )
    from stripe.params._payment_intent_verify_microdeposits_params import (
        PaymentIntentVerifyMicrodepositsParams,
    )


@nested_resource_class_methods("amount_details_line_item")
class PaymentIntent(
    CreateableAPIResource["PaymentIntent"],
    ListableAPIResource["PaymentIntent"],
    SearchableAPIResource["PaymentIntent"],
    UpdateableAPIResource["PaymentIntent"],
):
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

    OBJECT_NAME: ClassVar[Literal["payment_intent"]] = "payment_intent"

    class AmountDetails(StripeObject):
        class Error(StripeObject):
            code: Optional[
                Literal[
                    "amount_details_amount_mismatch",
                    "amount_details_tax_shipping_discount_greater_than_amount",
                ]
            ]
            """
            The code of the error that occurred when validating the current amount details.
            """
            message: Optional[str]
            """
            A message providing more details about the error.
            """

        class Shipping(StripeObject):
            amount: Optional[int]
            """
            If a physical good is being shipped, the cost of shipping represented in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal). An integer greater than or equal to 0.
            """
            from_postal_code: Optional[str]
            """
            If a physical good is being shipped, the postal code of where it is being shipped from. At most 10 alphanumeric characters long, hyphens are allowed.
            """
            to_postal_code: Optional[str]
            """
            If a physical good is being shipped, the postal code of where it is being shipped to. At most 10 alphanumeric characters long, hyphens are allowed.
            """

        class Tax(StripeObject):
            total_tax_amount: Optional[int]
            """
            The total amount of tax on the transaction represented in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal). Required for L2 rates. An integer greater than or equal to 0.

            This field is mutually exclusive with the `amount_details[line_items][#][tax][total_tax_amount]` field.
            """

        class Tip(StripeObject):
            amount: Optional[int]
            """
            Portion of the amount that corresponds to a tip.
            """

        discount_amount: Optional[int]
        """
        The total discount applied on the transaction represented in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal). An integer greater than 0.

        This field is mutually exclusive with the `amount_details[line_items][#][discount_amount]` field.
        """
        error: Optional[Error]
        line_items: Optional[ListObject["PaymentIntentAmountDetailsLineItem"]]
        """
        A list of line items, each containing information about a product in the PaymentIntent. There is a maximum of 200 line items.
        """
        shipping: Optional[Shipping]
        tax: Optional[Tax]
        tip: Optional[Tip]
        _inner_class_types = {
            "error": Error,
            "shipping": Shipping,
            "tax": Tax,
            "tip": Tip,
        }

    class AutomaticPaymentMethods(StripeObject):
        allow_redirects: Optional[Literal["always", "never"]]
        """
        Controls whether this PaymentIntent will accept redirect-based payment methods.

        Redirect-based payment methods may require your customer to be redirected to a payment method's app or site for authentication or additional steps. To [confirm](https://docs.stripe.com/api/payment_intents/confirm) this PaymentIntent, you may be required to provide a `return_url` to redirect customers back to your site after they authenticate or complete the payment.
        """
        enabled: bool
        """
        Automatically calculates compatible payment methods
        """

    class Hooks(StripeObject):
        class Inputs(StripeObject):
            class Tax(StripeObject):
                calculation: str
                """
                The [TaxCalculation](https://docs.stripe.com/api/tax/calculations) id
                """

            tax: Optional[Tax]
            _inner_class_types = {"tax": Tax}

        inputs: Optional[Inputs]
        _inner_class_types = {"inputs": Inputs}

    class LastPaymentError(StripeObject):
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
        class AlipayHandleRedirect(StripeObject):
            native_data: Optional[str]
            """
            The native data to be used with Alipay SDK you must redirect your customer to in order to authenticate the payment in an Android App.
            """
            native_url: Optional[str]
            """
            The native URL you must redirect your customer to in order to authenticate the payment in an iOS App.
            """
            return_url: Optional[str]
            """
            If the customer does not exit their browser while authenticating, they will be redirected to this specified URL after completion.
            """
            url: Optional[str]
            """
            The URL you must redirect your customer to in order to authenticate the payment.
            """

        class BoletoDisplayDetails(StripeObject):
            expires_at: Optional[int]
            """
            The timestamp after which the boleto expires.
            """
            hosted_voucher_url: Optional[str]
            """
            The URL to the hosted boleto voucher page, which allows customers to view the boleto voucher.
            """
            number: Optional[str]
            """
            The boleto number.
            """
            pdf: Optional[str]
            """
            The URL to the downloadable boleto voucher PDF.
            """

        class CardAwaitNotification(StripeObject):
            charge_attempt_at: Optional[int]
            """
            The time that payment will be attempted. If customer approval is required, they need to provide approval before this time.
            """
            customer_approval_required: Optional[bool]
            """
            For payments greater than INR 15000, the customer must provide explicit approval of the payment with their bank. For payments of lower amount, no customer action is required.
            """

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

        class DisplayBankTransferInstructions(StripeObject):
            class FinancialAddress(StripeObject):
                class Aba(StripeObject):
                    class AccountHolderAddress(StripeObject):
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

                    class BankAddress(StripeObject):
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

                    account_holder_address: AccountHolderAddress
                    account_holder_name: str
                    """
                    The account holder name
                    """
                    account_number: str
                    """
                    The ABA account number
                    """
                    account_type: str
                    """
                    The account type
                    """
                    bank_address: BankAddress
                    bank_name: str
                    """
                    The bank name
                    """
                    routing_number: str
                    """
                    The ABA routing number
                    """
                    _inner_class_types = {
                        "account_holder_address": AccountHolderAddress,
                        "bank_address": BankAddress,
                    }

                class Iban(StripeObject):
                    class AccountHolderAddress(StripeObject):
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

                    class BankAddress(StripeObject):
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

                    account_holder_address: AccountHolderAddress
                    account_holder_name: str
                    """
                    The name of the person or business that owns the bank account
                    """
                    bank_address: BankAddress
                    bic: str
                    """
                    The BIC/SWIFT code of the account.
                    """
                    country: str
                    """
                    Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
                    """
                    iban: str
                    """
                    The IBAN of the account.
                    """
                    _inner_class_types = {
                        "account_holder_address": AccountHolderAddress,
                        "bank_address": BankAddress,
                    }

                class SortCode(StripeObject):
                    class AccountHolderAddress(StripeObject):
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

                    class BankAddress(StripeObject):
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

                    account_holder_address: AccountHolderAddress
                    account_holder_name: str
                    """
                    The name of the person or business that owns the bank account
                    """
                    account_number: str
                    """
                    The account number
                    """
                    bank_address: BankAddress
                    sort_code: str
                    """
                    The six-digit sort code
                    """
                    _inner_class_types = {
                        "account_holder_address": AccountHolderAddress,
                        "bank_address": BankAddress,
                    }

                class Spei(StripeObject):
                    class AccountHolderAddress(StripeObject):
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

                    class BankAddress(StripeObject):
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

                    account_holder_address: AccountHolderAddress
                    account_holder_name: str
                    """
                    The account holder name
                    """
                    bank_address: BankAddress
                    bank_code: str
                    """
                    The three-digit bank code
                    """
                    bank_name: str
                    """
                    The short banking institution name
                    """
                    clabe: str
                    """
                    The CLABE number
                    """
                    _inner_class_types = {
                        "account_holder_address": AccountHolderAddress,
                        "bank_address": BankAddress,
                    }

                class Swift(StripeObject):
                    class AccountHolderAddress(StripeObject):
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

                    class BankAddress(StripeObject):
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

                    account_holder_address: AccountHolderAddress
                    account_holder_name: str
                    """
                    The account holder name
                    """
                    account_number: str
                    """
                    The account number
                    """
                    account_type: str
                    """
                    The account type
                    """
                    bank_address: BankAddress
                    bank_name: str
                    """
                    The bank name
                    """
                    swift_code: str
                    """
                    The SWIFT code
                    """
                    _inner_class_types = {
                        "account_holder_address": AccountHolderAddress,
                        "bank_address": BankAddress,
                    }

                class Zengin(StripeObject):
                    class AccountHolderAddress(StripeObject):
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

                    class BankAddress(StripeObject):
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

                    account_holder_address: AccountHolderAddress
                    account_holder_name: Optional[str]
                    """
                    The account holder name
                    """
                    account_number: Optional[str]
                    """
                    The account number
                    """
                    account_type: Optional[str]
                    """
                    The bank account type. In Japan, this can only be `futsu` or `toza`.
                    """
                    bank_address: BankAddress
                    bank_code: Optional[str]
                    """
                    The bank code of the account
                    """
                    bank_name: Optional[str]
                    """
                    The bank name of the account
                    """
                    branch_code: Optional[str]
                    """
                    The branch code of the account
                    """
                    branch_name: Optional[str]
                    """
                    The branch name of the account
                    """
                    _inner_class_types = {
                        "account_holder_address": AccountHolderAddress,
                        "bank_address": BankAddress,
                    }

                aba: Optional[Aba]
                """
                ABA Records contain U.S. bank account details per the ABA format.
                """
                iban: Optional[Iban]
                """
                Iban Records contain E.U. bank account details per the SEPA format.
                """
                sort_code: Optional[SortCode]
                """
                Sort Code Records contain U.K. bank account details per the sort code format.
                """
                spei: Optional[Spei]
                """
                SPEI Records contain Mexico bank account details per the SPEI format.
                """
                supported_networks: Optional[
                    List[
                        Literal[
                            "ach",
                            "bacs",
                            "domestic_wire_us",
                            "fps",
                            "sepa",
                            "spei",
                            "swift",
                            "zengin",
                        ]
                    ]
                ]
                """
                The payment networks supported by this FinancialAddress
                """
                swift: Optional[Swift]
                """
                SWIFT Records contain U.S. bank account details per the SWIFT format.
                """
                type: Literal[
                    "aba", "iban", "sort_code", "spei", "swift", "zengin"
                ]
                """
                The type of financial address
                """
                zengin: Optional[Zengin]
                """
                Zengin Records contain Japan bank account details per the Zengin format.
                """
                _inner_class_types = {
                    "aba": Aba,
                    "iban": Iban,
                    "sort_code": SortCode,
                    "spei": Spei,
                    "swift": Swift,
                    "zengin": Zengin,
                }

            amount_remaining: Optional[int]
            """
            The remaining amount that needs to be transferred to complete the payment.
            """
            currency: Optional[str]
            """
            Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
            """
            financial_addresses: Optional[List[FinancialAddress]]
            """
            A list of financial addresses that can be used to fund the customer balance
            """
            hosted_instructions_url: Optional[str]
            """
            A link to a hosted page that guides your customer through completing the transfer.
            """
            reference: Optional[str]
            """
            A string identifying this payment. Instruct your customer to include this code in the reference or memo field of their bank transfer.
            """
            type: Literal[
                "eu_bank_transfer",
                "gb_bank_transfer",
                "jp_bank_transfer",
                "mx_bank_transfer",
                "us_bank_transfer",
            ]
            """
            Type of bank transfer
            """
            _inner_class_types = {"financial_addresses": FinancialAddress}

        class KonbiniDisplayDetails(StripeObject):
            class Stores(StripeObject):
                class Familymart(StripeObject):
                    confirmation_number: Optional[str]
                    """
                    The confirmation number.
                    """
                    payment_code: str
                    """
                    The payment code.
                    """

                class Lawson(StripeObject):
                    confirmation_number: Optional[str]
                    """
                    The confirmation number.
                    """
                    payment_code: str
                    """
                    The payment code.
                    """

                class Ministop(StripeObject):
                    confirmation_number: Optional[str]
                    """
                    The confirmation number.
                    """
                    payment_code: str
                    """
                    The payment code.
                    """

                class Seicomart(StripeObject):
                    confirmation_number: Optional[str]
                    """
                    The confirmation number.
                    """
                    payment_code: str
                    """
                    The payment code.
                    """

                familymart: Optional[Familymart]
                """
                FamilyMart instruction details.
                """
                lawson: Optional[Lawson]
                """
                Lawson instruction details.
                """
                ministop: Optional[Ministop]
                """
                Ministop instruction details.
                """
                seicomart: Optional[Seicomart]
                """
                Seicomart instruction details.
                """
                _inner_class_types = {
                    "familymart": Familymart,
                    "lawson": Lawson,
                    "ministop": Ministop,
                    "seicomart": Seicomart,
                }

            expires_at: int
            """
            The timestamp at which the pending Konbini payment expires.
            """
            hosted_voucher_url: Optional[str]
            """
            The URL for the Konbini payment instructions page, which allows customers to view and print a Konbini voucher.
            """
            stores: Stores
            _inner_class_types = {"stores": Stores}

        class MultibancoDisplayDetails(StripeObject):
            entity: Optional[str]
            """
            Entity number associated with this Multibanco payment.
            """
            expires_at: Optional[int]
            """
            The timestamp at which the Multibanco voucher expires.
            """
            hosted_voucher_url: Optional[str]
            """
            The URL for the hosted Multibanco voucher page, which allows customers to view a Multibanco voucher.
            """
            reference: Optional[str]
            """
            Reference number associated with this Multibanco payment.
            """

        class OxxoDisplayDetails(StripeObject):
            expires_after: Optional[int]
            """
            The timestamp after which the OXXO voucher expires.
            """
            hosted_voucher_url: Optional[str]
            """
            The URL for the hosted OXXO voucher page, which allows customers to view and print an OXXO voucher.
            """
            number: Optional[str]
            """
            OXXO reference number.
            """

        class PaynowDisplayQrCode(StripeObject):
            data: str
            """
            The raw data string used to generate QR code, it should be used together with QR code library.
            """
            hosted_instructions_url: Optional[str]
            """
            The URL to the hosted PayNow instructions page, which allows customers to view the PayNow QR code.
            """
            image_url_png: str
            """
            The image_url_png string used to render QR code
            """
            image_url_svg: str
            """
            The image_url_svg string used to render QR code
            """

        class PixDisplayQrCode(StripeObject):
            data: Optional[str]
            """
            The raw data string used to generate QR code, it should be used together with QR code library.
            """
            expires_at: Optional[int]
            """
            The date (unix timestamp) when the PIX expires.
            """
            hosted_instructions_url: Optional[str]
            """
            The URL to the hosted pix instructions page, which allows customers to view the pix QR code.
            """
            image_url_png: Optional[str]
            """
            The image_url_png string used to render png QR code
            """
            image_url_svg: Optional[str]
            """
            The image_url_svg string used to render svg QR code
            """

        class PromptpayDisplayQrCode(StripeObject):
            data: str
            """
            The raw data string used to generate QR code, it should be used together with QR code library.
            """
            hosted_instructions_url: str
            """
            The URL to the hosted PromptPay instructions page, which allows customers to view the PromptPay QR code.
            """
            image_url_png: str
            """
            The PNG path used to render the QR code, can be used as the source in an HTML img tag
            """
            image_url_svg: str
            """
            The SVG path used to render the QR code, can be used as the source in an HTML img tag
            """

        class RedirectToUrl(StripeObject):
            return_url: Optional[str]
            """
            If the customer does not exit their browser while authenticating, they will be redirected to this specified URL after completion.
            """
            url: Optional[str]
            """
            The URL you must redirect your customer to in order to authenticate the payment.
            """

        class SwishHandleRedirectOrDisplayQrCode(StripeObject):
            class QrCode(StripeObject):
                data: str
                """
                The raw data string used to generate QR code, it should be used together with QR code library.
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
            The URL to the hosted Swish instructions page, which allows customers to view the QR code.
            """
            mobile_auth_url: str
            """
            The url for mobile redirect based auth (for internal use only and not typically available in standard API requests).
            """
            qr_code: QrCode
            _inner_class_types = {"qr_code": QrCode}

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

        class WechatPayDisplayQrCode(StripeObject):
            data: str
            """
            The data being used to generate QR code
            """
            hosted_instructions_url: str
            """
            The URL to the hosted WeChat Pay instructions page, which allows customers to view the WeChat Pay QR code.
            """
            image_data_url: str
            """
            The base64 image data for a pre-generated QR code
            """
            image_url_png: str
            """
            The image_url_png string used to render QR code
            """
            image_url_svg: str
            """
            The image_url_svg string used to render QR code
            """

        class WechatPayRedirectToAndroidApp(StripeObject):
            app_id: str
            """
            app_id is the APP ID registered on WeChat open platform
            """
            nonce_str: str
            """
            nonce_str is a random string
            """
            package: str
            """
            package is static value
            """
            partner_id: str
            """
            an unique merchant ID assigned by WeChat Pay
            """
            prepay_id: str
            """
            an unique trading ID assigned by WeChat Pay
            """
            sign: str
            """
            A signature
            """
            timestamp: str
            """
            Specifies the current time in epoch format
            """

        class WechatPayRedirectToIosApp(StripeObject):
            native_url: str
            """
            An universal link that redirect to WeChat Pay app
            """

        alipay_handle_redirect: Optional[AlipayHandleRedirect]
        boleto_display_details: Optional[BoletoDisplayDetails]
        card_await_notification: Optional[CardAwaitNotification]
        cashapp_handle_redirect_or_display_qr_code: Optional[
            CashappHandleRedirectOrDisplayQrCode
        ]
        display_bank_transfer_instructions: Optional[
            DisplayBankTransferInstructions
        ]
        konbini_display_details: Optional[KonbiniDisplayDetails]
        multibanco_display_details: Optional[MultibancoDisplayDetails]
        oxxo_display_details: Optional[OxxoDisplayDetails]
        paynow_display_qr_code: Optional[PaynowDisplayQrCode]
        pix_display_qr_code: Optional[PixDisplayQrCode]
        promptpay_display_qr_code: Optional[PromptpayDisplayQrCode]
        redirect_to_url: Optional[RedirectToUrl]
        swish_handle_redirect_or_display_qr_code: Optional[
            SwishHandleRedirectOrDisplayQrCode
        ]
        type: str
        """
        Type of the next action to perform. Refer to the other child attributes under `next_action` for available values. Examples include: `redirect_to_url`, `use_stripe_sdk`, `alipay_handle_redirect`, `oxxo_display_details`, or `verify_with_microdeposits`.
        """
        use_stripe_sdk: Optional[Dict[str, Any]]
        """
        When confirming a PaymentIntent with Stripe.js, Stripe.js depends on the contents of this dictionary to invoke authentication flows. The shape of the contents is subject to change and is only intended to be used by Stripe.js.
        """
        verify_with_microdeposits: Optional[VerifyWithMicrodeposits]
        wechat_pay_display_qr_code: Optional[WechatPayDisplayQrCode]
        wechat_pay_redirect_to_android_app: Optional[
            WechatPayRedirectToAndroidApp
        ]
        wechat_pay_redirect_to_ios_app: Optional[WechatPayRedirectToIosApp]
        _inner_class_types = {
            "alipay_handle_redirect": AlipayHandleRedirect,
            "boleto_display_details": BoletoDisplayDetails,
            "card_await_notification": CardAwaitNotification,
            "cashapp_handle_redirect_or_display_qr_code": CashappHandleRedirectOrDisplayQrCode,
            "display_bank_transfer_instructions": DisplayBankTransferInstructions,
            "konbini_display_details": KonbiniDisplayDetails,
            "multibanco_display_details": MultibancoDisplayDetails,
            "oxxo_display_details": OxxoDisplayDetails,
            "paynow_display_qr_code": PaynowDisplayQrCode,
            "pix_display_qr_code": PixDisplayQrCode,
            "promptpay_display_qr_code": PromptpayDisplayQrCode,
            "redirect_to_url": RedirectToUrl,
            "swish_handle_redirect_or_display_qr_code": SwishHandleRedirectOrDisplayQrCode,
            "verify_with_microdeposits": VerifyWithMicrodeposits,
            "wechat_pay_display_qr_code": WechatPayDisplayQrCode,
            "wechat_pay_redirect_to_android_app": WechatPayRedirectToAndroidApp,
            "wechat_pay_redirect_to_ios_app": WechatPayRedirectToIosApp,
        }

    class PaymentDetails(StripeObject):
        customer_reference: Optional[str]
        """
        A unique value to identify the customer. This field is available only for card payments.

        This field is truncated to 25 alphanumeric characters, excluding spaces, before being sent to card networks.
        """
        order_reference: Optional[str]
        """
        A unique value assigned by the business to identify the transaction. Required for L2 and L3 rates.

        Required when the Payment Method Types array contains `card`, including when [automatic_payment_methods.enabled](https://docs.stripe.com/api/payment_intents/create#create_payment_intent-automatic_payment_methods-enabled) is set to `true`.

        For Cards, this field is truncated to 25 alphanumeric characters, excluding spaces, before being sent to card networks. For Klarna, this field is truncated to 255 characters and is visible to customers when they view the order in the Klarna app.
        """

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
            preferred_locale: Optional[str]
            """
            Preferred language of the Affirm authorization page that the customer is redirected to.
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
            reference: Optional[str]
            """
            An internal identifier or reference that this payment corresponds to. You must limit the identifier to 128 characters, and it can only contain letters, numbers, underscores, backslashes, and dashes.
            This field differs from the statement descriptor and item name.
            """
            setup_future_usage: Optional[Literal["none"]]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
            """

        class Alipay(StripeObject):
            setup_future_usage: Optional[Literal["none", "off_session"]]
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
            preferred_language: Literal["de", "en", "fr", "nl"]
            """
            Preferred language of the Bancontact authorization page that the customer is redirected to.
            """
            setup_future_usage: Optional[Literal["none", "off_session"]]
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

        class Blik(StripeObject):
            setup_future_usage: Optional[Literal["none"]]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
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
                class AvailablePlan(StripeObject):
                    count: Optional[int]
                    """
                    For `fixed_count` installment plans, this is the number of installment payments your customer will make to their credit card.
                    """
                    interval: Optional[Literal["month"]]
                    """
                    For `fixed_count` installment plans, this is the interval between installment payments your customer will make to their credit card.
                    One of `month`.
                    """
                    type: Literal["bonus", "fixed_count", "revolving"]
                    """
                    Type of installment plan, one of `fixed_count`, `bonus`, or `revolving`.
                    """

                class Plan(StripeObject):
                    count: Optional[int]
                    """
                    For `fixed_count` installment plans, this is the number of installment payments your customer will make to their credit card.
                    """
                    interval: Optional[Literal["month"]]
                    """
                    For `fixed_count` installment plans, this is the interval between installment payments your customer will make to their credit card.
                    One of `month`.
                    """
                    type: Literal["bonus", "fixed_count", "revolving"]
                    """
                    Type of installment plan, one of `fixed_count`, `bonus`, or `revolving`.
                    """

                available_plans: Optional[List[AvailablePlan]]
                """
                Installment plans that may be selected for this PaymentIntent.
                """
                enabled: bool
                """
                Whether Installments are enabled for this PaymentIntent.
                """
                plan: Optional[Plan]
                """
                Installment plan selected for this PaymentIntent.
                """
                _inner_class_types = {
                    "available_plans": AvailablePlan,
                    "plan": Plan,
                }

            class MandateOptions(StripeObject):
                amount: int
                """
                Amount to be charged for future payments.
                """
                amount_type: Literal["fixed", "maximum"]
                """
                One of `fixed` or `maximum`. If `fixed`, the `amount` param refers to the exact amount to be charged in future payments. If `maximum`, the amount charged can be up to the value passed for the `amount` param.
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

            capture_method: Optional[Literal["manual"]]
            """
            Controls when the funds will be captured from the customer's account.
            """
            installments: Optional[Installments]
            """
            Installment details for this payment.

            For more information, see the [installments integration guide](https://docs.stripe.com/payments/installments).
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
            Selected network to process this payment intent on. Depends on the available networks of the card attached to the payment intent. Can be only set confirm-time.
            """
            request_extended_authorization: Optional[
                Literal["if_available", "never"]
            ]
            """
            Request ability to [capture beyond the standard authorization validity window](https://docs.stripe.com/payments/extended-authorization) for this PaymentIntent.
            """
            request_incremental_authorization: Optional[
                Literal["if_available", "never"]
            ]
            """
            Request ability to [increment the authorization](https://docs.stripe.com/payments/incremental-authorization) for this PaymentIntent.
            """
            request_multicapture: Optional[Literal["if_available", "never"]]
            """
            Request ability to make [multiple captures](https://docs.stripe.com/payments/multicapture) for this PaymentIntent.
            """
            request_overcapture: Optional[Literal["if_available", "never"]]
            """
            Request ability to [overcapture](https://docs.stripe.com/payments/overcapture) for this PaymentIntent.
            """
            request_three_d_secure: Optional[
                Literal["any", "automatic", "challenge"]
            ]
            """
            We strongly recommend that you rely on our SCA Engine to automatically prompt your customers for authentication based on risk level and [other requirements](https://docs.stripe.com/strong-customer-authentication). However, if you wish to request 3D Secure based on logic from your own fraud engine, provide this option. If not provided, this value defaults to `automatic`. Read our guide on [manually requesting 3D Secure](https://docs.stripe.com/payments/3d-secure/authentication-flow#manual-three-ds) for more information on how this configuration interacts with Radar and our SCA Engine.
            """
            require_cvc_recollection: Optional[bool]
            """
            When enabled, using a card that is attached to a customer will require the CVC to be provided again (i.e. using the cvc_token parameter).
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
                "mandate_options": MandateOptions,
            }

        class CardPresent(StripeObject):
            class Routing(StripeObject):
                requested_priority: Optional[
                    Literal["domestic", "international"]
                ]
                """
                Requested routing priority
                """

            capture_method: Optional[Literal["manual", "manual_preferred"]]
            """
            Controls when the funds will be captured from the customer's account.
            """
            request_extended_authorization: Optional[bool]
            """
            Request ability to capture this payment beyond the standard [authorization validity window](https://docs.stripe.com/terminal/features/extended-authorizations#authorization-validity)
            """
            request_incremental_authorization_support: Optional[bool]
            """
            Request ability to [increment](https://docs.stripe.com/terminal/features/incremental-authorizations) this PaymentIntent if the combination of MCC and card brand is eligible. Check [incremental_authorization_supported](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-incremental_authorization_supported) in the [Confirm](https://docs.stripe.com/api/payment_intents/confirm) response to verify support.
            """
            routing: Optional[Routing]
            _inner_class_types = {"routing": Routing}

        class Cashapp(StripeObject):
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

        class Crypto(StripeObject):
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
            setup_future_usage: Optional[Literal["none", "off_session"]]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
            """

        class InteracPresent(StripeObject):
            pass

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
            preferred_locale: Optional[str]
            """
            Preferred locale of the Klarna checkout page that the customer is redirected to.
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
            confirmation_number: Optional[str]
            """
            An optional 10 to 11 digit numeric-only string determining the confirmation code at applicable convenience stores.
            """
            expires_after_days: Optional[int]
            """
            The number of calendar days (between 1 and 60) after which Konbini payment instructions will expire. For example, if a PaymentIntent is confirmed with Konbini and `expires_after_days` set to 2 on Monday JST, the instructions will expire on Wednesday 23:59:59 JST.
            """
            expires_at: Optional[int]
            """
            The timestamp at which the Konbini payment instructions will expire. Only one of `expires_after_days` or `expires_at` may be set.
            """
            product_description: Optional[str]
            """
            A product descriptor of up to 22 characters, which will appear to customers at the convenience store.
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
            persistent_token: Optional[str]
            """
            [Deprecated] This is a legacy parameter that no longer has any function.
            """
            setup_future_usage: Optional[Literal["none", "off_session"]]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
            """

        class MbWay(StripeObject):
            setup_future_usage: Optional[Literal["none"]]
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

        class NzBankAccount(StripeObject):
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

        class PayByBank(StripeObject):
            pass

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
            The number of seconds (between 10 and 1209600) after which Pix payment will expire.
            """
            expires_at: Optional[int]
            """
            The timestamp at which the Pix expires.
            """
            setup_future_usage: Optional[Literal["none"]]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
            """

        class Promptpay(StripeObject):
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
            preferred_language: Optional[
                Literal["de", "en", "es", "fr", "it", "nl", "pl"]
            ]
            """
            Preferred language of the SOFORT authorization page that the customer is redirected to.
            """
            setup_future_usage: Optional[Literal["none", "off_session"]]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
            """

        class Swish(StripeObject):
            reference: Optional[str]
            """
            A reference for this payment to be displayed in the Swish app.
            """
            setup_future_usage: Optional[Literal["none"]]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
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

            class MandateOptions(StripeObject):
                collection_method: Optional[Literal["paper"]]
                """
                Mandate collection method
                """

            financial_connections: Optional[FinancialConnections]
            mandate_options: Optional[MandateOptions]
            preferred_settlement_speed: Optional[
                Literal["fastest", "standard"]
            ]
            """
            Preferred transaction settlement speed
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
            target_date: Optional[str]
            """
            Controls when Stripe will attempt to debit the funds from the customer's account. The date must be a string in YYYY-MM-DD format. The date must be in the future and between 3 and 15 calendar days from now.
            """
            transaction_purpose: Optional[
                Literal["goods", "other", "services", "unspecified"]
            ]
            """
            The purpose of the transaction.
            """
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

        class WechatPay(StripeObject):
            app_id: Optional[str]
            """
            The app ID registered with WeChat Pay. Only required when client is ios or android.
            """
            client: Optional[Literal["android", "ios", "web"]]
            """
            The client type that the end customer will pay from
            """
            setup_future_usage: Optional[Literal["none"]]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
            """

        class Zip(StripeObject):
            setup_future_usage: Optional[Literal["none"]]
            """
            Indicates that you intend to make future payments with this PaymentIntent's payment method.

            If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

            If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

            When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
            """

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
        blik: Optional[Blik]
        boleto: Optional[Boleto]
        card: Optional[Card]
        card_present: Optional[CardPresent]
        cashapp: Optional[Cashapp]
        crypto: Optional[Crypto]
        customer_balance: Optional[CustomerBalance]
        eps: Optional[Eps]
        fpx: Optional[Fpx]
        giropay: Optional[Giropay]
        grabpay: Optional[Grabpay]
        ideal: Optional[Ideal]
        interac_present: Optional[InteracPresent]
        kakao_pay: Optional[KakaoPay]
        klarna: Optional[Klarna]
        konbini: Optional[Konbini]
        kr_card: Optional[KrCard]
        link: Optional[Link]
        mb_way: Optional[MbWay]
        mobilepay: Optional[Mobilepay]
        multibanco: Optional[Multibanco]
        naver_pay: Optional[NaverPay]
        nz_bank_account: Optional[NzBankAccount]
        oxxo: Optional[Oxxo]
        p24: Optional[P24]
        pay_by_bank: Optional[PayByBank]
        payco: Optional[Payco]
        paynow: Optional[Paynow]
        paypal: Optional[Paypal]
        payto: Optional[Payto]
        pix: Optional[Pix]
        promptpay: Optional[Promptpay]
        revolut_pay: Optional[RevolutPay]
        samsung_pay: Optional[SamsungPay]
        satispay: Optional[Satispay]
        sepa_debit: Optional[SepaDebit]
        sofort: Optional[Sofort]
        swish: Optional[Swish]
        twint: Optional[Twint]
        us_bank_account: Optional[UsBankAccount]
        wechat_pay: Optional[WechatPay]
        zip: Optional[Zip]
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
            "blik": Blik,
            "boleto": Boleto,
            "card": Card,
            "card_present": CardPresent,
            "cashapp": Cashapp,
            "crypto": Crypto,
            "customer_balance": CustomerBalance,
            "eps": Eps,
            "fpx": Fpx,
            "giropay": Giropay,
            "grabpay": Grabpay,
            "ideal": Ideal,
            "interac_present": InteracPresent,
            "kakao_pay": KakaoPay,
            "klarna": Klarna,
            "konbini": Konbini,
            "kr_card": KrCard,
            "link": Link,
            "mb_way": MbWay,
            "mobilepay": Mobilepay,
            "multibanco": Multibanco,
            "naver_pay": NaverPay,
            "nz_bank_account": NzBankAccount,
            "oxxo": Oxxo,
            "p24": P24,
            "pay_by_bank": PayByBank,
            "payco": Payco,
            "paynow": Paynow,
            "paypal": Paypal,
            "payto": Payto,
            "pix": Pix,
            "promptpay": Promptpay,
            "revolut_pay": RevolutPay,
            "samsung_pay": SamsungPay,
            "satispay": Satispay,
            "sepa_debit": SepaDebit,
            "sofort": Sofort,
            "swish": Swish,
            "twint": Twint,
            "us_bank_account": UsBankAccount,
            "wechat_pay": WechatPay,
            "zip": Zip,
        }

    class PresentmentDetails(StripeObject):
        presentment_amount: int
        """
        Amount intended to be collected by this payment, denominated in `presentment_currency`.
        """
        presentment_currency: str
        """
        Currency presented to the customer during payment.
        """

    class Processing(StripeObject):
        class Card(StripeObject):
            class CustomerNotification(StripeObject):
                approval_requested: Optional[bool]
                """
                Whether customer approval has been requested for this payment. For payments greater than INR 15000 or mandate amount, the customer must provide explicit approval of the payment with their bank.
                """
                completes_at: Optional[int]
                """
                If customer approval is required, they need to provide approval before this time.
                """

            customer_notification: Optional[CustomerNotification]
            _inner_class_types = {
                "customer_notification": CustomerNotification
            }

        card: Optional[Card]
        type: Literal["card"]
        """
        Type of the payment method for which payment is in `processing` state, one of `card`.
        """
        _inner_class_types = {"card": Card}

    class Shipping(StripeObject):
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

    class TransferData(StripeObject):
        amount: Optional[int]
        """
        The amount transferred to the destination account. This transfer will occur automatically after the payment succeeds. If no amount is specified, by default the entire payment amount is transferred to the destination account.
         The amount must be less than or equal to the [amount](https://docs.stripe.com/api/payment_intents/object#payment_intent_object-amount), and must be a positive integer
         representing how much to transfer in the smallest currency unit (e.g., 100 cents to charge $1.00).
        """
        destination: ExpandableField["Account"]
        """
        The account (if any) that the payment is attributed to for tax reporting, and where funds from the payment are transferred to after payment success.
        """

    amount: int
    """
    Amount intended to be collected by this PaymentIntent. A positive integer representing how much to charge in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal) (e.g., 100 cents to charge $1.00 or 100 to charge ¥100, a zero-decimal currency). The minimum amount is $0.50 US or [equivalent in charge currency](https://docs.stripe.com/currencies#minimum-and-maximum-charge-amounts). The amount value supports up to eight digits (e.g., a value of 99999999 for a USD charge of $999,999.99).
    """
    amount_capturable: int
    """
    Amount that can be captured from this PaymentIntent.
    """
    amount_details: Optional[AmountDetails]
    amount_received: int
    """
    Amount that this PaymentIntent collects.
    """
    application: Optional[ExpandableField["Application"]]
    """
    ID of the Connect application that created the PaymentIntent.
    """
    application_fee_amount: Optional[int]
    """
    The amount of the application fee (if any) that will be requested to be applied to the payment and transferred to the application owner's Stripe account. The amount of the application fee collected will be capped at the total amount captured. For more information, see the PaymentIntents [use case for connected accounts](https://docs.stripe.com/payments/connected-accounts).
    """
    automatic_payment_methods: Optional[AutomaticPaymentMethods]
    """
    Settings to configure compatible payment methods from the [Stripe Dashboard](https://dashboard.stripe.com/settings/payment_methods)
    """
    canceled_at: Optional[int]
    """
    Populated when `status` is `canceled`, this is the time at which the PaymentIntent was canceled. Measured in seconds since the Unix epoch.
    """
    cancellation_reason: Optional[
        Literal[
            "abandoned",
            "automatic",
            "duplicate",
            "expired",
            "failed_invoice",
            "fraudulent",
            "requested_by_customer",
            "void_invoice",
        ]
    ]
    """
    Reason for cancellation of this PaymentIntent, either user-provided (`duplicate`, `fraudulent`, `requested_by_customer`, or `abandoned`) or generated by Stripe internally (`failed_invoice`, `void_invoice`, `automatic`, or `expired`).
    """
    capture_method: Literal["automatic", "automatic_async", "manual"]
    """
    Controls when the funds will be captured from the customer's account.
    """
    client_secret: Optional[str]
    """
    The client secret of this PaymentIntent. Used for client-side retrieval using a publishable key.

    The client secret can be used to complete a payment from your frontend. It should not be stored, logged, or exposed to anyone other than the customer. Make sure that you have TLS enabled on any page that includes the client secret.

    Refer to our docs to [accept a payment](https://docs.stripe.com/payments/accept-a-payment?ui=elements) and learn about how `client_secret` should be handled.
    """
    confirmation_method: Literal["automatic", "manual"]
    """
    Describes whether we can confirm this PaymentIntent automatically, or if it requires customer action to confirm the payment.
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    customer: Optional[ExpandableField["Customer"]]
    """
    ID of the Customer this PaymentIntent belongs to, if one exists.

    Payment methods attached to other Customers cannot be used with this PaymentIntent.

    If [setup_future_usage](https://api.stripe.com#payment_intent_object-setup_future_usage) is set and this PaymentIntent's payment method is not `card_present`, then the payment method attaches to the Customer after the PaymentIntent has been confirmed and any required actions from the user are complete. If the payment method is `card_present` and isn't a digital wallet, then a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card is created and attached to the Customer instead.
    """
    customer_account: Optional[str]
    """
    ID of the Account representing the customer that this PaymentIntent belongs to, if one exists.

    Payment methods attached to other Accounts cannot be used with this PaymentIntent.

    If [setup_future_usage](https://api.stripe.com#payment_intent_object-setup_future_usage) is set and this PaymentIntent's payment method is not `card_present`, then the payment method attaches to the Account after the PaymentIntent has been confirmed and any required actions from the user are complete. If the payment method is `card_present` and isn't a digital wallet, then a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card is created and attached to the Account instead.
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
    The list of payment method types to exclude from use with this payment.
    """
    hooks: Optional[Hooks]
    id: str
    """
    Unique identifier for the object.
    """
    last_payment_error: Optional[LastPaymentError]
    """
    The payment error encountered in the previous PaymentIntent confirmation. It will be cleared if the PaymentIntent is later updated for any reason.
    """
    latest_charge: Optional[ExpandableField["Charge"]]
    """
    ID of the latest [Charge object](https://docs.stripe.com/api/charges) created by this PaymentIntent. This property is `null` until PaymentIntent confirmation is attempted.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    metadata: Dict[str, str]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Learn more about [storing information in metadata](https://docs.stripe.com/payments/payment-intents/creating-payment-intents#storing-information-in-metadata).
    """
    next_action: Optional[NextAction]
    """
    If present, this property tells you what actions you need to take in order for your customer to fulfill a payment using the provided source.
    """
    object: Literal["payment_intent"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    on_behalf_of: Optional[ExpandableField["Account"]]
    """
    You can specify the settlement merchant as the
    connected account using the `on_behalf_of` attribute on the charge. See the PaymentIntents [use case for connected accounts](https://docs.stripe.com/payments/connected-accounts) for details.
    """
    payment_details: Optional[PaymentDetails]
    payment_method: Optional[ExpandableField["PaymentMethod"]]
    """
    ID of the payment method used in this PaymentIntent.
    """
    payment_method_configuration_details: Optional[
        PaymentMethodConfigurationDetails
    ]
    """
    Information about the [payment method configuration](https://docs.stripe.com/api/payment_method_configurations) used for this PaymentIntent.
    """
    payment_method_options: Optional[PaymentMethodOptions]
    """
    Payment-method-specific configuration for this PaymentIntent.
    """
    payment_method_types: List[str]
    """
    The list of payment method types (e.g. card) that this PaymentIntent is allowed to use. A comprehensive list of valid payment method types can be found [here](https://docs.stripe.com/api/payment_methods/object#payment_method_object-type).
    """
    presentment_details: Optional[PresentmentDetails]
    processing: Optional[Processing]
    """
    If present, this property tells you about the processing state of the payment.
    """
    receipt_email: Optional[str]
    """
    Email address that the receipt for the resulting payment will be sent to. If `receipt_email` is specified for a payment in live mode, a receipt will be sent regardless of your [email settings](https://dashboard.stripe.com/account/emails).
    """
    review: Optional[ExpandableField["Review"]]
    """
    ID of the review associated with this PaymentIntent, if any.
    """
    setup_future_usage: Optional[Literal["off_session", "on_session"]]
    """
    Indicates that you intend to make future payments with this PaymentIntent's payment method.

    If you provide a Customer with the PaymentIntent, you can use this parameter to [attach the payment method](https://docs.stripe.com/payments/save-during-payment) to the Customer after the PaymentIntent is confirmed and the customer completes any required actions. If you don't provide a Customer, you can still [attach](https://docs.stripe.com/api/payment_methods/attach) the payment method to a Customer after the transaction completes.

    If the payment method is `card_present` and isn't a digital wallet, Stripe creates and attaches a [generated_card](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-card_present-generated_card) payment method representing the card to the Customer instead.

    When processing card payments, Stripe uses `setup_future_usage` to help you comply with regional legislation and network rules, such as [SCA](https://docs.stripe.com/strong-customer-authentication).
    """
    shipping: Optional[Shipping]
    """
    Shipping information for this PaymentIntent.
    """
    source: Optional[
        ExpandableField[
            Union["Account", "BankAccount", "CardResource", "Source"]
        ]
    ]
    """
    This is a legacy field that will be removed in the future. It is the ID of the Source object that is associated with this PaymentIntent, if one was supplied.
    """
    statement_descriptor: Optional[str]
    """
    Text that appears on the customer's statement as the statement descriptor for a non-card charge. This value overrides the account's default statement descriptor. For information about requirements, including the 22-character limit, see [the Statement Descriptor docs](https://docs.stripe.com/get-started/account/statement-descriptors).

    Setting this value for a card charge returns an error. For card charges, set the [statement_descriptor_suffix](https://docs.stripe.com/get-started/account/statement-descriptors#dynamic) instead.
    """
    statement_descriptor_suffix: Optional[str]
    """
    Provides information about a card charge. Concatenated to the account's [statement descriptor prefix](https://docs.stripe.com/get-started/account/statement-descriptors#static) to form the complete statement descriptor that appears on the customer's statement.
    """
    status: Literal[
        "canceled",
        "processing",
        "requires_action",
        "requires_capture",
        "requires_confirmation",
        "requires_payment_method",
        "succeeded",
    ]
    """
    Status of this PaymentIntent, one of `requires_payment_method`, `requires_confirmation`, `requires_action`, `processing`, `requires_capture`, `canceled`, or `succeeded`. Read more about each PaymentIntent [status](https://docs.stripe.com/payments/intents#intent-statuses).
    """
    transfer_data: Optional[TransferData]
    """
    The data that automatically creates a Transfer after the payment finalizes. Learn more about the [use case for connected accounts](https://docs.stripe.com/payments/connected-accounts).
    """
    transfer_group: Optional[str]
    """
    A string that identifies the resulting payment as part of a group. Learn more about the [use case for connected accounts](https://docs.stripe.com/connect/separate-charges-and-transfers).
    """

    @classmethod
    def _cls_apply_customer_balance(
        cls,
        intent: str,
        **params: Unpack["PaymentIntentApplyCustomerBalanceParams"],
    ) -> "PaymentIntent":
        """
        Manually reconcile the remaining amount for a customer_balance PaymentIntent.
        """
        return cast(
            "PaymentIntent",
            cls._static_request(
                "post",
                "/v1/payment_intents/{intent}/apply_customer_balance".format(
                    intent=sanitize_id(intent)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def apply_customer_balance(
        intent: str,
        **params: Unpack["PaymentIntentApplyCustomerBalanceParams"],
    ) -> "PaymentIntent":
        """
        Manually reconcile the remaining amount for a customer_balance PaymentIntent.
        """
        ...

    @overload
    def apply_customer_balance(
        self, **params: Unpack["PaymentIntentApplyCustomerBalanceParams"]
    ) -> "PaymentIntent":
        """
        Manually reconcile the remaining amount for a customer_balance PaymentIntent.
        """
        ...

    @class_method_variant("_cls_apply_customer_balance")
    def apply_customer_balance(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["PaymentIntentApplyCustomerBalanceParams"]
    ) -> "PaymentIntent":
        """
        Manually reconcile the remaining amount for a customer_balance PaymentIntent.
        """
        return cast(
            "PaymentIntent",
            self._request(
                "post",
                "/v1/payment_intents/{intent}/apply_customer_balance".format(
                    intent=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_apply_customer_balance_async(
        cls,
        intent: str,
        **params: Unpack["PaymentIntentApplyCustomerBalanceParams"],
    ) -> "PaymentIntent":
        """
        Manually reconcile the remaining amount for a customer_balance PaymentIntent.
        """
        return cast(
            "PaymentIntent",
            await cls._static_request_async(
                "post",
                "/v1/payment_intents/{intent}/apply_customer_balance".format(
                    intent=sanitize_id(intent)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def apply_customer_balance_async(
        intent: str,
        **params: Unpack["PaymentIntentApplyCustomerBalanceParams"],
    ) -> "PaymentIntent":
        """
        Manually reconcile the remaining amount for a customer_balance PaymentIntent.
        """
        ...

    @overload
    async def apply_customer_balance_async(
        self, **params: Unpack["PaymentIntentApplyCustomerBalanceParams"]
    ) -> "PaymentIntent":
        """
        Manually reconcile the remaining amount for a customer_balance PaymentIntent.
        """
        ...

    @class_method_variant("_cls_apply_customer_balance_async")
    async def apply_customer_balance_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["PaymentIntentApplyCustomerBalanceParams"]
    ) -> "PaymentIntent":
        """
        Manually reconcile the remaining amount for a customer_balance PaymentIntent.
        """
        return cast(
            "PaymentIntent",
            await self._request_async(
                "post",
                "/v1/payment_intents/{intent}/apply_customer_balance".format(
                    intent=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def _cls_cancel(
        cls, intent: str, **params: Unpack["PaymentIntentCancelParams"]
    ) -> "PaymentIntent":
        """
        You can cancel a PaymentIntent object when it's in one of these statuses: requires_payment_method, requires_capture, requires_confirmation, requires_action or, [in rare cases](https://docs.stripe.com/docs/payments/intents), processing.

        After it's canceled, no additional charges are made by the PaymentIntent and any operations on the PaymentIntent fail with an error. For PaymentIntents with a status of requires_capture, the remaining amount_capturable is automatically refunded.

        You can't cancel the PaymentIntent for a Checkout Session. [Expire the Checkout Session](https://docs.stripe.com/docs/api/checkout/sessions/expire) instead.
        """
        return cast(
            "PaymentIntent",
            cls._static_request(
                "post",
                "/v1/payment_intents/{intent}/cancel".format(
                    intent=sanitize_id(intent)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def cancel(
        intent: str, **params: Unpack["PaymentIntentCancelParams"]
    ) -> "PaymentIntent":
        """
        You can cancel a PaymentIntent object when it's in one of these statuses: requires_payment_method, requires_capture, requires_confirmation, requires_action or, [in rare cases](https://docs.stripe.com/docs/payments/intents), processing.

        After it's canceled, no additional charges are made by the PaymentIntent and any operations on the PaymentIntent fail with an error. For PaymentIntents with a status of requires_capture, the remaining amount_capturable is automatically refunded.

        You can't cancel the PaymentIntent for a Checkout Session. [Expire the Checkout Session](https://docs.stripe.com/docs/api/checkout/sessions/expire) instead.
        """
        ...

    @overload
    def cancel(
        self, **params: Unpack["PaymentIntentCancelParams"]
    ) -> "PaymentIntent":
        """
        You can cancel a PaymentIntent object when it's in one of these statuses: requires_payment_method, requires_capture, requires_confirmation, requires_action or, [in rare cases](https://docs.stripe.com/docs/payments/intents), processing.

        After it's canceled, no additional charges are made by the PaymentIntent and any operations on the PaymentIntent fail with an error. For PaymentIntents with a status of requires_capture, the remaining amount_capturable is automatically refunded.

        You can't cancel the PaymentIntent for a Checkout Session. [Expire the Checkout Session](https://docs.stripe.com/docs/api/checkout/sessions/expire) instead.
        """
        ...

    @class_method_variant("_cls_cancel")
    def cancel(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["PaymentIntentCancelParams"]
    ) -> "PaymentIntent":
        """
        You can cancel a PaymentIntent object when it's in one of these statuses: requires_payment_method, requires_capture, requires_confirmation, requires_action or, [in rare cases](https://docs.stripe.com/docs/payments/intents), processing.

        After it's canceled, no additional charges are made by the PaymentIntent and any operations on the PaymentIntent fail with an error. For PaymentIntents with a status of requires_capture, the remaining amount_capturable is automatically refunded.

        You can't cancel the PaymentIntent for a Checkout Session. [Expire the Checkout Session](https://docs.stripe.com/docs/api/checkout/sessions/expire) instead.
        """
        return cast(
            "PaymentIntent",
            self._request(
                "post",
                "/v1/payment_intents/{intent}/cancel".format(
                    intent=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_cancel_async(
        cls, intent: str, **params: Unpack["PaymentIntentCancelParams"]
    ) -> "PaymentIntent":
        """
        You can cancel a PaymentIntent object when it's in one of these statuses: requires_payment_method, requires_capture, requires_confirmation, requires_action or, [in rare cases](https://docs.stripe.com/docs/payments/intents), processing.

        After it's canceled, no additional charges are made by the PaymentIntent and any operations on the PaymentIntent fail with an error. For PaymentIntents with a status of requires_capture, the remaining amount_capturable is automatically refunded.

        You can't cancel the PaymentIntent for a Checkout Session. [Expire the Checkout Session](https://docs.stripe.com/docs/api/checkout/sessions/expire) instead.
        """
        return cast(
            "PaymentIntent",
            await cls._static_request_async(
                "post",
                "/v1/payment_intents/{intent}/cancel".format(
                    intent=sanitize_id(intent)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def cancel_async(
        intent: str, **params: Unpack["PaymentIntentCancelParams"]
    ) -> "PaymentIntent":
        """
        You can cancel a PaymentIntent object when it's in one of these statuses: requires_payment_method, requires_capture, requires_confirmation, requires_action or, [in rare cases](https://docs.stripe.com/docs/payments/intents), processing.

        After it's canceled, no additional charges are made by the PaymentIntent and any operations on the PaymentIntent fail with an error. For PaymentIntents with a status of requires_capture, the remaining amount_capturable is automatically refunded.

        You can't cancel the PaymentIntent for a Checkout Session. [Expire the Checkout Session](https://docs.stripe.com/docs/api/checkout/sessions/expire) instead.
        """
        ...

    @overload
    async def cancel_async(
        self, **params: Unpack["PaymentIntentCancelParams"]
    ) -> "PaymentIntent":
        """
        You can cancel a PaymentIntent object when it's in one of these statuses: requires_payment_method, requires_capture, requires_confirmation, requires_action or, [in rare cases](https://docs.stripe.com/docs/payments/intents), processing.

        After it's canceled, no additional charges are made by the PaymentIntent and any operations on the PaymentIntent fail with an error. For PaymentIntents with a status of requires_capture, the remaining amount_capturable is automatically refunded.

        You can't cancel the PaymentIntent for a Checkout Session. [Expire the Checkout Session](https://docs.stripe.com/docs/api/checkout/sessions/expire) instead.
        """
        ...

    @class_method_variant("_cls_cancel_async")
    async def cancel_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["PaymentIntentCancelParams"]
    ) -> "PaymentIntent":
        """
        You can cancel a PaymentIntent object when it's in one of these statuses: requires_payment_method, requires_capture, requires_confirmation, requires_action or, [in rare cases](https://docs.stripe.com/docs/payments/intents), processing.

        After it's canceled, no additional charges are made by the PaymentIntent and any operations on the PaymentIntent fail with an error. For PaymentIntents with a status of requires_capture, the remaining amount_capturable is automatically refunded.

        You can't cancel the PaymentIntent for a Checkout Session. [Expire the Checkout Session](https://docs.stripe.com/docs/api/checkout/sessions/expire) instead.
        """
        return cast(
            "PaymentIntent",
            await self._request_async(
                "post",
                "/v1/payment_intents/{intent}/cancel".format(
                    intent=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def _cls_capture(
        cls, intent: str, **params: Unpack["PaymentIntentCaptureParams"]
    ) -> "PaymentIntent":
        """
        Capture the funds of an existing uncaptured PaymentIntent when its status is requires_capture.

        Uncaptured PaymentIntents are cancelled a set number of days (7 by default) after their creation.

        Learn more about [separate authorization and capture](https://docs.stripe.com/docs/payments/capture-later).
        """
        return cast(
            "PaymentIntent",
            cls._static_request(
                "post",
                "/v1/payment_intents/{intent}/capture".format(
                    intent=sanitize_id(intent)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def capture(
        intent: str, **params: Unpack["PaymentIntentCaptureParams"]
    ) -> "PaymentIntent":
        """
        Capture the funds of an existing uncaptured PaymentIntent when its status is requires_capture.

        Uncaptured PaymentIntents are cancelled a set number of days (7 by default) after their creation.

        Learn more about [separate authorization and capture](https://docs.stripe.com/docs/payments/capture-later).
        """
        ...

    @overload
    def capture(
        self, **params: Unpack["PaymentIntentCaptureParams"]
    ) -> "PaymentIntent":
        """
        Capture the funds of an existing uncaptured PaymentIntent when its status is requires_capture.

        Uncaptured PaymentIntents are cancelled a set number of days (7 by default) after their creation.

        Learn more about [separate authorization and capture](https://docs.stripe.com/docs/payments/capture-later).
        """
        ...

    @class_method_variant("_cls_capture")
    def capture(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["PaymentIntentCaptureParams"]
    ) -> "PaymentIntent":
        """
        Capture the funds of an existing uncaptured PaymentIntent when its status is requires_capture.

        Uncaptured PaymentIntents are cancelled a set number of days (7 by default) after their creation.

        Learn more about [separate authorization and capture](https://docs.stripe.com/docs/payments/capture-later).
        """
        return cast(
            "PaymentIntent",
            self._request(
                "post",
                "/v1/payment_intents/{intent}/capture".format(
                    intent=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_capture_async(
        cls, intent: str, **params: Unpack["PaymentIntentCaptureParams"]
    ) -> "PaymentIntent":
        """
        Capture the funds of an existing uncaptured PaymentIntent when its status is requires_capture.

        Uncaptured PaymentIntents are cancelled a set number of days (7 by default) after their creation.

        Learn more about [separate authorization and capture](https://docs.stripe.com/docs/payments/capture-later).
        """
        return cast(
            "PaymentIntent",
            await cls._static_request_async(
                "post",
                "/v1/payment_intents/{intent}/capture".format(
                    intent=sanitize_id(intent)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def capture_async(
        intent: str, **params: Unpack["PaymentIntentCaptureParams"]
    ) -> "PaymentIntent":
        """
        Capture the funds of an existing uncaptured PaymentIntent when its status is requires_capture.

        Uncaptured PaymentIntents are cancelled a set number of days (7 by default) after their creation.

        Learn more about [separate authorization and capture](https://docs.stripe.com/docs/payments/capture-later).
        """
        ...

    @overload
    async def capture_async(
        self, **params: Unpack["PaymentIntentCaptureParams"]
    ) -> "PaymentIntent":
        """
        Capture the funds of an existing uncaptured PaymentIntent when its status is requires_capture.

        Uncaptured PaymentIntents are cancelled a set number of days (7 by default) after their creation.

        Learn more about [separate authorization and capture](https://docs.stripe.com/docs/payments/capture-later).
        """
        ...

    @class_method_variant("_cls_capture_async")
    async def capture_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["PaymentIntentCaptureParams"]
    ) -> "PaymentIntent":
        """
        Capture the funds of an existing uncaptured PaymentIntent when its status is requires_capture.

        Uncaptured PaymentIntents are cancelled a set number of days (7 by default) after their creation.

        Learn more about [separate authorization and capture](https://docs.stripe.com/docs/payments/capture-later).
        """
        return cast(
            "PaymentIntent",
            await self._request_async(
                "post",
                "/v1/payment_intents/{intent}/capture".format(
                    intent=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def _cls_confirm(
        cls, intent: str, **params: Unpack["PaymentIntentConfirmParams"]
    ) -> "PaymentIntent":
        """
        Confirm that your customer intends to pay with current or provided
        payment method. Upon confirmation, the PaymentIntent will attempt to initiate
        a payment.

        If the selected payment method requires additional authentication steps, the
        PaymentIntent will transition to the requires_action status and
        suggest additional actions via next_action. If payment fails,
        the PaymentIntent transitions to the requires_payment_method status or the
        canceled status if the confirmation limit is reached. If
        payment succeeds, the PaymentIntent will transition to the succeeded
        status (or requires_capture, if capture_method is set to manual).

        If the confirmation_method is automatic, payment may be attempted
        using our [client SDKs](https://docs.stripe.com/docs/stripe-js/reference#stripe-handle-card-payment)
        and the PaymentIntent's [client_secret](https://docs.stripe.com/api#payment_intent_object-client_secret).
        After next_actions are handled by the client, no additional
        confirmation is required to complete the payment.

        If the confirmation_method is manual, all payment attempts must be
        initiated using a secret key.

        If any actions are required for the payment, the PaymentIntent will
        return to the requires_confirmation state
        after those actions are completed. Your server needs to then
        explicitly re-confirm the PaymentIntent to initiate the next payment
        attempt.

        There is a variable upper limit on how many times a PaymentIntent can be confirmed.
        After this limit is reached, any further calls to this endpoint will
        transition the PaymentIntent to the canceled state.
        """
        return cast(
            "PaymentIntent",
            cls._static_request(
                "post",
                "/v1/payment_intents/{intent}/confirm".format(
                    intent=sanitize_id(intent)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def confirm(
        intent: str, **params: Unpack["PaymentIntentConfirmParams"]
    ) -> "PaymentIntent":
        """
        Confirm that your customer intends to pay with current or provided
        payment method. Upon confirmation, the PaymentIntent will attempt to initiate
        a payment.

        If the selected payment method requires additional authentication steps, the
        PaymentIntent will transition to the requires_action status and
        suggest additional actions via next_action. If payment fails,
        the PaymentIntent transitions to the requires_payment_method status or the
        canceled status if the confirmation limit is reached. If
        payment succeeds, the PaymentIntent will transition to the succeeded
        status (or requires_capture, if capture_method is set to manual).

        If the confirmation_method is automatic, payment may be attempted
        using our [client SDKs](https://docs.stripe.com/docs/stripe-js/reference#stripe-handle-card-payment)
        and the PaymentIntent's [client_secret](https://docs.stripe.com/api#payment_intent_object-client_secret).
        After next_actions are handled by the client, no additional
        confirmation is required to complete the payment.

        If the confirmation_method is manual, all payment attempts must be
        initiated using a secret key.

        If any actions are required for the payment, the PaymentIntent will
        return to the requires_confirmation state
        after those actions are completed. Your server needs to then
        explicitly re-confirm the PaymentIntent to initiate the next payment
        attempt.

        There is a variable upper limit on how many times a PaymentIntent can be confirmed.
        After this limit is reached, any further calls to this endpoint will
        transition the PaymentIntent to the canceled state.
        """
        ...

    @overload
    def confirm(
        self, **params: Unpack["PaymentIntentConfirmParams"]
    ) -> "PaymentIntent":
        """
        Confirm that your customer intends to pay with current or provided
        payment method. Upon confirmation, the PaymentIntent will attempt to initiate
        a payment.

        If the selected payment method requires additional authentication steps, the
        PaymentIntent will transition to the requires_action status and
        suggest additional actions via next_action. If payment fails,
        the PaymentIntent transitions to the requires_payment_method status or the
        canceled status if the confirmation limit is reached. If
        payment succeeds, the PaymentIntent will transition to the succeeded
        status (or requires_capture, if capture_method is set to manual).

        If the confirmation_method is automatic, payment may be attempted
        using our [client SDKs](https://docs.stripe.com/docs/stripe-js/reference#stripe-handle-card-payment)
        and the PaymentIntent's [client_secret](https://docs.stripe.com/api#payment_intent_object-client_secret).
        After next_actions are handled by the client, no additional
        confirmation is required to complete the payment.

        If the confirmation_method is manual, all payment attempts must be
        initiated using a secret key.

        If any actions are required for the payment, the PaymentIntent will
        return to the requires_confirmation state
        after those actions are completed. Your server needs to then
        explicitly re-confirm the PaymentIntent to initiate the next payment
        attempt.

        There is a variable upper limit on how many times a PaymentIntent can be confirmed.
        After this limit is reached, any further calls to this endpoint will
        transition the PaymentIntent to the canceled state.
        """
        ...

    @class_method_variant("_cls_confirm")
    def confirm(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["PaymentIntentConfirmParams"]
    ) -> "PaymentIntent":
        """
        Confirm that your customer intends to pay with current or provided
        payment method. Upon confirmation, the PaymentIntent will attempt to initiate
        a payment.

        If the selected payment method requires additional authentication steps, the
        PaymentIntent will transition to the requires_action status and
        suggest additional actions via next_action. If payment fails,
        the PaymentIntent transitions to the requires_payment_method status or the
        canceled status if the confirmation limit is reached. If
        payment succeeds, the PaymentIntent will transition to the succeeded
        status (or requires_capture, if capture_method is set to manual).

        If the confirmation_method is automatic, payment may be attempted
        using our [client SDKs](https://docs.stripe.com/docs/stripe-js/reference#stripe-handle-card-payment)
        and the PaymentIntent's [client_secret](https://docs.stripe.com/api#payment_intent_object-client_secret).
        After next_actions are handled by the client, no additional
        confirmation is required to complete the payment.

        If the confirmation_method is manual, all payment attempts must be
        initiated using a secret key.

        If any actions are required for the payment, the PaymentIntent will
        return to the requires_confirmation state
        after those actions are completed. Your server needs to then
        explicitly re-confirm the PaymentIntent to initiate the next payment
        attempt.

        There is a variable upper limit on how many times a PaymentIntent can be confirmed.
        After this limit is reached, any further calls to this endpoint will
        transition the PaymentIntent to the canceled state.
        """
        return cast(
            "PaymentIntent",
            self._request(
                "post",
                "/v1/payment_intents/{intent}/confirm".format(
                    intent=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_confirm_async(
        cls, intent: str, **params: Unpack["PaymentIntentConfirmParams"]
    ) -> "PaymentIntent":
        """
        Confirm that your customer intends to pay with current or provided
        payment method. Upon confirmation, the PaymentIntent will attempt to initiate
        a payment.

        If the selected payment method requires additional authentication steps, the
        PaymentIntent will transition to the requires_action status and
        suggest additional actions via next_action. If payment fails,
        the PaymentIntent transitions to the requires_payment_method status or the
        canceled status if the confirmation limit is reached. If
        payment succeeds, the PaymentIntent will transition to the succeeded
        status (or requires_capture, if capture_method is set to manual).

        If the confirmation_method is automatic, payment may be attempted
        using our [client SDKs](https://docs.stripe.com/docs/stripe-js/reference#stripe-handle-card-payment)
        and the PaymentIntent's [client_secret](https://docs.stripe.com/api#payment_intent_object-client_secret).
        After next_actions are handled by the client, no additional
        confirmation is required to complete the payment.

        If the confirmation_method is manual, all payment attempts must be
        initiated using a secret key.

        If any actions are required for the payment, the PaymentIntent will
        return to the requires_confirmation state
        after those actions are completed. Your server needs to then
        explicitly re-confirm the PaymentIntent to initiate the next payment
        attempt.

        There is a variable upper limit on how many times a PaymentIntent can be confirmed.
        After this limit is reached, any further calls to this endpoint will
        transition the PaymentIntent to the canceled state.
        """
        return cast(
            "PaymentIntent",
            await cls._static_request_async(
                "post",
                "/v1/payment_intents/{intent}/confirm".format(
                    intent=sanitize_id(intent)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def confirm_async(
        intent: str, **params: Unpack["PaymentIntentConfirmParams"]
    ) -> "PaymentIntent":
        """
        Confirm that your customer intends to pay with current or provided
        payment method. Upon confirmation, the PaymentIntent will attempt to initiate
        a payment.

        If the selected payment method requires additional authentication steps, the
        PaymentIntent will transition to the requires_action status and
        suggest additional actions via next_action. If payment fails,
        the PaymentIntent transitions to the requires_payment_method status or the
        canceled status if the confirmation limit is reached. If
        payment succeeds, the PaymentIntent will transition to the succeeded
        status (or requires_capture, if capture_method is set to manual).

        If the confirmation_method is automatic, payment may be attempted
        using our [client SDKs](https://docs.stripe.com/docs/stripe-js/reference#stripe-handle-card-payment)
        and the PaymentIntent's [client_secret](https://docs.stripe.com/api#payment_intent_object-client_secret).
        After next_actions are handled by the client, no additional
        confirmation is required to complete the payment.

        If the confirmation_method is manual, all payment attempts must be
        initiated using a secret key.

        If any actions are required for the payment, the PaymentIntent will
        return to the requires_confirmation state
        after those actions are completed. Your server needs to then
        explicitly re-confirm the PaymentIntent to initiate the next payment
        attempt.

        There is a variable upper limit on how many times a PaymentIntent can be confirmed.
        After this limit is reached, any further calls to this endpoint will
        transition the PaymentIntent to the canceled state.
        """
        ...

    @overload
    async def confirm_async(
        self, **params: Unpack["PaymentIntentConfirmParams"]
    ) -> "PaymentIntent":
        """
        Confirm that your customer intends to pay with current or provided
        payment method. Upon confirmation, the PaymentIntent will attempt to initiate
        a payment.

        If the selected payment method requires additional authentication steps, the
        PaymentIntent will transition to the requires_action status and
        suggest additional actions via next_action. If payment fails,
        the PaymentIntent transitions to the requires_payment_method status or the
        canceled status if the confirmation limit is reached. If
        payment succeeds, the PaymentIntent will transition to the succeeded
        status (or requires_capture, if capture_method is set to manual).

        If the confirmation_method is automatic, payment may be attempted
        using our [client SDKs](https://docs.stripe.com/docs/stripe-js/reference#stripe-handle-card-payment)
        and the PaymentIntent's [client_secret](https://docs.stripe.com/api#payment_intent_object-client_secret).
        After next_actions are handled by the client, no additional
        confirmation is required to complete the payment.

        If the confirmation_method is manual, all payment attempts must be
        initiated using a secret key.

        If any actions are required for the payment, the PaymentIntent will
        return to the requires_confirmation state
        after those actions are completed. Your server needs to then
        explicitly re-confirm the PaymentIntent to initiate the next payment
        attempt.

        There is a variable upper limit on how many times a PaymentIntent can be confirmed.
        After this limit is reached, any further calls to this endpoint will
        transition the PaymentIntent to the canceled state.
        """
        ...

    @class_method_variant("_cls_confirm_async")
    async def confirm_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["PaymentIntentConfirmParams"]
    ) -> "PaymentIntent":
        """
        Confirm that your customer intends to pay with current or provided
        payment method. Upon confirmation, the PaymentIntent will attempt to initiate
        a payment.

        If the selected payment method requires additional authentication steps, the
        PaymentIntent will transition to the requires_action status and
        suggest additional actions via next_action. If payment fails,
        the PaymentIntent transitions to the requires_payment_method status or the
        canceled status if the confirmation limit is reached. If
        payment succeeds, the PaymentIntent will transition to the succeeded
        status (or requires_capture, if capture_method is set to manual).

        If the confirmation_method is automatic, payment may be attempted
        using our [client SDKs](https://docs.stripe.com/docs/stripe-js/reference#stripe-handle-card-payment)
        and the PaymentIntent's [client_secret](https://docs.stripe.com/api#payment_intent_object-client_secret).
        After next_actions are handled by the client, no additional
        confirmation is required to complete the payment.

        If the confirmation_method is manual, all payment attempts must be
        initiated using a secret key.

        If any actions are required for the payment, the PaymentIntent will
        return to the requires_confirmation state
        after those actions are completed. Your server needs to then
        explicitly re-confirm the PaymentIntent to initiate the next payment
        attempt.

        There is a variable upper limit on how many times a PaymentIntent can be confirmed.
        After this limit is reached, any further calls to this endpoint will
        transition the PaymentIntent to the canceled state.
        """
        return cast(
            "PaymentIntent",
            await self._request_async(
                "post",
                "/v1/payment_intents/{intent}/confirm".format(
                    intent=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def create(
        cls, **params: Unpack["PaymentIntentCreateParams"]
    ) -> "PaymentIntent":
        """
        Creates a PaymentIntent object.

        After the PaymentIntent is created, attach a payment method and [confirm](https://docs.stripe.com/docs/api/payment_intents/confirm)
        to continue the payment. Learn more about <a href="/docs/payments/payment-intents">the available payment flows
        with the Payment Intents API.

        When you use confirm=true during creation, it's equivalent to creating
        and confirming the PaymentIntent in the same call. You can use any parameters
        available in the [confirm API](https://docs.stripe.com/docs/api/payment_intents/confirm) when you supply
        confirm=true.
        """
        return cast(
            "PaymentIntent",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["PaymentIntentCreateParams"]
    ) -> "PaymentIntent":
        """
        Creates a PaymentIntent object.

        After the PaymentIntent is created, attach a payment method and [confirm](https://docs.stripe.com/docs/api/payment_intents/confirm)
        to continue the payment. Learn more about <a href="/docs/payments/payment-intents">the available payment flows
        with the Payment Intents API.

        When you use confirm=true during creation, it's equivalent to creating
        and confirming the PaymentIntent in the same call. You can use any parameters
        available in the [confirm API](https://docs.stripe.com/docs/api/payment_intents/confirm) when you supply
        confirm=true.
        """
        return cast(
            "PaymentIntent",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def _cls_increment_authorization(
        cls,
        intent: str,
        **params: Unpack["PaymentIntentIncrementAuthorizationParams"],
    ) -> "PaymentIntent":
        """
        Perform an incremental authorization on an eligible
        [PaymentIntent](https://docs.stripe.com/docs/api/payment_intents/object). To be eligible, the
        PaymentIntent's status must be requires_capture and
        [incremental_authorization_supported](https://docs.stripe.com/docs/api/charges/object#charge_object-payment_method_details-card_present-incremental_authorization_supported)
        must be true.

        Incremental authorizations attempt to increase the authorized amount on
        your customer's card to the new, higher amount provided. Similar to the
        initial authorization, incremental authorizations can be declined. A
        single PaymentIntent can call this endpoint multiple times to further
        increase the authorized amount.

        If the incremental authorization succeeds, the PaymentIntent object
        returns with the updated
        [amount](https://docs.stripe.com/docs/api/payment_intents/object#payment_intent_object-amount).
        If the incremental authorization fails, a
        [card_declined](https://docs.stripe.com/docs/error-codes#card-declined) error returns, and no other
        fields on the PaymentIntent or Charge update. The PaymentIntent
        object remains capturable for the previously authorized amount.

        Each PaymentIntent can have a maximum of 10 incremental authorization attempts, including declines.
        After it's captured, a PaymentIntent can no longer be incremented.

        Learn more about [incremental authorizations](https://docs.stripe.com/docs/terminal/features/incremental-authorizations).
        """
        return cast(
            "PaymentIntent",
            cls._static_request(
                "post",
                "/v1/payment_intents/{intent}/increment_authorization".format(
                    intent=sanitize_id(intent)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def increment_authorization(
        intent: str,
        **params: Unpack["PaymentIntentIncrementAuthorizationParams"],
    ) -> "PaymentIntent":
        """
        Perform an incremental authorization on an eligible
        [PaymentIntent](https://docs.stripe.com/docs/api/payment_intents/object). To be eligible, the
        PaymentIntent's status must be requires_capture and
        [incremental_authorization_supported](https://docs.stripe.com/docs/api/charges/object#charge_object-payment_method_details-card_present-incremental_authorization_supported)
        must be true.

        Incremental authorizations attempt to increase the authorized amount on
        your customer's card to the new, higher amount provided. Similar to the
        initial authorization, incremental authorizations can be declined. A
        single PaymentIntent can call this endpoint multiple times to further
        increase the authorized amount.

        If the incremental authorization succeeds, the PaymentIntent object
        returns with the updated
        [amount](https://docs.stripe.com/docs/api/payment_intents/object#payment_intent_object-amount).
        If the incremental authorization fails, a
        [card_declined](https://docs.stripe.com/docs/error-codes#card-declined) error returns, and no other
        fields on the PaymentIntent or Charge update. The PaymentIntent
        object remains capturable for the previously authorized amount.

        Each PaymentIntent can have a maximum of 10 incremental authorization attempts, including declines.
        After it's captured, a PaymentIntent can no longer be incremented.

        Learn more about [incremental authorizations](https://docs.stripe.com/docs/terminal/features/incremental-authorizations).
        """
        ...

    @overload
    def increment_authorization(
        self, **params: Unpack["PaymentIntentIncrementAuthorizationParams"]
    ) -> "PaymentIntent":
        """
        Perform an incremental authorization on an eligible
        [PaymentIntent](https://docs.stripe.com/docs/api/payment_intents/object). To be eligible, the
        PaymentIntent's status must be requires_capture and
        [incremental_authorization_supported](https://docs.stripe.com/docs/api/charges/object#charge_object-payment_method_details-card_present-incremental_authorization_supported)
        must be true.

        Incremental authorizations attempt to increase the authorized amount on
        your customer's card to the new, higher amount provided. Similar to the
        initial authorization, incremental authorizations can be declined. A
        single PaymentIntent can call this endpoint multiple times to further
        increase the authorized amount.

        If the incremental authorization succeeds, the PaymentIntent object
        returns with the updated
        [amount](https://docs.stripe.com/docs/api/payment_intents/object#payment_intent_object-amount).
        If the incremental authorization fails, a
        [card_declined](https://docs.stripe.com/docs/error-codes#card-declined) error returns, and no other
        fields on the PaymentIntent or Charge update. The PaymentIntent
        object remains capturable for the previously authorized amount.

        Each PaymentIntent can have a maximum of 10 incremental authorization attempts, including declines.
        After it's captured, a PaymentIntent can no longer be incremented.

        Learn more about [incremental authorizations](https://docs.stripe.com/docs/terminal/features/incremental-authorizations).
        """
        ...

    @class_method_variant("_cls_increment_authorization")
    def increment_authorization(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["PaymentIntentIncrementAuthorizationParams"]
    ) -> "PaymentIntent":
        """
        Perform an incremental authorization on an eligible
        [PaymentIntent](https://docs.stripe.com/docs/api/payment_intents/object). To be eligible, the
        PaymentIntent's status must be requires_capture and
        [incremental_authorization_supported](https://docs.stripe.com/docs/api/charges/object#charge_object-payment_method_details-card_present-incremental_authorization_supported)
        must be true.

        Incremental authorizations attempt to increase the authorized amount on
        your customer's card to the new, higher amount provided. Similar to the
        initial authorization, incremental authorizations can be declined. A
        single PaymentIntent can call this endpoint multiple times to further
        increase the authorized amount.

        If the incremental authorization succeeds, the PaymentIntent object
        returns with the updated
        [amount](https://docs.stripe.com/docs/api/payment_intents/object#payment_intent_object-amount).
        If the incremental authorization fails, a
        [card_declined](https://docs.stripe.com/docs/error-codes#card-declined) error returns, and no other
        fields on the PaymentIntent or Charge update. The PaymentIntent
        object remains capturable for the previously authorized amount.

        Each PaymentIntent can have a maximum of 10 incremental authorization attempts, including declines.
        After it's captured, a PaymentIntent can no longer be incremented.

        Learn more about [incremental authorizations](https://docs.stripe.com/docs/terminal/features/incremental-authorizations).
        """
        return cast(
            "PaymentIntent",
            self._request(
                "post",
                "/v1/payment_intents/{intent}/increment_authorization".format(
                    intent=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_increment_authorization_async(
        cls,
        intent: str,
        **params: Unpack["PaymentIntentIncrementAuthorizationParams"],
    ) -> "PaymentIntent":
        """
        Perform an incremental authorization on an eligible
        [PaymentIntent](https://docs.stripe.com/docs/api/payment_intents/object). To be eligible, the
        PaymentIntent's status must be requires_capture and
        [incremental_authorization_supported](https://docs.stripe.com/docs/api/charges/object#charge_object-payment_method_details-card_present-incremental_authorization_supported)
        must be true.

        Incremental authorizations attempt to increase the authorized amount on
        your customer's card to the new, higher amount provided. Similar to the
        initial authorization, incremental authorizations can be declined. A
        single PaymentIntent can call this endpoint multiple times to further
        increase the authorized amount.

        If the incremental authorization succeeds, the PaymentIntent object
        returns with the updated
        [amount](https://docs.stripe.com/docs/api/payment_intents/object#payment_intent_object-amount).
        If the incremental authorization fails, a
        [card_declined](https://docs.stripe.com/docs/error-codes#card-declined) error returns, and no other
        fields on the PaymentIntent or Charge update. The PaymentIntent
        object remains capturable for the previously authorized amount.

        Each PaymentIntent can have a maximum of 10 incremental authorization attempts, including declines.
        After it's captured, a PaymentIntent can no longer be incremented.

        Learn more about [incremental authorizations](https://docs.stripe.com/docs/terminal/features/incremental-authorizations).
        """
        return cast(
            "PaymentIntent",
            await cls._static_request_async(
                "post",
                "/v1/payment_intents/{intent}/increment_authorization".format(
                    intent=sanitize_id(intent)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def increment_authorization_async(
        intent: str,
        **params: Unpack["PaymentIntentIncrementAuthorizationParams"],
    ) -> "PaymentIntent":
        """
        Perform an incremental authorization on an eligible
        [PaymentIntent](https://docs.stripe.com/docs/api/payment_intents/object). To be eligible, the
        PaymentIntent's status must be requires_capture and
        [incremental_authorization_supported](https://docs.stripe.com/docs/api/charges/object#charge_object-payment_method_details-card_present-incremental_authorization_supported)
        must be true.

        Incremental authorizations attempt to increase the authorized amount on
        your customer's card to the new, higher amount provided. Similar to the
        initial authorization, incremental authorizations can be declined. A
        single PaymentIntent can call this endpoint multiple times to further
        increase the authorized amount.

        If the incremental authorization succeeds, the PaymentIntent object
        returns with the updated
        [amount](https://docs.stripe.com/docs/api/payment_intents/object#payment_intent_object-amount).
        If the incremental authorization fails, a
        [card_declined](https://docs.stripe.com/docs/error-codes#card-declined) error returns, and no other
        fields on the PaymentIntent or Charge update. The PaymentIntent
        object remains capturable for the previously authorized amount.

        Each PaymentIntent can have a maximum of 10 incremental authorization attempts, including declines.
        After it's captured, a PaymentIntent can no longer be incremented.

        Learn more about [incremental authorizations](https://docs.stripe.com/docs/terminal/features/incremental-authorizations).
        """
        ...

    @overload
    async def increment_authorization_async(
        self, **params: Unpack["PaymentIntentIncrementAuthorizationParams"]
    ) -> "PaymentIntent":
        """
        Perform an incremental authorization on an eligible
        [PaymentIntent](https://docs.stripe.com/docs/api/payment_intents/object). To be eligible, the
        PaymentIntent's status must be requires_capture and
        [incremental_authorization_supported](https://docs.stripe.com/docs/api/charges/object#charge_object-payment_method_details-card_present-incremental_authorization_supported)
        must be true.

        Incremental authorizations attempt to increase the authorized amount on
        your customer's card to the new, higher amount provided. Similar to the
        initial authorization, incremental authorizations can be declined. A
        single PaymentIntent can call this endpoint multiple times to further
        increase the authorized amount.

        If the incremental authorization succeeds, the PaymentIntent object
        returns with the updated
        [amount](https://docs.stripe.com/docs/api/payment_intents/object#payment_intent_object-amount).
        If the incremental authorization fails, a
        [card_declined](https://docs.stripe.com/docs/error-codes#card-declined) error returns, and no other
        fields on the PaymentIntent or Charge update. The PaymentIntent
        object remains capturable for the previously authorized amount.

        Each PaymentIntent can have a maximum of 10 incremental authorization attempts, including declines.
        After it's captured, a PaymentIntent can no longer be incremented.

        Learn more about [incremental authorizations](https://docs.stripe.com/docs/terminal/features/incremental-authorizations).
        """
        ...

    @class_method_variant("_cls_increment_authorization_async")
    async def increment_authorization_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["PaymentIntentIncrementAuthorizationParams"]
    ) -> "PaymentIntent":
        """
        Perform an incremental authorization on an eligible
        [PaymentIntent](https://docs.stripe.com/docs/api/payment_intents/object). To be eligible, the
        PaymentIntent's status must be requires_capture and
        [incremental_authorization_supported](https://docs.stripe.com/docs/api/charges/object#charge_object-payment_method_details-card_present-incremental_authorization_supported)
        must be true.

        Incremental authorizations attempt to increase the authorized amount on
        your customer's card to the new, higher amount provided. Similar to the
        initial authorization, incremental authorizations can be declined. A
        single PaymentIntent can call this endpoint multiple times to further
        increase the authorized amount.

        If the incremental authorization succeeds, the PaymentIntent object
        returns with the updated
        [amount](https://docs.stripe.com/docs/api/payment_intents/object#payment_intent_object-amount).
        If the incremental authorization fails, a
        [card_declined](https://docs.stripe.com/docs/error-codes#card-declined) error returns, and no other
        fields on the PaymentIntent or Charge update. The PaymentIntent
        object remains capturable for the previously authorized amount.

        Each PaymentIntent can have a maximum of 10 incremental authorization attempts, including declines.
        After it's captured, a PaymentIntent can no longer be incremented.

        Learn more about [incremental authorizations](https://docs.stripe.com/docs/terminal/features/incremental-authorizations).
        """
        return cast(
            "PaymentIntent",
            await self._request_async(
                "post",
                "/v1/payment_intents/{intent}/increment_authorization".format(
                    intent=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def list(
        cls, **params: Unpack["PaymentIntentListParams"]
    ) -> ListObject["PaymentIntent"]:
        """
        Returns a list of PaymentIntents.
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
        cls, **params: Unpack["PaymentIntentListParams"]
    ) -> ListObject["PaymentIntent"]:
        """
        Returns a list of PaymentIntents.
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
        cls, id: str, **params: Unpack["PaymentIntentModifyParams"]
    ) -> "PaymentIntent":
        """
        Updates properties on a PaymentIntent object without confirming.

        Depending on which properties you update, you might need to confirm the
        PaymentIntent again. For example, updating the payment_method
        always requires you to confirm the PaymentIntent again. If you prefer to
        update and confirm at the same time, we recommend updating properties through
        the [confirm API](https://docs.stripe.com/docs/api/payment_intents/confirm) instead.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "PaymentIntent",
            cls._static_request(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    async def modify_async(
        cls, id: str, **params: Unpack["PaymentIntentModifyParams"]
    ) -> "PaymentIntent":
        """
        Updates properties on a PaymentIntent object without confirming.

        Depending on which properties you update, you might need to confirm the
        PaymentIntent again. For example, updating the payment_method
        always requires you to confirm the PaymentIntent again. If you prefer to
        update and confirm at the same time, we recommend updating properties through
        the [confirm API](https://docs.stripe.com/docs/api/payment_intents/confirm) instead.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "PaymentIntent",
            await cls._static_request_async(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["PaymentIntentRetrieveParams"]
    ) -> "PaymentIntent":
        """
        Retrieves the details of a PaymentIntent that has previously been created.

        You can retrieve a PaymentIntent client-side using a publishable key when the client_secret is in the query string.

        If you retrieve a PaymentIntent with a publishable key, it only returns a subset of properties. Refer to the [payment intent](https://docs.stripe.com/api#payment_intent_object) object reference for more details.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["PaymentIntentRetrieveParams"]
    ) -> "PaymentIntent":
        """
        Retrieves the details of a PaymentIntent that has previously been created.

        You can retrieve a PaymentIntent client-side using a publishable key when the client_secret is in the query string.

        If you retrieve a PaymentIntent with a publishable key, it only returns a subset of properties. Refer to the [payment intent](https://docs.stripe.com/api#payment_intent_object) object reference for more details.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    @classmethod
    def _cls_verify_microdeposits(
        cls,
        intent: str,
        **params: Unpack["PaymentIntentVerifyMicrodepositsParams"],
    ) -> "PaymentIntent":
        """
        Verifies microdeposits on a PaymentIntent object.
        """
        return cast(
            "PaymentIntent",
            cls._static_request(
                "post",
                "/v1/payment_intents/{intent}/verify_microdeposits".format(
                    intent=sanitize_id(intent)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def verify_microdeposits(
        intent: str, **params: Unpack["PaymentIntentVerifyMicrodepositsParams"]
    ) -> "PaymentIntent":
        """
        Verifies microdeposits on a PaymentIntent object.
        """
        ...

    @overload
    def verify_microdeposits(
        self, **params: Unpack["PaymentIntentVerifyMicrodepositsParams"]
    ) -> "PaymentIntent":
        """
        Verifies microdeposits on a PaymentIntent object.
        """
        ...

    @class_method_variant("_cls_verify_microdeposits")
    def verify_microdeposits(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["PaymentIntentVerifyMicrodepositsParams"]
    ) -> "PaymentIntent":
        """
        Verifies microdeposits on a PaymentIntent object.
        """
        return cast(
            "PaymentIntent",
            self._request(
                "post",
                "/v1/payment_intents/{intent}/verify_microdeposits".format(
                    intent=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_verify_microdeposits_async(
        cls,
        intent: str,
        **params: Unpack["PaymentIntentVerifyMicrodepositsParams"],
    ) -> "PaymentIntent":
        """
        Verifies microdeposits on a PaymentIntent object.
        """
        return cast(
            "PaymentIntent",
            await cls._static_request_async(
                "post",
                "/v1/payment_intents/{intent}/verify_microdeposits".format(
                    intent=sanitize_id(intent)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def verify_microdeposits_async(
        intent: str, **params: Unpack["PaymentIntentVerifyMicrodepositsParams"]
    ) -> "PaymentIntent":
        """
        Verifies microdeposits on a PaymentIntent object.
        """
        ...

    @overload
    async def verify_microdeposits_async(
        self, **params: Unpack["PaymentIntentVerifyMicrodepositsParams"]
    ) -> "PaymentIntent":
        """
        Verifies microdeposits on a PaymentIntent object.
        """
        ...

    @class_method_variant("_cls_verify_microdeposits_async")
    async def verify_microdeposits_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["PaymentIntentVerifyMicrodepositsParams"]
    ) -> "PaymentIntent":
        """
        Verifies microdeposits on a PaymentIntent object.
        """
        return cast(
            "PaymentIntent",
            await self._request_async(
                "post",
                "/v1/payment_intents/{intent}/verify_microdeposits".format(
                    intent=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def search(
        cls, *args, **kwargs: Unpack["PaymentIntentSearchParams"]
    ) -> SearchResultObject["PaymentIntent"]:
        """
        Search for PaymentIntents you've previously created using Stripe's [Search Query Language](https://docs.stripe.com/docs/search#search-query-language).
        Don't use search in read-after-write flows where strict consistency is necessary. Under normal operating
        conditions, data is searchable in less than a minute. Occasionally, propagation of new or updated data can be up
        to an hour behind during outages. Search functionality is not available to merchants in India.
        """
        return cls._search(
            search_url="/v1/payment_intents/search", *args, **kwargs
        )

    @classmethod
    async def search_async(
        cls, *args, **kwargs: Unpack["PaymentIntentSearchParams"]
    ) -> SearchResultObject["PaymentIntent"]:
        """
        Search for PaymentIntents you've previously created using Stripe's [Search Query Language](https://docs.stripe.com/docs/search#search-query-language).
        Don't use search in read-after-write flows where strict consistency is necessary. Under normal operating
        conditions, data is searchable in less than a minute. Occasionally, propagation of new or updated data can be up
        to an hour behind during outages. Search functionality is not available to merchants in India.
        """
        return await cls._search_async(
            search_url="/v1/payment_intents/search", *args, **kwargs
        )

    @classmethod
    def search_auto_paging_iter(
        cls, *args, **kwargs: Unpack["PaymentIntentSearchParams"]
    ) -> Iterator["PaymentIntent"]:
        return cls.search(*args, **kwargs).auto_paging_iter()

    @classmethod
    async def search_auto_paging_iter_async(
        cls, *args, **kwargs: Unpack["PaymentIntentSearchParams"]
    ) -> AsyncIterator["PaymentIntent"]:
        return (await cls.search_async(*args, **kwargs)).auto_paging_iter()

    @classmethod
    def list_amount_details_line_items(
        cls,
        intent: str,
        **params: Unpack["PaymentIntentListAmountDetailsLineItemsParams"],
    ) -> ListObject["PaymentIntentAmountDetailsLineItem"]:
        """
        Lists all LineItems of a given PaymentIntent.
        """
        return cast(
            ListObject["PaymentIntentAmountDetailsLineItem"],
            cls._static_request(
                "get",
                "/v1/payment_intents/{intent}/amount_details_line_items".format(
                    intent=sanitize_id(intent)
                ),
                params=params,
            ),
        )

    @classmethod
    async def list_amount_details_line_items_async(
        cls,
        intent: str,
        **params: Unpack["PaymentIntentListAmountDetailsLineItemsParams"],
    ) -> ListObject["PaymentIntentAmountDetailsLineItem"]:
        """
        Lists all LineItems of a given PaymentIntent.
        """
        return cast(
            ListObject["PaymentIntentAmountDetailsLineItem"],
            await cls._static_request_async(
                "get",
                "/v1/payment_intents/{intent}/amount_details_line_items".format(
                    intent=sanitize_id(intent)
                ),
                params=params,
            ),
        )

    _inner_class_types = {
        "amount_details": AmountDetails,
        "automatic_payment_methods": AutomaticPaymentMethods,
        "hooks": Hooks,
        "last_payment_error": LastPaymentError,
        "next_action": NextAction,
        "payment_details": PaymentDetails,
        "payment_method_configuration_details": PaymentMethodConfigurationDetails,
        "payment_method_options": PaymentMethodOptions,
        "presentment_details": PresentmentDetails,
        "processing": Processing,
        "shipping": Shipping,
        "transfer_data": TransferData,
    }
