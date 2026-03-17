from typing_extensions import TYPE_CHECKING, Literal
from typing import Optional
import os
import warnings

# Stripe Python bindings
# API docs at http://stripe.com/docs/api
# Authors:
# Patrick Collison <patrick@stripe.com>
# Greg Brockman <gdb@stripe.com>
# Andrew Metcalf <andrew@stripe.com>

# Configuration variables
from stripe._api_version import _ApiVersion

from stripe._app_info import AppInfo as AppInfo
from stripe._version import VERSION as VERSION

# Constants
DEFAULT_API_BASE: str = "https://api.stripe.com"
DEFAULT_CONNECT_API_BASE: str = "https://connect.stripe.com"
DEFAULT_UPLOAD_API_BASE: str = "https://files.stripe.com"
DEFAULT_METER_EVENTS_API_BASE: str = "https://meter-events.stripe.com"


api_key: Optional[str] = None
client_id: Optional[str] = None
api_base: str = DEFAULT_API_BASE
connect_api_base: str = DEFAULT_CONNECT_API_BASE
upload_api_base: str = DEFAULT_UPLOAD_API_BASE
meter_events_api_base: str = DEFAULT_METER_EVENTS_API_BASE
api_version: str = _ApiVersion.CURRENT
verify_ssl_certs: bool = True
proxy: Optional[str] = None
default_http_client: Optional["HTTPClient"] = None
app_info: Optional[AppInfo] = None
enable_telemetry: bool = True
max_network_retries: int = 2
ca_bundle_path: str = os.path.join(
    os.path.dirname(__file__), "data", "ca-certificates.crt"
)

# Lazily initialized stripe.default_http_client
default_http_client = None
_default_proxy = None

from stripe._http_client import (
    new_default_http_client as new_default_http_client,
)


def ensure_default_http_client():
    if default_http_client:
        _warn_if_mismatched_proxy()
        return
    _init_default_http_client()


def _init_default_http_client():
    global _default_proxy
    global default_http_client

    # If the stripe.default_http_client has not been set by the user
    # yet, we'll set it here. This way, we aren't creating a new
    # HttpClient for every request.
    default_http_client = new_default_http_client(
        verify_ssl_certs=verify_ssl_certs, proxy=proxy
    )
    _default_proxy = proxy


def _warn_if_mismatched_proxy():
    global _default_proxy
    from stripe import proxy

    if proxy != _default_proxy:
        warnings.warn(
            "stripe.proxy was updated after sending a "
            "request - this is a no-op. To use a different proxy, "
            "set stripe.default_http_client to a new client "
            "configured with the proxy."
        )


# Set to either 'debug' or 'info', controls console logging
log: Optional[Literal["debug", "info"]] = None


# Sets some basic information about the running application that's sent along
# with API requests. Useful for plugin authors to identify their plugin when
# communicating with Stripe.
#
# Takes a name and optional version and plugin URL.
def set_app_info(
    name: str,
    partner_id: Optional[str] = None,
    url: Optional[str] = None,
    version: Optional[str] = None,
):
    global app_info
    app_info = {
        "name": name,
        "partner_id": partner_id,
        "url": url,
        "version": version,
    }


# The beginning of the section generated from our OpenAPI spec
from importlib import import_module

if TYPE_CHECKING:
    from stripe import (
        _error as error,
        apps as apps,
        billing as billing,
        billing_portal as billing_portal,
        checkout as checkout,
        climate as climate,
        entitlements as entitlements,
        events as events,
        financial_connections as financial_connections,
        forwarding as forwarding,
        identity as identity,
        issuing as issuing,
        params as params,
        radar as radar,
        reporting as reporting,
        reserve as reserve,
        sigma as sigma,
        tax as tax,
        terminal as terminal,
        test_helpers as test_helpers,
        treasury as treasury,
        v2 as v2,
    )
    from stripe._account import Account as Account
    from stripe._account_capability_service import (
        AccountCapabilityService as AccountCapabilityService,
    )
    from stripe._account_external_account_service import (
        AccountExternalAccountService as AccountExternalAccountService,
    )
    from stripe._account_link import AccountLink as AccountLink
    from stripe._account_link_service import (
        AccountLinkService as AccountLinkService,
    )
    from stripe._account_login_link_service import (
        AccountLoginLinkService as AccountLoginLinkService,
    )
    from stripe._account_person_service import (
        AccountPersonService as AccountPersonService,
    )
    from stripe._account_service import AccountService as AccountService
    from stripe._account_session import AccountSession as AccountSession
    from stripe._account_session_service import (
        AccountSessionService as AccountSessionService,
    )
    from stripe._api_mode import ApiMode as ApiMode
    from stripe._api_resource import APIResource as APIResource
    from stripe._apple_pay_domain import ApplePayDomain as ApplePayDomain
    from stripe._apple_pay_domain_service import (
        ApplePayDomainService as ApplePayDomainService,
    )
    from stripe._application import Application as Application
    from stripe._application_fee import ApplicationFee as ApplicationFee
    from stripe._application_fee_refund import (
        ApplicationFeeRefund as ApplicationFeeRefund,
    )
    from stripe._application_fee_refund_service import (
        ApplicationFeeRefundService as ApplicationFeeRefundService,
    )
    from stripe._application_fee_service import (
        ApplicationFeeService as ApplicationFeeService,
    )
    from stripe._apps_service import AppsService as AppsService
    from stripe._balance import Balance as Balance
    from stripe._balance_service import BalanceService as BalanceService
    from stripe._balance_settings import BalanceSettings as BalanceSettings
    from stripe._balance_settings_service import (
        BalanceSettingsService as BalanceSettingsService,
    )
    from stripe._balance_transaction import (
        BalanceTransaction as BalanceTransaction,
    )
    from stripe._balance_transaction_service import (
        BalanceTransactionService as BalanceTransactionService,
    )
    from stripe._bank_account import BankAccount as BankAccount
    from stripe._base_address import BaseAddress as BaseAddress
    from stripe._billing_portal_service import (
        BillingPortalService as BillingPortalService,
    )
    from stripe._billing_service import BillingService as BillingService
    from stripe._capability import Capability as Capability
    from stripe._card import Card as Card
    from stripe._cash_balance import CashBalance as CashBalance
    from stripe._charge import Charge as Charge
    from stripe._charge_service import ChargeService as ChargeService
    from stripe._checkout_service import CheckoutService as CheckoutService
    from stripe._climate_service import ClimateService as ClimateService
    from stripe._confirmation_token import (
        ConfirmationToken as ConfirmationToken,
    )
    from stripe._confirmation_token_service import (
        ConfirmationTokenService as ConfirmationTokenService,
    )
    from stripe._connect_collection_transfer import (
        ConnectCollectionTransfer as ConnectCollectionTransfer,
    )
    from stripe._country_spec import CountrySpec as CountrySpec
    from stripe._country_spec_service import (
        CountrySpecService as CountrySpecService,
    )
    from stripe._coupon import Coupon as Coupon
    from stripe._coupon_service import CouponService as CouponService
    from stripe._createable_api_resource import (
        CreateableAPIResource as CreateableAPIResource,
    )
    from stripe._credit_note import CreditNote as CreditNote
    from stripe._credit_note_line_item import (
        CreditNoteLineItem as CreditNoteLineItem,
    )
    from stripe._credit_note_line_item_service import (
        CreditNoteLineItemService as CreditNoteLineItemService,
    )
    from stripe._credit_note_preview_lines_service import (
        CreditNotePreviewLinesService as CreditNotePreviewLinesService,
    )
    from stripe._credit_note_service import (
        CreditNoteService as CreditNoteService,
    )
    from stripe._custom_method import custom_method as custom_method
    from stripe._customer import Customer as Customer
    from stripe._customer_balance_transaction import (
        CustomerBalanceTransaction as CustomerBalanceTransaction,
    )
    from stripe._customer_balance_transaction_service import (
        CustomerBalanceTransactionService as CustomerBalanceTransactionService,
    )
    from stripe._customer_cash_balance_service import (
        CustomerCashBalanceService as CustomerCashBalanceService,
    )
    from stripe._customer_cash_balance_transaction import (
        CustomerCashBalanceTransaction as CustomerCashBalanceTransaction,
    )
    from stripe._customer_cash_balance_transaction_service import (
        CustomerCashBalanceTransactionService as CustomerCashBalanceTransactionService,
    )
    from stripe._customer_funding_instructions_service import (
        CustomerFundingInstructionsService as CustomerFundingInstructionsService,
    )
    from stripe._customer_payment_method_service import (
        CustomerPaymentMethodService as CustomerPaymentMethodService,
    )
    from stripe._customer_payment_source_service import (
        CustomerPaymentSourceService as CustomerPaymentSourceService,
    )
    from stripe._customer_service import CustomerService as CustomerService
    from stripe._customer_session import CustomerSession as CustomerSession
    from stripe._customer_session_service import (
        CustomerSessionService as CustomerSessionService,
    )
    from stripe._customer_tax_id_service import (
        CustomerTaxIdService as CustomerTaxIdService,
    )
    from stripe._deletable_api_resource import (
        DeletableAPIResource as DeletableAPIResource,
    )
    from stripe._discount import Discount as Discount
    from stripe._dispute import Dispute as Dispute
    from stripe._dispute_service import DisputeService as DisputeService
    from stripe._entitlements_service import (
        EntitlementsService as EntitlementsService,
    )
    from stripe._ephemeral_key import EphemeralKey as EphemeralKey
    from stripe._ephemeral_key_service import (
        EphemeralKeyService as EphemeralKeyService,
    )
    from stripe._error import (
        APIConnectionError as APIConnectionError,
        APIError as APIError,
        AuthenticationError as AuthenticationError,
        CardError as CardError,
        IdempotencyError as IdempotencyError,
        InvalidRequestError as InvalidRequestError,
        PermissionError as PermissionError,
        RateLimitError as RateLimitError,
        SignatureVerificationError as SignatureVerificationError,
        StripeError as StripeError,
        StripeErrorWithParamCode as StripeErrorWithParamCode,
        TemporarySessionExpiredError as TemporarySessionExpiredError,
    )
    from stripe._error_object import (
        ErrorObject as ErrorObject,
        OAuthErrorObject as OAuthErrorObject,
    )
    from stripe._event import Event as Event
    from stripe._event_service import EventService as EventService
    from stripe._exchange_rate import ExchangeRate as ExchangeRate
    from stripe._exchange_rate_service import (
        ExchangeRateService as ExchangeRateService,
    )
    from stripe._file import File as File
    from stripe._file_link import FileLink as FileLink
    from stripe._file_link_service import FileLinkService as FileLinkService
    from stripe._file_service import FileService as FileService
    from stripe._financial_connections_service import (
        FinancialConnectionsService as FinancialConnectionsService,
    )
    from stripe._forwarding_service import (
        ForwardingService as ForwardingService,
    )
    from stripe._funding_instructions import (
        FundingInstructions as FundingInstructions,
    )
    from stripe._http_client import (
        AIOHTTPClient as AIOHTTPClient,
        HTTPClient as HTTPClient,
        HTTPXClient as HTTPXClient,
        PycurlClient as PycurlClient,
        RequestsClient as RequestsClient,
        UrlFetchClient as UrlFetchClient,
        UrllibClient as UrllibClient,
    )
    from stripe._identity_service import IdentityService as IdentityService
    from stripe._invoice import Invoice as Invoice
    from stripe._invoice_item import InvoiceItem as InvoiceItem
    from stripe._invoice_item_service import (
        InvoiceItemService as InvoiceItemService,
    )
    from stripe._invoice_line_item import InvoiceLineItem as InvoiceLineItem
    from stripe._invoice_line_item_service import (
        InvoiceLineItemService as InvoiceLineItemService,
    )
    from stripe._invoice_payment import InvoicePayment as InvoicePayment
    from stripe._invoice_payment_service import (
        InvoicePaymentService as InvoicePaymentService,
    )
    from stripe._invoice_rendering_template import (
        InvoiceRenderingTemplate as InvoiceRenderingTemplate,
    )
    from stripe._invoice_rendering_template_service import (
        InvoiceRenderingTemplateService as InvoiceRenderingTemplateService,
    )
    from stripe._invoice_service import InvoiceService as InvoiceService
    from stripe._issuing_service import IssuingService as IssuingService
    from stripe._line_item import LineItem as LineItem
    from stripe._list_object import ListObject as ListObject
    from stripe._listable_api_resource import (
        ListableAPIResource as ListableAPIResource,
    )
    from stripe._login_link import LoginLink as LoginLink
    from stripe._mandate import Mandate as Mandate
    from stripe._mandate_service import MandateService as MandateService
    from stripe._nested_resource_class_methods import (
        nested_resource_class_methods as nested_resource_class_methods,
    )
    from stripe._oauth import OAuth as OAuth
    from stripe._oauth_service import OAuthService as OAuthService
    from stripe._payment_attempt_record import (
        PaymentAttemptRecord as PaymentAttemptRecord,
    )
    from stripe._payment_attempt_record_service import (
        PaymentAttemptRecordService as PaymentAttemptRecordService,
    )
    from stripe._payment_intent import PaymentIntent as PaymentIntent
    from stripe._payment_intent_amount_details_line_item import (
        PaymentIntentAmountDetailsLineItem as PaymentIntentAmountDetailsLineItem,
    )
    from stripe._payment_intent_amount_details_line_item_service import (
        PaymentIntentAmountDetailsLineItemService as PaymentIntentAmountDetailsLineItemService,
    )
    from stripe._payment_intent_service import (
        PaymentIntentService as PaymentIntentService,
    )
    from stripe._payment_link import PaymentLink as PaymentLink
    from stripe._payment_link_line_item_service import (
        PaymentLinkLineItemService as PaymentLinkLineItemService,
    )
    from stripe._payment_link_service import (
        PaymentLinkService as PaymentLinkService,
    )
    from stripe._payment_method import PaymentMethod as PaymentMethod
    from stripe._payment_method_configuration import (
        PaymentMethodConfiguration as PaymentMethodConfiguration,
    )
    from stripe._payment_method_configuration_service import (
        PaymentMethodConfigurationService as PaymentMethodConfigurationService,
    )
    from stripe._payment_method_domain import (
        PaymentMethodDomain as PaymentMethodDomain,
    )
    from stripe._payment_method_domain_service import (
        PaymentMethodDomainService as PaymentMethodDomainService,
    )
    from stripe._payment_method_service import (
        PaymentMethodService as PaymentMethodService,
    )
    from stripe._payment_record import PaymentRecord as PaymentRecord
    from stripe._payment_record_service import (
        PaymentRecordService as PaymentRecordService,
    )
    from stripe._payout import Payout as Payout
    from stripe._payout_service import PayoutService as PayoutService
    from stripe._person import Person as Person
    from stripe._plan import Plan as Plan
    from stripe._plan_service import PlanService as PlanService
    from stripe._price import Price as Price
    from stripe._price_service import PriceService as PriceService
    from stripe._product import Product as Product
    from stripe._product_feature import ProductFeature as ProductFeature
    from stripe._product_feature_service import (
        ProductFeatureService as ProductFeatureService,
    )
    from stripe._product_service import ProductService as ProductService
    from stripe._promotion_code import PromotionCode as PromotionCode
    from stripe._promotion_code_service import (
        PromotionCodeService as PromotionCodeService,
    )
    from stripe._quote import Quote as Quote
    from stripe._quote_computed_upfront_line_items_service import (
        QuoteComputedUpfrontLineItemsService as QuoteComputedUpfrontLineItemsService,
    )
    from stripe._quote_line_item_service import (
        QuoteLineItemService as QuoteLineItemService,
    )
    from stripe._quote_service import QuoteService as QuoteService
    from stripe._radar_service import RadarService as RadarService
    from stripe._refund import Refund as Refund
    from stripe._refund_service import RefundService as RefundService
    from stripe._reporting_service import ReportingService as ReportingService
    from stripe._request_options import RequestOptions as RequestOptions
    from stripe._requestor_options import RequestorOptions as RequestorOptions
    from stripe._reserve_transaction import (
        ReserveTransaction as ReserveTransaction,
    )
    from stripe._reversal import Reversal as Reversal
    from stripe._review import Review as Review
    from stripe._review_service import ReviewService as ReviewService
    from stripe._search_result_object import (
        SearchResultObject as SearchResultObject,
    )
    from stripe._searchable_api_resource import (
        SearchableAPIResource as SearchableAPIResource,
    )
    from stripe._setup_attempt import SetupAttempt as SetupAttempt
    from stripe._setup_attempt_service import (
        SetupAttemptService as SetupAttemptService,
    )
    from stripe._setup_intent import SetupIntent as SetupIntent
    from stripe._setup_intent_service import (
        SetupIntentService as SetupIntentService,
    )
    from stripe._shipping_rate import ShippingRate as ShippingRate
    from stripe._shipping_rate_service import (
        ShippingRateService as ShippingRateService,
    )
    from stripe._sigma_service import SigmaService as SigmaService
    from stripe._singleton_api_resource import (
        SingletonAPIResource as SingletonAPIResource,
    )
    from stripe._source import Source as Source
    from stripe._source_mandate_notification import (
        SourceMandateNotification as SourceMandateNotification,
    )
    from stripe._source_service import SourceService as SourceService
    from stripe._source_transaction import (
        SourceTransaction as SourceTransaction,
    )
    from stripe._source_transaction_service import (
        SourceTransactionService as SourceTransactionService,
    )
    from stripe._stripe_client import StripeClient as StripeClient
    from stripe._stripe_context import StripeContext as StripeContext
    from stripe._stripe_object import StripeObject as StripeObject
    from stripe._stripe_response import (
        StripeResponse as StripeResponse,
        StripeResponseBase as StripeResponseBase,
        StripeStreamResponse as StripeStreamResponse,
        StripeStreamResponseAsync as StripeStreamResponseAsync,
    )
    from stripe._subscription import Subscription as Subscription
    from stripe._subscription_item import SubscriptionItem as SubscriptionItem
    from stripe._subscription_item_service import (
        SubscriptionItemService as SubscriptionItemService,
    )
    from stripe._subscription_schedule import (
        SubscriptionSchedule as SubscriptionSchedule,
    )
    from stripe._subscription_schedule_service import (
        SubscriptionScheduleService as SubscriptionScheduleService,
    )
    from stripe._subscription_service import (
        SubscriptionService as SubscriptionService,
    )
    from stripe._tax_code import TaxCode as TaxCode
    from stripe._tax_code_service import TaxCodeService as TaxCodeService
    from stripe._tax_deducted_at_source import (
        TaxDeductedAtSource as TaxDeductedAtSource,
    )
    from stripe._tax_id import TaxId as TaxId
    from stripe._tax_id_service import TaxIdService as TaxIdService
    from stripe._tax_rate import TaxRate as TaxRate
    from stripe._tax_rate_service import TaxRateService as TaxRateService
    from stripe._tax_service import TaxService as TaxService
    from stripe._terminal_service import TerminalService as TerminalService
    from stripe._test_helpers import (
        APIResourceTestHelpers as APIResourceTestHelpers,
    )
    from stripe._test_helpers_service import (
        TestHelpersService as TestHelpersService,
    )
    from stripe._token import Token as Token
    from stripe._token_service import TokenService as TokenService
    from stripe._topup import Topup as Topup
    from stripe._topup_service import TopupService as TopupService
    from stripe._transfer import Transfer as Transfer
    from stripe._transfer_reversal_service import (
        TransferReversalService as TransferReversalService,
    )
    from stripe._transfer_service import TransferService as TransferService
    from stripe._treasury_service import TreasuryService as TreasuryService
    from stripe._updateable_api_resource import (
        UpdateableAPIResource as UpdateableAPIResource,
    )
    from stripe._util import (
        convert_to_stripe_object as convert_to_stripe_object,
    )
    from stripe._v1_services import V1Services as V1Services
    from stripe._v2_services import V2Services as V2Services
    from stripe._verify_mixin import VerifyMixin as VerifyMixin
    from stripe._webhook import (
        Webhook as Webhook,
        WebhookSignature as WebhookSignature,
    )
    from stripe._webhook_endpoint import WebhookEndpoint as WebhookEndpoint
    from stripe._webhook_endpoint_service import (
        WebhookEndpointService as WebhookEndpointService,
    )

# name -> (import_target, is_submodule)
_import_map = {
    "error": ("stripe._error", True),
    "apps": ("stripe.apps", True),
    "billing": ("stripe.billing", True),
    "billing_portal": ("stripe.billing_portal", True),
    "checkout": ("stripe.checkout", True),
    "climate": ("stripe.climate", True),
    "entitlements": ("stripe.entitlements", True),
    "events": ("stripe.events", True),
    "financial_connections": ("stripe.financial_connections", True),
    "forwarding": ("stripe.forwarding", True),
    "identity": ("stripe.identity", True),
    "issuing": ("stripe.issuing", True),
    "params": ("stripe.params", True),
    "radar": ("stripe.radar", True),
    "reporting": ("stripe.reporting", True),
    "reserve": ("stripe.reserve", True),
    "sigma": ("stripe.sigma", True),
    "tax": ("stripe.tax", True),
    "terminal": ("stripe.terminal", True),
    "test_helpers": ("stripe.test_helpers", True),
    "treasury": ("stripe.treasury", True),
    "v2": ("stripe.v2", True),
    "Account": ("stripe._account", False),
    "AccountCapabilityService": ("stripe._account_capability_service", False),
    "AccountExternalAccountService": (
        "stripe._account_external_account_service",
        False,
    ),
    "AccountLink": ("stripe._account_link", False),
    "AccountLinkService": ("stripe._account_link_service", False),
    "AccountLoginLinkService": ("stripe._account_login_link_service", False),
    "AccountPersonService": ("stripe._account_person_service", False),
    "AccountService": ("stripe._account_service", False),
    "AccountSession": ("stripe._account_session", False),
    "AccountSessionService": ("stripe._account_session_service", False),
    "ApiMode": ("stripe._api_mode", False),
    "APIResource": ("stripe._api_resource", False),
    "ApplePayDomain": ("stripe._apple_pay_domain", False),
    "ApplePayDomainService": ("stripe._apple_pay_domain_service", False),
    "Application": ("stripe._application", False),
    "ApplicationFee": ("stripe._application_fee", False),
    "ApplicationFeeRefund": ("stripe._application_fee_refund", False),
    "ApplicationFeeRefundService": (
        "stripe._application_fee_refund_service",
        False,
    ),
    "ApplicationFeeService": ("stripe._application_fee_service", False),
    "AppsService": ("stripe._apps_service", False),
    "Balance": ("stripe._balance", False),
    "BalanceService": ("stripe._balance_service", False),
    "BalanceSettings": ("stripe._balance_settings", False),
    "BalanceSettingsService": ("stripe._balance_settings_service", False),
    "BalanceTransaction": ("stripe._balance_transaction", False),
    "BalanceTransactionService": (
        "stripe._balance_transaction_service",
        False,
    ),
    "BankAccount": ("stripe._bank_account", False),
    "BaseAddress": ("stripe._base_address", False),
    "BillingPortalService": ("stripe._billing_portal_service", False),
    "BillingService": ("stripe._billing_service", False),
    "Capability": ("stripe._capability", False),
    "Card": ("stripe._card", False),
    "CashBalance": ("stripe._cash_balance", False),
    "Charge": ("stripe._charge", False),
    "ChargeService": ("stripe._charge_service", False),
    "CheckoutService": ("stripe._checkout_service", False),
    "ClimateService": ("stripe._climate_service", False),
    "ConfirmationToken": ("stripe._confirmation_token", False),
    "ConfirmationTokenService": ("stripe._confirmation_token_service", False),
    "ConnectCollectionTransfer": (
        "stripe._connect_collection_transfer",
        False,
    ),
    "CountrySpec": ("stripe._country_spec", False),
    "CountrySpecService": ("stripe._country_spec_service", False),
    "Coupon": ("stripe._coupon", False),
    "CouponService": ("stripe._coupon_service", False),
    "CreateableAPIResource": ("stripe._createable_api_resource", False),
    "CreditNote": ("stripe._credit_note", False),
    "CreditNoteLineItem": ("stripe._credit_note_line_item", False),
    "CreditNoteLineItemService": (
        "stripe._credit_note_line_item_service",
        False,
    ),
    "CreditNotePreviewLinesService": (
        "stripe._credit_note_preview_lines_service",
        False,
    ),
    "CreditNoteService": ("stripe._credit_note_service", False),
    "custom_method": ("stripe._custom_method", False),
    "Customer": ("stripe._customer", False),
    "CustomerBalanceTransaction": (
        "stripe._customer_balance_transaction",
        False,
    ),
    "CustomerBalanceTransactionService": (
        "stripe._customer_balance_transaction_service",
        False,
    ),
    "CustomerCashBalanceService": (
        "stripe._customer_cash_balance_service",
        False,
    ),
    "CustomerCashBalanceTransaction": (
        "stripe._customer_cash_balance_transaction",
        False,
    ),
    "CustomerCashBalanceTransactionService": (
        "stripe._customer_cash_balance_transaction_service",
        False,
    ),
    "CustomerFundingInstructionsService": (
        "stripe._customer_funding_instructions_service",
        False,
    ),
    "CustomerPaymentMethodService": (
        "stripe._customer_payment_method_service",
        False,
    ),
    "CustomerPaymentSourceService": (
        "stripe._customer_payment_source_service",
        False,
    ),
    "CustomerService": ("stripe._customer_service", False),
    "CustomerSession": ("stripe._customer_session", False),
    "CustomerSessionService": ("stripe._customer_session_service", False),
    "CustomerTaxIdService": ("stripe._customer_tax_id_service", False),
    "DeletableAPIResource": ("stripe._deletable_api_resource", False),
    "Discount": ("stripe._discount", False),
    "Dispute": ("stripe._dispute", False),
    "DisputeService": ("stripe._dispute_service", False),
    "EntitlementsService": ("stripe._entitlements_service", False),
    "EphemeralKey": ("stripe._ephemeral_key", False),
    "EphemeralKeyService": ("stripe._ephemeral_key_service", False),
    "APIConnectionError": ("stripe._error", False),
    "APIError": ("stripe._error", False),
    "AuthenticationError": ("stripe._error", False),
    "CardError": ("stripe._error", False),
    "IdempotencyError": ("stripe._error", False),
    "InvalidRequestError": ("stripe._error", False),
    "PermissionError": ("stripe._error", False),
    "RateLimitError": ("stripe._error", False),
    "SignatureVerificationError": ("stripe._error", False),
    "StripeError": ("stripe._error", False),
    "StripeErrorWithParamCode": ("stripe._error", False),
    "TemporarySessionExpiredError": ("stripe._error", False),
    "ErrorObject": ("stripe._error_object", False),
    "OAuthErrorObject": ("stripe._error_object", False),
    "Event": ("stripe._event", False),
    "EventService": ("stripe._event_service", False),
    "ExchangeRate": ("stripe._exchange_rate", False),
    "ExchangeRateService": ("stripe._exchange_rate_service", False),
    "File": ("stripe._file", False),
    "FileLink": ("stripe._file_link", False),
    "FileLinkService": ("stripe._file_link_service", False),
    "FileService": ("stripe._file_service", False),
    "FinancialConnectionsService": (
        "stripe._financial_connections_service",
        False,
    ),
    "ForwardingService": ("stripe._forwarding_service", False),
    "FundingInstructions": ("stripe._funding_instructions", False),
    "AIOHTTPClient": ("stripe._http_client", False),
    "HTTPClient": ("stripe._http_client", False),
    "HTTPXClient": ("stripe._http_client", False),
    "PycurlClient": ("stripe._http_client", False),
    "RequestsClient": ("stripe._http_client", False),
    "UrlFetchClient": ("stripe._http_client", False),
    "UrllibClient": ("stripe._http_client", False),
    "IdentityService": ("stripe._identity_service", False),
    "Invoice": ("stripe._invoice", False),
    "InvoiceItem": ("stripe._invoice_item", False),
    "InvoiceItemService": ("stripe._invoice_item_service", False),
    "InvoiceLineItem": ("stripe._invoice_line_item", False),
    "InvoiceLineItemService": ("stripe._invoice_line_item_service", False),
    "InvoicePayment": ("stripe._invoice_payment", False),
    "InvoicePaymentService": ("stripe._invoice_payment_service", False),
    "InvoiceRenderingTemplate": ("stripe._invoice_rendering_template", False),
    "InvoiceRenderingTemplateService": (
        "stripe._invoice_rendering_template_service",
        False,
    ),
    "InvoiceService": ("stripe._invoice_service", False),
    "IssuingService": ("stripe._issuing_service", False),
    "LineItem": ("stripe._line_item", False),
    "ListObject": ("stripe._list_object", False),
    "ListableAPIResource": ("stripe._listable_api_resource", False),
    "LoginLink": ("stripe._login_link", False),
    "Mandate": ("stripe._mandate", False),
    "MandateService": ("stripe._mandate_service", False),
    "nested_resource_class_methods": (
        "stripe._nested_resource_class_methods",
        False,
    ),
    "OAuth": ("stripe._oauth", False),
    "OAuthService": ("stripe._oauth_service", False),
    "PaymentAttemptRecord": ("stripe._payment_attempt_record", False),
    "PaymentAttemptRecordService": (
        "stripe._payment_attempt_record_service",
        False,
    ),
    "PaymentIntent": ("stripe._payment_intent", False),
    "PaymentIntentAmountDetailsLineItem": (
        "stripe._payment_intent_amount_details_line_item",
        False,
    ),
    "PaymentIntentAmountDetailsLineItemService": (
        "stripe._payment_intent_amount_details_line_item_service",
        False,
    ),
    "PaymentIntentService": ("stripe._payment_intent_service", False),
    "PaymentLink": ("stripe._payment_link", False),
    "PaymentLinkLineItemService": (
        "stripe._payment_link_line_item_service",
        False,
    ),
    "PaymentLinkService": ("stripe._payment_link_service", False),
    "PaymentMethod": ("stripe._payment_method", False),
    "PaymentMethodConfiguration": (
        "stripe._payment_method_configuration",
        False,
    ),
    "PaymentMethodConfigurationService": (
        "stripe._payment_method_configuration_service",
        False,
    ),
    "PaymentMethodDomain": ("stripe._payment_method_domain", False),
    "PaymentMethodDomainService": (
        "stripe._payment_method_domain_service",
        False,
    ),
    "PaymentMethodService": ("stripe._payment_method_service", False),
    "PaymentRecord": ("stripe._payment_record", False),
    "PaymentRecordService": ("stripe._payment_record_service", False),
    "Payout": ("stripe._payout", False),
    "PayoutService": ("stripe._payout_service", False),
    "Person": ("stripe._person", False),
    "Plan": ("stripe._plan", False),
    "PlanService": ("stripe._plan_service", False),
    "Price": ("stripe._price", False),
    "PriceService": ("stripe._price_service", False),
    "Product": ("stripe._product", False),
    "ProductFeature": ("stripe._product_feature", False),
    "ProductFeatureService": ("stripe._product_feature_service", False),
    "ProductService": ("stripe._product_service", False),
    "PromotionCode": ("stripe._promotion_code", False),
    "PromotionCodeService": ("stripe._promotion_code_service", False),
    "Quote": ("stripe._quote", False),
    "QuoteComputedUpfrontLineItemsService": (
        "stripe._quote_computed_upfront_line_items_service",
        False,
    ),
    "QuoteLineItemService": ("stripe._quote_line_item_service", False),
    "QuoteService": ("stripe._quote_service", False),
    "RadarService": ("stripe._radar_service", False),
    "Refund": ("stripe._refund", False),
    "RefundService": ("stripe._refund_service", False),
    "ReportingService": ("stripe._reporting_service", False),
    "RequestOptions": ("stripe._request_options", False),
    "RequestorOptions": ("stripe._requestor_options", False),
    "ReserveTransaction": ("stripe._reserve_transaction", False),
    "Reversal": ("stripe._reversal", False),
    "Review": ("stripe._review", False),
    "ReviewService": ("stripe._review_service", False),
    "SearchResultObject": ("stripe._search_result_object", False),
    "SearchableAPIResource": ("stripe._searchable_api_resource", False),
    "SetupAttempt": ("stripe._setup_attempt", False),
    "SetupAttemptService": ("stripe._setup_attempt_service", False),
    "SetupIntent": ("stripe._setup_intent", False),
    "SetupIntentService": ("stripe._setup_intent_service", False),
    "ShippingRate": ("stripe._shipping_rate", False),
    "ShippingRateService": ("stripe._shipping_rate_service", False),
    "SigmaService": ("stripe._sigma_service", False),
    "SingletonAPIResource": ("stripe._singleton_api_resource", False),
    "Source": ("stripe._source", False),
    "SourceMandateNotification": (
        "stripe._source_mandate_notification",
        False,
    ),
    "SourceService": ("stripe._source_service", False),
    "SourceTransaction": ("stripe._source_transaction", False),
    "SourceTransactionService": ("stripe._source_transaction_service", False),
    "StripeClient": ("stripe._stripe_client", False),
    "StripeContext": ("stripe._stripe_context", False),
    "StripeObject": ("stripe._stripe_object", False),
    "StripeResponse": ("stripe._stripe_response", False),
    "StripeResponseBase": ("stripe._stripe_response", False),
    "StripeStreamResponse": ("stripe._stripe_response", False),
    "StripeStreamResponseAsync": ("stripe._stripe_response", False),
    "Subscription": ("stripe._subscription", False),
    "SubscriptionItem": ("stripe._subscription_item", False),
    "SubscriptionItemService": ("stripe._subscription_item_service", False),
    "SubscriptionSchedule": ("stripe._subscription_schedule", False),
    "SubscriptionScheduleService": (
        "stripe._subscription_schedule_service",
        False,
    ),
    "SubscriptionService": ("stripe._subscription_service", False),
    "TaxCode": ("stripe._tax_code", False),
    "TaxCodeService": ("stripe._tax_code_service", False),
    "TaxDeductedAtSource": ("stripe._tax_deducted_at_source", False),
    "TaxId": ("stripe._tax_id", False),
    "TaxIdService": ("stripe._tax_id_service", False),
    "TaxRate": ("stripe._tax_rate", False),
    "TaxRateService": ("stripe._tax_rate_service", False),
    "TaxService": ("stripe._tax_service", False),
    "TerminalService": ("stripe._terminal_service", False),
    "APIResourceTestHelpers": ("stripe._test_helpers", False),
    "TestHelpersService": ("stripe._test_helpers_service", False),
    "Token": ("stripe._token", False),
    "TokenService": ("stripe._token_service", False),
    "Topup": ("stripe._topup", False),
    "TopupService": ("stripe._topup_service", False),
    "Transfer": ("stripe._transfer", False),
    "TransferReversalService": ("stripe._transfer_reversal_service", False),
    "TransferService": ("stripe._transfer_service", False),
    "TreasuryService": ("stripe._treasury_service", False),
    "UpdateableAPIResource": ("stripe._updateable_api_resource", False),
    "convert_to_stripe_object": ("stripe._util", False),
    "V1Services": ("stripe._v1_services", False),
    "V2Services": ("stripe._v2_services", False),
    "VerifyMixin": ("stripe._verify_mixin", False),
    "Webhook": ("stripe._webhook", False),
    "WebhookSignature": ("stripe._webhook", False),
    "WebhookEndpoint": ("stripe._webhook_endpoint", False),
    "WebhookEndpointService": ("stripe._webhook_endpoint_service", False),
}
if not TYPE_CHECKING:

    def __getattr__(name):
        try:
            target, is_submodule = _import_map[name]
            module = import_module(target)
            if is_submodule:
                return module

            return getattr(
                module,
                name,
            )
        except KeyError:
            raise AttributeError()

# The end of the section generated from our OpenAPI spec
