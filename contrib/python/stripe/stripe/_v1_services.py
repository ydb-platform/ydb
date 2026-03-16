# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._account_link_service import AccountLinkService
    from stripe._account_service import AccountService
    from stripe._account_session_service import AccountSessionService
    from stripe._apple_pay_domain_service import ApplePayDomainService
    from stripe._application_fee_service import ApplicationFeeService
    from stripe._apps_service import AppsService
    from stripe._balance_service import BalanceService
    from stripe._balance_settings_service import BalanceSettingsService
    from stripe._balance_transaction_service import BalanceTransactionService
    from stripe._billing_portal_service import BillingPortalService
    from stripe._billing_service import BillingService
    from stripe._charge_service import ChargeService
    from stripe._checkout_service import CheckoutService
    from stripe._climate_service import ClimateService
    from stripe._confirmation_token_service import ConfirmationTokenService
    from stripe._country_spec_service import CountrySpecService
    from stripe._coupon_service import CouponService
    from stripe._credit_note_service import CreditNoteService
    from stripe._customer_service import CustomerService
    from stripe._customer_session_service import CustomerSessionService
    from stripe._dispute_service import DisputeService
    from stripe._entitlements_service import EntitlementsService
    from stripe._ephemeral_key_service import EphemeralKeyService
    from stripe._event_service import EventService
    from stripe._exchange_rate_service import ExchangeRateService
    from stripe._file_link_service import FileLinkService
    from stripe._file_service import FileService
    from stripe._financial_connections_service import (
        FinancialConnectionsService,
    )
    from stripe._forwarding_service import ForwardingService
    from stripe._identity_service import IdentityService
    from stripe._invoice_item_service import InvoiceItemService
    from stripe._invoice_payment_service import InvoicePaymentService
    from stripe._invoice_rendering_template_service import (
        InvoiceRenderingTemplateService,
    )
    from stripe._invoice_service import InvoiceService
    from stripe._issuing_service import IssuingService
    from stripe._mandate_service import MandateService
    from stripe._payment_attempt_record_service import (
        PaymentAttemptRecordService,
    )
    from stripe._payment_intent_service import PaymentIntentService
    from stripe._payment_link_service import PaymentLinkService
    from stripe._payment_method_configuration_service import (
        PaymentMethodConfigurationService,
    )
    from stripe._payment_method_domain_service import (
        PaymentMethodDomainService,
    )
    from stripe._payment_method_service import PaymentMethodService
    from stripe._payment_record_service import PaymentRecordService
    from stripe._payout_service import PayoutService
    from stripe._plan_service import PlanService
    from stripe._price_service import PriceService
    from stripe._product_service import ProductService
    from stripe._promotion_code_service import PromotionCodeService
    from stripe._quote_service import QuoteService
    from stripe._radar_service import RadarService
    from stripe._refund_service import RefundService
    from stripe._reporting_service import ReportingService
    from stripe._review_service import ReviewService
    from stripe._setup_attempt_service import SetupAttemptService
    from stripe._setup_intent_service import SetupIntentService
    from stripe._shipping_rate_service import ShippingRateService
    from stripe._sigma_service import SigmaService
    from stripe._source_service import SourceService
    from stripe._subscription_item_service import SubscriptionItemService
    from stripe._subscription_schedule_service import (
        SubscriptionScheduleService,
    )
    from stripe._subscription_service import SubscriptionService
    from stripe._tax_code_service import TaxCodeService
    from stripe._tax_id_service import TaxIdService
    from stripe._tax_rate_service import TaxRateService
    from stripe._tax_service import TaxService
    from stripe._terminal_service import TerminalService
    from stripe._test_helpers_service import TestHelpersService
    from stripe._token_service import TokenService
    from stripe._topup_service import TopupService
    from stripe._transfer_service import TransferService
    from stripe._treasury_service import TreasuryService
    from stripe._webhook_endpoint_service import WebhookEndpointService

_subservices = {
    "accounts": ["stripe._account_service", "AccountService"],
    "account_links": ["stripe._account_link_service", "AccountLinkService"],
    "account_sessions": [
        "stripe._account_session_service",
        "AccountSessionService",
    ],
    "apple_pay_domains": [
        "stripe._apple_pay_domain_service",
        "ApplePayDomainService",
    ],
    "application_fees": [
        "stripe._application_fee_service",
        "ApplicationFeeService",
    ],
    "apps": ["stripe._apps_service", "AppsService"],
    "balance": ["stripe._balance_service", "BalanceService"],
    "balance_settings": [
        "stripe._balance_settings_service",
        "BalanceSettingsService",
    ],
    "balance_transactions": [
        "stripe._balance_transaction_service",
        "BalanceTransactionService",
    ],
    "billing": ["stripe._billing_service", "BillingService"],
    "billing_portal": [
        "stripe._billing_portal_service",
        "BillingPortalService",
    ],
    "charges": ["stripe._charge_service", "ChargeService"],
    "checkout": ["stripe._checkout_service", "CheckoutService"],
    "climate": ["stripe._climate_service", "ClimateService"],
    "confirmation_tokens": [
        "stripe._confirmation_token_service",
        "ConfirmationTokenService",
    ],
    "country_specs": ["stripe._country_spec_service", "CountrySpecService"],
    "coupons": ["stripe._coupon_service", "CouponService"],
    "credit_notes": ["stripe._credit_note_service", "CreditNoteService"],
    "customers": ["stripe._customer_service", "CustomerService"],
    "customer_sessions": [
        "stripe._customer_session_service",
        "CustomerSessionService",
    ],
    "disputes": ["stripe._dispute_service", "DisputeService"],
    "entitlements": ["stripe._entitlements_service", "EntitlementsService"],
    "ephemeral_keys": ["stripe._ephemeral_key_service", "EphemeralKeyService"],
    "events": ["stripe._event_service", "EventService"],
    "exchange_rates": ["stripe._exchange_rate_service", "ExchangeRateService"],
    "files": ["stripe._file_service", "FileService"],
    "file_links": ["stripe._file_link_service", "FileLinkService"],
    "financial_connections": [
        "stripe._financial_connections_service",
        "FinancialConnectionsService",
    ],
    "forwarding": ["stripe._forwarding_service", "ForwardingService"],
    "identity": ["stripe._identity_service", "IdentityService"],
    "invoices": ["stripe._invoice_service", "InvoiceService"],
    "invoice_items": ["stripe._invoice_item_service", "InvoiceItemService"],
    "invoice_payments": [
        "stripe._invoice_payment_service",
        "InvoicePaymentService",
    ],
    "invoice_rendering_templates": [
        "stripe._invoice_rendering_template_service",
        "InvoiceRenderingTemplateService",
    ],
    "issuing": ["stripe._issuing_service", "IssuingService"],
    "mandates": ["stripe._mandate_service", "MandateService"],
    "payment_attempt_records": [
        "stripe._payment_attempt_record_service",
        "PaymentAttemptRecordService",
    ],
    "payment_intents": [
        "stripe._payment_intent_service",
        "PaymentIntentService",
    ],
    "payment_links": ["stripe._payment_link_service", "PaymentLinkService"],
    "payment_methods": [
        "stripe._payment_method_service",
        "PaymentMethodService",
    ],
    "payment_method_configurations": [
        "stripe._payment_method_configuration_service",
        "PaymentMethodConfigurationService",
    ],
    "payment_method_domains": [
        "stripe._payment_method_domain_service",
        "PaymentMethodDomainService",
    ],
    "payment_records": [
        "stripe._payment_record_service",
        "PaymentRecordService",
    ],
    "payouts": ["stripe._payout_service", "PayoutService"],
    "plans": ["stripe._plan_service", "PlanService"],
    "prices": ["stripe._price_service", "PriceService"],
    "products": ["stripe._product_service", "ProductService"],
    "promotion_codes": [
        "stripe._promotion_code_service",
        "PromotionCodeService",
    ],
    "quotes": ["stripe._quote_service", "QuoteService"],
    "radar": ["stripe._radar_service", "RadarService"],
    "refunds": ["stripe._refund_service", "RefundService"],
    "reporting": ["stripe._reporting_service", "ReportingService"],
    "reviews": ["stripe._review_service", "ReviewService"],
    "setup_attempts": ["stripe._setup_attempt_service", "SetupAttemptService"],
    "setup_intents": ["stripe._setup_intent_service", "SetupIntentService"],
    "shipping_rates": ["stripe._shipping_rate_service", "ShippingRateService"],
    "sigma": ["stripe._sigma_service", "SigmaService"],
    "sources": ["stripe._source_service", "SourceService"],
    "subscriptions": ["stripe._subscription_service", "SubscriptionService"],
    "subscription_items": [
        "stripe._subscription_item_service",
        "SubscriptionItemService",
    ],
    "subscription_schedules": [
        "stripe._subscription_schedule_service",
        "SubscriptionScheduleService",
    ],
    "tax": ["stripe._tax_service", "TaxService"],
    "tax_codes": ["stripe._tax_code_service", "TaxCodeService"],
    "tax_ids": ["stripe._tax_id_service", "TaxIdService"],
    "tax_rates": ["stripe._tax_rate_service", "TaxRateService"],
    "terminal": ["stripe._terminal_service", "TerminalService"],
    "test_helpers": ["stripe._test_helpers_service", "TestHelpersService"],
    "tokens": ["stripe._token_service", "TokenService"],
    "topups": ["stripe._topup_service", "TopupService"],
    "transfers": ["stripe._transfer_service", "TransferService"],
    "treasury": ["stripe._treasury_service", "TreasuryService"],
    "webhook_endpoints": [
        "stripe._webhook_endpoint_service",
        "WebhookEndpointService",
    ],
}


class V1Services(StripeService):
    accounts: "AccountService"
    account_links: "AccountLinkService"
    account_sessions: "AccountSessionService"
    apple_pay_domains: "ApplePayDomainService"
    application_fees: "ApplicationFeeService"
    apps: "AppsService"
    balance: "BalanceService"
    balance_settings: "BalanceSettingsService"
    balance_transactions: "BalanceTransactionService"
    billing: "BillingService"
    billing_portal: "BillingPortalService"
    charges: "ChargeService"
    checkout: "CheckoutService"
    climate: "ClimateService"
    confirmation_tokens: "ConfirmationTokenService"
    country_specs: "CountrySpecService"
    coupons: "CouponService"
    credit_notes: "CreditNoteService"
    customers: "CustomerService"
    customer_sessions: "CustomerSessionService"
    disputes: "DisputeService"
    entitlements: "EntitlementsService"
    ephemeral_keys: "EphemeralKeyService"
    events: "EventService"
    exchange_rates: "ExchangeRateService"
    files: "FileService"
    file_links: "FileLinkService"
    financial_connections: "FinancialConnectionsService"
    forwarding: "ForwardingService"
    identity: "IdentityService"
    invoices: "InvoiceService"
    invoice_items: "InvoiceItemService"
    invoice_payments: "InvoicePaymentService"
    invoice_rendering_templates: "InvoiceRenderingTemplateService"
    issuing: "IssuingService"
    mandates: "MandateService"
    payment_attempt_records: "PaymentAttemptRecordService"
    payment_intents: "PaymentIntentService"
    payment_links: "PaymentLinkService"
    payment_methods: "PaymentMethodService"
    payment_method_configurations: "PaymentMethodConfigurationService"
    payment_method_domains: "PaymentMethodDomainService"
    payment_records: "PaymentRecordService"
    payouts: "PayoutService"
    plans: "PlanService"
    prices: "PriceService"
    products: "ProductService"
    promotion_codes: "PromotionCodeService"
    quotes: "QuoteService"
    radar: "RadarService"
    refunds: "RefundService"
    reporting: "ReportingService"
    reviews: "ReviewService"
    setup_attempts: "SetupAttemptService"
    setup_intents: "SetupIntentService"
    shipping_rates: "ShippingRateService"
    sigma: "SigmaService"
    sources: "SourceService"
    subscriptions: "SubscriptionService"
    subscription_items: "SubscriptionItemService"
    subscription_schedules: "SubscriptionScheduleService"
    tax: "TaxService"
    tax_codes: "TaxCodeService"
    tax_ids: "TaxIdService"
    tax_rates: "TaxRateService"
    terminal: "TerminalService"
    test_helpers: "TestHelpersService"
    tokens: "TokenService"
    topups: "TopupService"
    transfers: "TransferService"
    treasury: "TreasuryService"
    webhook_endpoints: "WebhookEndpointService"

    def __init__(self, requestor):
        super().__init__(requestor)

    def __getattr__(self, name):
        try:
            import_from, service = _subservices[name]
            service_class = getattr(
                import_module(import_from),
                service,
            )
            setattr(
                self,
                name,
                service_class(self._requestor),
            )
            return getattr(self, name)
        except KeyError:
            raise AttributeError()
