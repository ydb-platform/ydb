# -*- coding: utf-8 -*-
from importlib import import_module
from typing import Dict, Tuple
from typing_extensions import TYPE_CHECKING, Type

from stripe._stripe_object import StripeObject

if TYPE_CHECKING:
    from stripe._api_mode import ApiMode

OBJECT_CLASSES: Dict[str, Tuple[str, str]] = {
    # data structures
    "list": ("stripe._list_object", "ListObject"),
    "search_result": ("stripe._search_result_object", "SearchResultObject"),
    "file": ("stripe._file", "File"),
    # there's also an alt name for compatibility
    "file_upload": ("stripe._file", "File"),
    # Object classes: The beginning of the section generated from our OpenAPI spec
    "account": ("stripe._account", "Account"),
    "account_link": ("stripe._account_link", "AccountLink"),
    "account_session": ("stripe._account_session", "AccountSession"),
    "apple_pay_domain": ("stripe._apple_pay_domain", "ApplePayDomain"),
    "application": ("stripe._application", "Application"),
    "application_fee": ("stripe._application_fee", "ApplicationFee"),
    "fee_refund": ("stripe._application_fee_refund", "ApplicationFeeRefund"),
    "apps.secret": ("stripe.apps._secret", "Secret"),
    "balance": ("stripe._balance", "Balance"),
    "balance_settings": ("stripe._balance_settings", "BalanceSettings"),
    "balance_transaction": (
        "stripe._balance_transaction",
        "BalanceTransaction",
    ),
    "bank_account": ("stripe._bank_account", "BankAccount"),
    "billing_portal.configuration": (
        "stripe.billing_portal._configuration",
        "Configuration",
    ),
    "billing_portal.session": ("stripe.billing_portal._session", "Session"),
    "billing.alert": ("stripe.billing._alert", "Alert"),
    "billing.alert_triggered": (
        "stripe.billing._alert_triggered",
        "AlertTriggered",
    ),
    "billing.credit_balance_summary": (
        "stripe.billing._credit_balance_summary",
        "CreditBalanceSummary",
    ),
    "billing.credit_balance_transaction": (
        "stripe.billing._credit_balance_transaction",
        "CreditBalanceTransaction",
    ),
    "billing.credit_grant": ("stripe.billing._credit_grant", "CreditGrant"),
    "billing.meter": ("stripe.billing._meter", "Meter"),
    "billing.meter_event": ("stripe.billing._meter_event", "MeterEvent"),
    "billing.meter_event_adjustment": (
        "stripe.billing._meter_event_adjustment",
        "MeterEventAdjustment",
    ),
    "billing.meter_event_summary": (
        "stripe.billing._meter_event_summary",
        "MeterEventSummary",
    ),
    "capability": ("stripe._capability", "Capability"),
    "card": ("stripe._card", "Card"),
    "cash_balance": ("stripe._cash_balance", "CashBalance"),
    "charge": ("stripe._charge", "Charge"),
    "checkout.session": ("stripe.checkout._session", "Session"),
    "climate.order": ("stripe.climate._order", "Order"),
    "climate.product": ("stripe.climate._product", "Product"),
    "climate.supplier": ("stripe.climate._supplier", "Supplier"),
    "confirmation_token": ("stripe._confirmation_token", "ConfirmationToken"),
    "connect_collection_transfer": (
        "stripe._connect_collection_transfer",
        "ConnectCollectionTransfer",
    ),
    "country_spec": ("stripe._country_spec", "CountrySpec"),
    "coupon": ("stripe._coupon", "Coupon"),
    "credit_note": ("stripe._credit_note", "CreditNote"),
    "credit_note_line_item": (
        "stripe._credit_note_line_item",
        "CreditNoteLineItem",
    ),
    "customer": ("stripe._customer", "Customer"),
    "customer_balance_transaction": (
        "stripe._customer_balance_transaction",
        "CustomerBalanceTransaction",
    ),
    "customer_cash_balance_transaction": (
        "stripe._customer_cash_balance_transaction",
        "CustomerCashBalanceTransaction",
    ),
    "customer_session": ("stripe._customer_session", "CustomerSession"),
    "discount": ("stripe._discount", "Discount"),
    "dispute": ("stripe._dispute", "Dispute"),
    "entitlements.active_entitlement": (
        "stripe.entitlements._active_entitlement",
        "ActiveEntitlement",
    ),
    "entitlements.active_entitlement_summary": (
        "stripe.entitlements._active_entitlement_summary",
        "ActiveEntitlementSummary",
    ),
    "entitlements.feature": ("stripe.entitlements._feature", "Feature"),
    "ephemeral_key": ("stripe._ephemeral_key", "EphemeralKey"),
    "event": ("stripe._event", "Event"),
    "exchange_rate": ("stripe._exchange_rate", "ExchangeRate"),
    "file": ("stripe._file", "File"),
    "file_link": ("stripe._file_link", "FileLink"),
    "financial_connections.account": (
        "stripe.financial_connections._account",
        "Account",
    ),
    "financial_connections.account_owner": (
        "stripe.financial_connections._account_owner",
        "AccountOwner",
    ),
    "financial_connections.account_ownership": (
        "stripe.financial_connections._account_ownership",
        "AccountOwnership",
    ),
    "financial_connections.session": (
        "stripe.financial_connections._session",
        "Session",
    ),
    "financial_connections.transaction": (
        "stripe.financial_connections._transaction",
        "Transaction",
    ),
    "forwarding.request": ("stripe.forwarding._request", "Request"),
    "funding_instructions": (
        "stripe._funding_instructions",
        "FundingInstructions",
    ),
    "identity.verification_report": (
        "stripe.identity._verification_report",
        "VerificationReport",
    ),
    "identity.verification_session": (
        "stripe.identity._verification_session",
        "VerificationSession",
    ),
    "invoice": ("stripe._invoice", "Invoice"),
    "invoiceitem": ("stripe._invoice_item", "InvoiceItem"),
    "line_item": ("stripe._invoice_line_item", "InvoiceLineItem"),
    "invoice_payment": ("stripe._invoice_payment", "InvoicePayment"),
    "invoice_rendering_template": (
        "stripe._invoice_rendering_template",
        "InvoiceRenderingTemplate",
    ),
    "issuing.authorization": (
        "stripe.issuing._authorization",
        "Authorization",
    ),
    "issuing.card": ("stripe.issuing._card", "Card"),
    "issuing.cardholder": ("stripe.issuing._cardholder", "Cardholder"),
    "issuing.dispute": ("stripe.issuing._dispute", "Dispute"),
    "issuing.personalization_design": (
        "stripe.issuing._personalization_design",
        "PersonalizationDesign",
    ),
    "issuing.physical_bundle": (
        "stripe.issuing._physical_bundle",
        "PhysicalBundle",
    ),
    "issuing.token": ("stripe.issuing._token", "Token"),
    "issuing.transaction": ("stripe.issuing._transaction", "Transaction"),
    "item": ("stripe._line_item", "LineItem"),
    "login_link": ("stripe._login_link", "LoginLink"),
    "mandate": ("stripe._mandate", "Mandate"),
    "payment_attempt_record": (
        "stripe._payment_attempt_record",
        "PaymentAttemptRecord",
    ),
    "payment_intent": ("stripe._payment_intent", "PaymentIntent"),
    "payment_intent_amount_details_line_item": (
        "stripe._payment_intent_amount_details_line_item",
        "PaymentIntentAmountDetailsLineItem",
    ),
    "payment_link": ("stripe._payment_link", "PaymentLink"),
    "payment_method": ("stripe._payment_method", "PaymentMethod"),
    "payment_method_configuration": (
        "stripe._payment_method_configuration",
        "PaymentMethodConfiguration",
    ),
    "payment_method_domain": (
        "stripe._payment_method_domain",
        "PaymentMethodDomain",
    ),
    "payment_record": ("stripe._payment_record", "PaymentRecord"),
    "payout": ("stripe._payout", "Payout"),
    "person": ("stripe._person", "Person"),
    "plan": ("stripe._plan", "Plan"),
    "price": ("stripe._price", "Price"),
    "product": ("stripe._product", "Product"),
    "product_feature": ("stripe._product_feature", "ProductFeature"),
    "promotion_code": ("stripe._promotion_code", "PromotionCode"),
    "quote": ("stripe._quote", "Quote"),
    "radar.early_fraud_warning": (
        "stripe.radar._early_fraud_warning",
        "EarlyFraudWarning",
    ),
    "radar.payment_evaluation": (
        "stripe.radar._payment_evaluation",
        "PaymentEvaluation",
    ),
    "radar.value_list": ("stripe.radar._value_list", "ValueList"),
    "radar.value_list_item": (
        "stripe.radar._value_list_item",
        "ValueListItem",
    ),
    "refund": ("stripe._refund", "Refund"),
    "reporting.report_run": ("stripe.reporting._report_run", "ReportRun"),
    "reporting.report_type": ("stripe.reporting._report_type", "ReportType"),
    "reserve.hold": ("stripe.reserve._hold", "Hold"),
    "reserve.plan": ("stripe.reserve._plan", "Plan"),
    "reserve.release": ("stripe.reserve._release", "Release"),
    "reserve_transaction": (
        "stripe._reserve_transaction",
        "ReserveTransaction",
    ),
    "transfer_reversal": ("stripe._reversal", "Reversal"),
    "review": ("stripe._review", "Review"),
    "setup_attempt": ("stripe._setup_attempt", "SetupAttempt"),
    "setup_intent": ("stripe._setup_intent", "SetupIntent"),
    "shipping_rate": ("stripe._shipping_rate", "ShippingRate"),
    "scheduled_query_run": (
        "stripe.sigma._scheduled_query_run",
        "ScheduledQueryRun",
    ),
    "source": ("stripe._source", "Source"),
    "source_mandate_notification": (
        "stripe._source_mandate_notification",
        "SourceMandateNotification",
    ),
    "source_transaction": ("stripe._source_transaction", "SourceTransaction"),
    "subscription": ("stripe._subscription", "Subscription"),
    "subscription_item": ("stripe._subscription_item", "SubscriptionItem"),
    "subscription_schedule": (
        "stripe._subscription_schedule",
        "SubscriptionSchedule",
    ),
    "tax.association": ("stripe.tax._association", "Association"),
    "tax.calculation": ("stripe.tax._calculation", "Calculation"),
    "tax.calculation_line_item": (
        "stripe.tax._calculation_line_item",
        "CalculationLineItem",
    ),
    "tax.registration": ("stripe.tax._registration", "Registration"),
    "tax.settings": ("stripe.tax._settings", "Settings"),
    "tax.transaction": ("stripe.tax._transaction", "Transaction"),
    "tax.transaction_line_item": (
        "stripe.tax._transaction_line_item",
        "TransactionLineItem",
    ),
    "tax_code": ("stripe._tax_code", "TaxCode"),
    "tax_deducted_at_source": (
        "stripe._tax_deducted_at_source",
        "TaxDeductedAtSource",
    ),
    "tax_id": ("stripe._tax_id", "TaxId"),
    "tax_rate": ("stripe._tax_rate", "TaxRate"),
    "terminal.configuration": (
        "stripe.terminal._configuration",
        "Configuration",
    ),
    "terminal.connection_token": (
        "stripe.terminal._connection_token",
        "ConnectionToken",
    ),
    "terminal.location": ("stripe.terminal._location", "Location"),
    "terminal.onboarding_link": (
        "stripe.terminal._onboarding_link",
        "OnboardingLink",
    ),
    "terminal.reader": ("stripe.terminal._reader", "Reader"),
    "test_helpers.test_clock": (
        "stripe.test_helpers._test_clock",
        "TestClock",
    ),
    "token": ("stripe._token", "Token"),
    "topup": ("stripe._topup", "Topup"),
    "transfer": ("stripe._transfer", "Transfer"),
    "treasury.credit_reversal": (
        "stripe.treasury._credit_reversal",
        "CreditReversal",
    ),
    "treasury.debit_reversal": (
        "stripe.treasury._debit_reversal",
        "DebitReversal",
    ),
    "treasury.financial_account": (
        "stripe.treasury._financial_account",
        "FinancialAccount",
    ),
    "treasury.financial_account_features": (
        "stripe.treasury._financial_account_features",
        "FinancialAccountFeatures",
    ),
    "treasury.inbound_transfer": (
        "stripe.treasury._inbound_transfer",
        "InboundTransfer",
    ),
    "treasury.outbound_payment": (
        "stripe.treasury._outbound_payment",
        "OutboundPayment",
    ),
    "treasury.outbound_transfer": (
        "stripe.treasury._outbound_transfer",
        "OutboundTransfer",
    ),
    "treasury.received_credit": (
        "stripe.treasury._received_credit",
        "ReceivedCredit",
    ),
    "treasury.received_debit": (
        "stripe.treasury._received_debit",
        "ReceivedDebit",
    ),
    "treasury.transaction": ("stripe.treasury._transaction", "Transaction"),
    "treasury.transaction_entry": (
        "stripe.treasury._transaction_entry",
        "TransactionEntry",
    ),
    "webhook_endpoint": ("stripe._webhook_endpoint", "WebhookEndpoint"),
    # Object classes: The end of the section generated from our OpenAPI spec
}

V2_OBJECT_CLASSES: Dict[str, Tuple[str, str]] = {
    # V2 Object classes: The beginning of the section generated from our OpenAPI spec
    "v2.billing.meter_event": ("stripe.v2.billing._meter_event", "MeterEvent"),
    "v2.billing.meter_event_adjustment": (
        "stripe.v2.billing._meter_event_adjustment",
        "MeterEventAdjustment",
    ),
    "v2.billing.meter_event_session": (
        "stripe.v2.billing._meter_event_session",
        "MeterEventSession",
    ),
    "v2.core.account": ("stripe.v2.core._account", "Account"),
    "v2.core.account_link": ("stripe.v2.core._account_link", "AccountLink"),
    "v2.core.account_person": (
        "stripe.v2.core._account_person",
        "AccountPerson",
    ),
    "v2.core.account_person_token": (
        "stripe.v2.core._account_person_token",
        "AccountPersonToken",
    ),
    "v2.core.account_token": ("stripe.v2.core._account_token", "AccountToken"),
    "v2.core.event": ("stripe.v2.core._event", "Event"),
    "v2.core.event_destination": (
        "stripe.v2.core._event_destination",
        "EventDestination",
    ),
    # V2 Object classes: The end of the section generated from our OpenAPI spec
}


def get_object_class(
    api_mode: "ApiMode", object_name: str
) -> Type[StripeObject]:
    mapping = OBJECT_CLASSES if api_mode == "V1" else V2_OBJECT_CLASSES

    if object_name not in mapping:
        return StripeObject

    import_path, class_name = mapping[object_name]
    return getattr(
        import_module(import_path),
        class_name,
    )
