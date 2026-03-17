# -*- coding: utf-8 -*-

import json
from collections import OrderedDict

from stripe import (
    DEFAULT_API_BASE,
    DEFAULT_CONNECT_API_BASE,
    DEFAULT_UPLOAD_API_BASE,
    DEFAULT_METER_EVENTS_API_BASE,
)

from stripe._api_mode import ApiMode
from stripe._error import AuthenticationError
from stripe._request_options import extract_options_from_dict
from stripe._requestor_options import RequestorOptions, BaseAddresses
from stripe._client_options import _ClientOptions
from stripe._http_client import (
    new_default_http_client,
    new_http_client_async_fallback,
)
from stripe._api_version import _ApiVersion
from stripe._stripe_object import StripeObject
from stripe._stripe_response import StripeResponse
from stripe._util import _convert_to_stripe_object, get_api_mode, deprecated  # noqa: F401
from stripe._webhook import Webhook, WebhookSignature
from stripe._event import Event
from stripe.v2.core._event import EventNotification

from typing import Any, Dict, Optional, Union, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._stripe_context import StripeContext
    from stripe._http_client import HTTPClient

# Non-generated services
from stripe._oauth_service import OAuthService

from stripe._v1_services import V1Services
from stripe._v2_services import V2Services

# service-types: The beginning of the section generated from our OpenAPI spec
if TYPE_CHECKING:
    from stripe._account_service import AccountService
    from stripe._account_link_service import AccountLinkService
    from stripe._account_session_service import AccountSessionService
    from stripe._apple_pay_domain_service import ApplePayDomainService
    from stripe._application_fee_service import ApplicationFeeService
    from stripe._apps_service import AppsService
    from stripe._balance_service import BalanceService
    from stripe._balance_settings_service import BalanceSettingsService
    from stripe._balance_transaction_service import BalanceTransactionService
    from stripe._billing_service import BillingService
    from stripe._billing_portal_service import BillingPortalService
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
    from stripe._file_service import FileService
    from stripe._file_link_service import FileLinkService
    from stripe._financial_connections_service import (
        FinancialConnectionsService,
    )
    from stripe._forwarding_service import ForwardingService
    from stripe._identity_service import IdentityService
    from stripe._invoice_service import InvoiceService
    from stripe._invoice_item_service import InvoiceItemService
    from stripe._invoice_payment_service import InvoicePaymentService
    from stripe._invoice_rendering_template_service import (
        InvoiceRenderingTemplateService,
    )
    from stripe._issuing_service import IssuingService
    from stripe._mandate_service import MandateService
    from stripe._payment_attempt_record_service import (
        PaymentAttemptRecordService,
    )
    from stripe._payment_intent_service import PaymentIntentService
    from stripe._payment_link_service import PaymentLinkService
    from stripe._payment_method_service import PaymentMethodService
    from stripe._payment_method_configuration_service import (
        PaymentMethodConfigurationService,
    )
    from stripe._payment_method_domain_service import (
        PaymentMethodDomainService,
    )
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
    from stripe._subscription_service import SubscriptionService
    from stripe._subscription_item_service import SubscriptionItemService
    from stripe._subscription_schedule_service import (
        SubscriptionScheduleService,
    )
    from stripe._tax_service import TaxService
    from stripe._tax_code_service import TaxCodeService
    from stripe._tax_id_service import TaxIdService
    from stripe._tax_rate_service import TaxRateService
    from stripe._terminal_service import TerminalService
    from stripe._test_helpers_service import TestHelpersService
    from stripe._token_service import TokenService
    from stripe._topup_service import TopupService
    from stripe._transfer_service import TransferService
    from stripe._treasury_service import TreasuryService
    from stripe._webhook_endpoint_service import WebhookEndpointService

# service-types: The end of the section generated from our OpenAPI spec

if TYPE_CHECKING:
    from stripe.events._event_classes import ALL_EVENT_NOTIFICATIONS


class StripeClient(object):
    def __init__(
        self,
        api_key: str,
        *,
        stripe_account: Optional[str] = None,
        stripe_context: "Optional[Union[str, StripeContext]]" = None,
        stripe_version: Optional[str] = None,
        base_addresses: Optional[BaseAddresses] = None,
        client_id: Optional[str] = None,
        verify_ssl_certs: bool = True,
        proxy: Optional[str] = None,
        max_network_retries: Optional[int] = None,
        http_client: Optional["HTTPClient"] = None,
    ):
        # The types forbid this, but let's give users without types a friendly error.
        if api_key is None:  # pyright: ignore[reportUnnecessaryComparison]
            raise AuthenticationError(
                "No API key provided. (HINT: set your API key using "
                '"client = stripe.StripeClient(<API-KEY>)"). You can '
                "generate API keys from the Stripe web interface. "
                "See https://stripe.com/api for details, or email "
                "support@stripe.com if you have any questions."
            )

        if http_client and (proxy or verify_ssl_certs is not True):
            raise ValueError(
                "You cannot specify `proxy` or `verify_ssl_certs` when passing "
                "in a custom `http_client`. Please set these values on your "
                "custom `http_client` instead."
            )

        # Default to stripe.DEFAULT_API_BASE, stripe.DEFAULT_CONNECT_API_BASE,
        # and stripe.DEFAULT_UPLOAD_API_BASE if not set in base_addresses.
        base_addresses = {
            "api": DEFAULT_API_BASE,
            "connect": DEFAULT_CONNECT_API_BASE,
            "files": DEFAULT_UPLOAD_API_BASE,
            "meter_events": DEFAULT_METER_EVENTS_API_BASE,
            **(base_addresses or {}),
        }

        requestor_options = RequestorOptions(
            api_key=api_key,
            stripe_account=stripe_account,
            stripe_context=stripe_context,
            stripe_version=stripe_version or _ApiVersion.CURRENT,
            base_addresses=base_addresses,
            max_network_retries=max_network_retries,
        )

        if http_client is None:
            http_client = new_default_http_client(
                async_fallback_client=new_http_client_async_fallback(
                    proxy=proxy, verify_ssl_certs=verify_ssl_certs
                ),
                proxy=proxy,
                verify_ssl_certs=verify_ssl_certs,
            )

        from stripe._api_requestor import _APIRequestor

        self._requestor = _APIRequestor(
            options=requestor_options,
            client=http_client,
        )

        self._options = _ClientOptions(
            client_id=client_id,
            proxy=proxy,
            verify_ssl_certs=verify_ssl_certs,
        )

        self.oauth = OAuthService(self._requestor, self._options)

        # top-level services: The beginning of the section generated from our OpenAPI spec
        self.v1 = V1Services(self._requestor)
        self.v2 = V2Services(self._requestor)
        # top-level services: The end of the section generated from our OpenAPI spec

    def parse_event_notification(
        self,
        raw: Union[bytes, str, bytearray],
        sig_header: str,
        secret: str,
        tolerance: int = Webhook.DEFAULT_TOLERANCE,
    ) -> "ALL_EVENT_NOTIFICATIONS":
        """
        This should be your main method for interacting with `EventNotifications`. It's the V2 equivalent of `construct_event()`, but with better typing support.

        It returns a union representing all known `EventNotification` classes. They have a `type` property that can be used for narrowing, which will get you very specific type support. If parsing an event the SDK isn't familiar with, it'll instead return `UnknownEventNotification`. That's not reflected in the return type of the function (because it messes up type narrowing) but is otherwise intended.
        """
        payload = (
            cast(Union[bytes, bytearray], raw).decode("utf-8")
            if hasattr(raw, "decode")
            else cast(str, raw)
        )

        WebhookSignature.verify_header(payload, sig_header, secret, tolerance)

        return cast(
            "ALL_EVENT_NOTIFICATIONS",
            EventNotification.from_json(payload, self),
        )

    def construct_event(
        self,
        payload: Union[bytes, str],
        sig_header: str,
        secret: str,
        tolerance: int = Webhook.DEFAULT_TOLERANCE,
    ) -> Event:
        if hasattr(payload, "decode"):
            payload = cast(bytes, payload).decode("utf-8")

        WebhookSignature.verify_header(payload, sig_header, secret, tolerance)

        data = json.loads(payload, object_pairs_hook=OrderedDict)
        event = Event._construct_from(
            values=data,
            requestor=self._requestor,
            api_mode="V1",
        )

        return event

    def raw_request(self, method_: str, url_: str, **params):
        params = params.copy()
        options, params = extract_options_from_dict(params)
        api_mode = get_api_mode(url_)
        base_address = params.pop("base", "api")

        # we manually pass usage in event internals, so use those if available
        usage = params.pop("usage", ["raw_request"])

        rbody, rcode, rheaders = self._requestor.request_raw(
            method_,
            url_,
            params=params,
            options=options,
            base_address=base_address,
            api_mode=api_mode,
            usage=usage,
        )

        return self._requestor._interpret_response(
            rbody, rcode, rheaders, api_mode
        )

    async def raw_request_async(self, method_: str, url_: str, **params):
        params = params.copy()
        options, params = extract_options_from_dict(params)
        api_mode = get_api_mode(url_)
        base_address = params.pop("base", "api")

        rbody, rcode, rheaders = await self._requestor.request_raw_async(
            method_,
            url_,
            params=params,
            options=options,
            base_address=base_address,
            api_mode=api_mode,
            usage=["raw_request"],
        )

        return self._requestor._interpret_response(
            rbody, rcode, rheaders, api_mode
        )

    def deserialize(
        self,
        resp: Union[StripeResponse, Dict[str, Any]],
        params: Optional[Dict[str, Any]] = None,
        *,
        api_mode: ApiMode,
    ) -> StripeObject:
        """
        Used to translate the result of a `raw_request` into a StripeObject.
        """
        return _convert_to_stripe_object(
            resp=resp,
            params=params,
            requestor=self._requestor,
            api_mode=api_mode,
        )

    # deprecated v1 services: The beginning of the section generated from our OpenAPI spec
    @property
    @deprecated(
        """
        StripeClient.accounts is deprecated, use StripeClient.v1.accounts instead.
          All functionality under it has been copied over to StripeClient.v1.accounts.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def accounts(self) -> "AccountService":
        return self.v1.accounts

    @property
    @deprecated(
        """
        StripeClient.account_links is deprecated, use StripeClient.v1.account_links instead.
          All functionality under it has been copied over to StripeClient.v1.account_links.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def account_links(self) -> "AccountLinkService":
        return self.v1.account_links

    @property
    @deprecated(
        """
        StripeClient.account_sessions is deprecated, use StripeClient.v1.account_sessions instead.
          All functionality under it has been copied over to StripeClient.v1.account_sessions.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def account_sessions(self) -> "AccountSessionService":
        return self.v1.account_sessions

    @property
    @deprecated(
        """
        StripeClient.apple_pay_domains is deprecated, use StripeClient.v1.apple_pay_domains instead.
          All functionality under it has been copied over to StripeClient.v1.apple_pay_domains.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def apple_pay_domains(self) -> "ApplePayDomainService":
        return self.v1.apple_pay_domains

    @property
    @deprecated(
        """
        StripeClient.application_fees is deprecated, use StripeClient.v1.application_fees instead.
          All functionality under it has been copied over to StripeClient.v1.application_fees.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def application_fees(self) -> "ApplicationFeeService":
        return self.v1.application_fees

    @property
    @deprecated(
        """
        StripeClient.apps is deprecated, use StripeClient.v1.apps instead.
          All functionality under it has been copied over to StripeClient.v1.apps.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def apps(self) -> "AppsService":
        return self.v1.apps

    @property
    @deprecated(
        """
        StripeClient.balance is deprecated, use StripeClient.v1.balance instead.
          All functionality under it has been copied over to StripeClient.v1.balance.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def balance(self) -> "BalanceService":
        return self.v1.balance

    @property
    @deprecated(
        """
        StripeClient.balance_settings is deprecated, use StripeClient.v1.balance_settings instead.
          All functionality under it has been copied over to StripeClient.v1.balance_settings.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def balance_settings(self) -> "BalanceSettingsService":
        return self.v1.balance_settings

    @property
    @deprecated(
        """
        StripeClient.balance_transactions is deprecated, use StripeClient.v1.balance_transactions instead.
          All functionality under it has been copied over to StripeClient.v1.balance_transactions.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def balance_transactions(self) -> "BalanceTransactionService":
        return self.v1.balance_transactions

    @property
    @deprecated(
        """
        StripeClient.billing is deprecated, use StripeClient.v1.billing instead.
          All functionality under it has been copied over to StripeClient.v1.billing.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def billing(self) -> "BillingService":
        return self.v1.billing

    @property
    @deprecated(
        """
        StripeClient.billing_portal is deprecated, use StripeClient.v1.billing_portal instead.
          All functionality under it has been copied over to StripeClient.v1.billing_portal.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def billing_portal(self) -> "BillingPortalService":
        return self.v1.billing_portal

    @property
    @deprecated(
        """
        StripeClient.charges is deprecated, use StripeClient.v1.charges instead.
          All functionality under it has been copied over to StripeClient.v1.charges.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def charges(self) -> "ChargeService":
        return self.v1.charges

    @property
    @deprecated(
        """
        StripeClient.checkout is deprecated, use StripeClient.v1.checkout instead.
          All functionality under it has been copied over to StripeClient.v1.checkout.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def checkout(self) -> "CheckoutService":
        return self.v1.checkout

    @property
    @deprecated(
        """
        StripeClient.climate is deprecated, use StripeClient.v1.climate instead.
          All functionality under it has been copied over to StripeClient.v1.climate.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def climate(self) -> "ClimateService":
        return self.v1.climate

    @property
    @deprecated(
        """
        StripeClient.confirmation_tokens is deprecated, use StripeClient.v1.confirmation_tokens instead.
          All functionality under it has been copied over to StripeClient.v1.confirmation_tokens.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def confirmation_tokens(self) -> "ConfirmationTokenService":
        return self.v1.confirmation_tokens

    @property
    @deprecated(
        """
        StripeClient.country_specs is deprecated, use StripeClient.v1.country_specs instead.
          All functionality under it has been copied over to StripeClient.v1.country_specs.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def country_specs(self) -> "CountrySpecService":
        return self.v1.country_specs

    @property
    @deprecated(
        """
        StripeClient.coupons is deprecated, use StripeClient.v1.coupons instead.
          All functionality under it has been copied over to StripeClient.v1.coupons.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def coupons(self) -> "CouponService":
        return self.v1.coupons

    @property
    @deprecated(
        """
        StripeClient.credit_notes is deprecated, use StripeClient.v1.credit_notes instead.
          All functionality under it has been copied over to StripeClient.v1.credit_notes.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def credit_notes(self) -> "CreditNoteService":
        return self.v1.credit_notes

    @property
    @deprecated(
        """
        StripeClient.customers is deprecated, use StripeClient.v1.customers instead.
          All functionality under it has been copied over to StripeClient.v1.customers.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def customers(self) -> "CustomerService":
        return self.v1.customers

    @property
    @deprecated(
        """
        StripeClient.customer_sessions is deprecated, use StripeClient.v1.customer_sessions instead.
          All functionality under it has been copied over to StripeClient.v1.customer_sessions.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def customer_sessions(self) -> "CustomerSessionService":
        return self.v1.customer_sessions

    @property
    @deprecated(
        """
        StripeClient.disputes is deprecated, use StripeClient.v1.disputes instead.
          All functionality under it has been copied over to StripeClient.v1.disputes.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def disputes(self) -> "DisputeService":
        return self.v1.disputes

    @property
    @deprecated(
        """
        StripeClient.entitlements is deprecated, use StripeClient.v1.entitlements instead.
          All functionality under it has been copied over to StripeClient.v1.entitlements.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def entitlements(self) -> "EntitlementsService":
        return self.v1.entitlements

    @property
    @deprecated(
        """
        StripeClient.ephemeral_keys is deprecated, use StripeClient.v1.ephemeral_keys instead.
          All functionality under it has been copied over to StripeClient.v1.ephemeral_keys.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def ephemeral_keys(self) -> "EphemeralKeyService":
        return self.v1.ephemeral_keys

    @property
    @deprecated(
        """
        StripeClient.events is deprecated, use StripeClient.v1.events instead.
          All functionality under it has been copied over to StripeClient.v1.events.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def events(self) -> "EventService":
        return self.v1.events

    @property
    @deprecated(
        """
        StripeClient.exchange_rates is deprecated, use StripeClient.v1.exchange_rates instead.
          All functionality under it has been copied over to StripeClient.v1.exchange_rates.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def exchange_rates(self) -> "ExchangeRateService":
        return self.v1.exchange_rates

    @property
    @deprecated(
        """
        StripeClient.files is deprecated, use StripeClient.v1.files instead.
          All functionality under it has been copied over to StripeClient.v1.files.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def files(self) -> "FileService":
        return self.v1.files

    @property
    @deprecated(
        """
        StripeClient.file_links is deprecated, use StripeClient.v1.file_links instead.
          All functionality under it has been copied over to StripeClient.v1.file_links.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def file_links(self) -> "FileLinkService":
        return self.v1.file_links

    @property
    @deprecated(
        """
        StripeClient.financial_connections is deprecated, use StripeClient.v1.financial_connections instead.
          All functionality under it has been copied over to StripeClient.v1.financial_connections.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def financial_connections(self) -> "FinancialConnectionsService":
        return self.v1.financial_connections

    @property
    @deprecated(
        """
        StripeClient.forwarding is deprecated, use StripeClient.v1.forwarding instead.
          All functionality under it has been copied over to StripeClient.v1.forwarding.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def forwarding(self) -> "ForwardingService":
        return self.v1.forwarding

    @property
    @deprecated(
        """
        StripeClient.identity is deprecated, use StripeClient.v1.identity instead.
          All functionality under it has been copied over to StripeClient.v1.identity.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def identity(self) -> "IdentityService":
        return self.v1.identity

    @property
    @deprecated(
        """
        StripeClient.invoices is deprecated, use StripeClient.v1.invoices instead.
          All functionality under it has been copied over to StripeClient.v1.invoices.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def invoices(self) -> "InvoiceService":
        return self.v1.invoices

    @property
    @deprecated(
        """
        StripeClient.invoice_items is deprecated, use StripeClient.v1.invoice_items instead.
          All functionality under it has been copied over to StripeClient.v1.invoice_items.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def invoice_items(self) -> "InvoiceItemService":
        return self.v1.invoice_items

    @property
    @deprecated(
        """
        StripeClient.invoice_payments is deprecated, use StripeClient.v1.invoice_payments instead.
          All functionality under it has been copied over to StripeClient.v1.invoice_payments.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def invoice_payments(self) -> "InvoicePaymentService":
        return self.v1.invoice_payments

    @property
    @deprecated(
        """
        StripeClient.invoice_rendering_templates is deprecated, use StripeClient.v1.invoice_rendering_templates instead.
          All functionality under it has been copied over to StripeClient.v1.invoice_rendering_templates.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def invoice_rendering_templates(self) -> "InvoiceRenderingTemplateService":
        return self.v1.invoice_rendering_templates

    @property
    @deprecated(
        """
        StripeClient.issuing is deprecated, use StripeClient.v1.issuing instead.
          All functionality under it has been copied over to StripeClient.v1.issuing.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def issuing(self) -> "IssuingService":
        return self.v1.issuing

    @property
    @deprecated(
        """
        StripeClient.mandates is deprecated, use StripeClient.v1.mandates instead.
          All functionality under it has been copied over to StripeClient.v1.mandates.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def mandates(self) -> "MandateService":
        return self.v1.mandates

    @property
    @deprecated(
        """
        StripeClient.payment_attempt_records is deprecated, use StripeClient.v1.payment_attempt_records instead.
          All functionality under it has been copied over to StripeClient.v1.payment_attempt_records.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def payment_attempt_records(self) -> "PaymentAttemptRecordService":
        return self.v1.payment_attempt_records

    @property
    @deprecated(
        """
        StripeClient.payment_intents is deprecated, use StripeClient.v1.payment_intents instead.
          All functionality under it has been copied over to StripeClient.v1.payment_intents.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def payment_intents(self) -> "PaymentIntentService":
        return self.v1.payment_intents

    @property
    @deprecated(
        """
        StripeClient.payment_links is deprecated, use StripeClient.v1.payment_links instead.
          All functionality under it has been copied over to StripeClient.v1.payment_links.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def payment_links(self) -> "PaymentLinkService":
        return self.v1.payment_links

    @property
    @deprecated(
        """
        StripeClient.payment_methods is deprecated, use StripeClient.v1.payment_methods instead.
          All functionality under it has been copied over to StripeClient.v1.payment_methods.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def payment_methods(self) -> "PaymentMethodService":
        return self.v1.payment_methods

    @property
    @deprecated(
        """
        StripeClient.payment_method_configurations is deprecated, use StripeClient.v1.payment_method_configurations instead.
          All functionality under it has been copied over to StripeClient.v1.payment_method_configurations.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def payment_method_configurations(
        self,
    ) -> "PaymentMethodConfigurationService":
        return self.v1.payment_method_configurations

    @property
    @deprecated(
        """
        StripeClient.payment_method_domains is deprecated, use StripeClient.v1.payment_method_domains instead.
          All functionality under it has been copied over to StripeClient.v1.payment_method_domains.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def payment_method_domains(self) -> "PaymentMethodDomainService":
        return self.v1.payment_method_domains

    @property
    @deprecated(
        """
        StripeClient.payment_records is deprecated, use StripeClient.v1.payment_records instead.
          All functionality under it has been copied over to StripeClient.v1.payment_records.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def payment_records(self) -> "PaymentRecordService":
        return self.v1.payment_records

    @property
    @deprecated(
        """
        StripeClient.payouts is deprecated, use StripeClient.v1.payouts instead.
          All functionality under it has been copied over to StripeClient.v1.payouts.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def payouts(self) -> "PayoutService":
        return self.v1.payouts

    @property
    @deprecated(
        """
        StripeClient.plans is deprecated, use StripeClient.v1.plans instead.
          All functionality under it has been copied over to StripeClient.v1.plans.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def plans(self) -> "PlanService":
        return self.v1.plans

    @property
    @deprecated(
        """
        StripeClient.prices is deprecated, use StripeClient.v1.prices instead.
          All functionality under it has been copied over to StripeClient.v1.prices.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def prices(self) -> "PriceService":
        return self.v1.prices

    @property
    @deprecated(
        """
        StripeClient.products is deprecated, use StripeClient.v1.products instead.
          All functionality under it has been copied over to StripeClient.v1.products.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def products(self) -> "ProductService":
        return self.v1.products

    @property
    @deprecated(
        """
        StripeClient.promotion_codes is deprecated, use StripeClient.v1.promotion_codes instead.
          All functionality under it has been copied over to StripeClient.v1.promotion_codes.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def promotion_codes(self) -> "PromotionCodeService":
        return self.v1.promotion_codes

    @property
    @deprecated(
        """
        StripeClient.quotes is deprecated, use StripeClient.v1.quotes instead.
          All functionality under it has been copied over to StripeClient.v1.quotes.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def quotes(self) -> "QuoteService":
        return self.v1.quotes

    @property
    @deprecated(
        """
        StripeClient.radar is deprecated, use StripeClient.v1.radar instead.
          All functionality under it has been copied over to StripeClient.v1.radar.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def radar(self) -> "RadarService":
        return self.v1.radar

    @property
    @deprecated(
        """
        StripeClient.refunds is deprecated, use StripeClient.v1.refunds instead.
          All functionality under it has been copied over to StripeClient.v1.refunds.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def refunds(self) -> "RefundService":
        return self.v1.refunds

    @property
    @deprecated(
        """
        StripeClient.reporting is deprecated, use StripeClient.v1.reporting instead.
          All functionality under it has been copied over to StripeClient.v1.reporting.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def reporting(self) -> "ReportingService":
        return self.v1.reporting

    @property
    @deprecated(
        """
        StripeClient.reviews is deprecated, use StripeClient.v1.reviews instead.
          All functionality under it has been copied over to StripeClient.v1.reviews.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def reviews(self) -> "ReviewService":
        return self.v1.reviews

    @property
    @deprecated(
        """
        StripeClient.setup_attempts is deprecated, use StripeClient.v1.setup_attempts instead.
          All functionality under it has been copied over to StripeClient.v1.setup_attempts.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def setup_attempts(self) -> "SetupAttemptService":
        return self.v1.setup_attempts

    @property
    @deprecated(
        """
        StripeClient.setup_intents is deprecated, use StripeClient.v1.setup_intents instead.
          All functionality under it has been copied over to StripeClient.v1.setup_intents.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def setup_intents(self) -> "SetupIntentService":
        return self.v1.setup_intents

    @property
    @deprecated(
        """
        StripeClient.shipping_rates is deprecated, use StripeClient.v1.shipping_rates instead.
          All functionality under it has been copied over to StripeClient.v1.shipping_rates.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def shipping_rates(self) -> "ShippingRateService":
        return self.v1.shipping_rates

    @property
    @deprecated(
        """
        StripeClient.sigma is deprecated, use StripeClient.v1.sigma instead.
          All functionality under it has been copied over to StripeClient.v1.sigma.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def sigma(self) -> "SigmaService":
        return self.v1.sigma

    @property
    @deprecated(
        """
        StripeClient.sources is deprecated, use StripeClient.v1.sources instead.
          All functionality under it has been copied over to StripeClient.v1.sources.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def sources(self) -> "SourceService":
        return self.v1.sources

    @property
    @deprecated(
        """
        StripeClient.subscriptions is deprecated, use StripeClient.v1.subscriptions instead.
          All functionality under it has been copied over to StripeClient.v1.subscriptions.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def subscriptions(self) -> "SubscriptionService":
        return self.v1.subscriptions

    @property
    @deprecated(
        """
        StripeClient.subscription_items is deprecated, use StripeClient.v1.subscription_items instead.
          All functionality under it has been copied over to StripeClient.v1.subscription_items.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def subscription_items(self) -> "SubscriptionItemService":
        return self.v1.subscription_items

    @property
    @deprecated(
        """
        StripeClient.subscription_schedules is deprecated, use StripeClient.v1.subscription_schedules instead.
          All functionality under it has been copied over to StripeClient.v1.subscription_schedules.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def subscription_schedules(self) -> "SubscriptionScheduleService":
        return self.v1.subscription_schedules

    @property
    @deprecated(
        """
        StripeClient.tax is deprecated, use StripeClient.v1.tax instead.
          All functionality under it has been copied over to StripeClient.v1.tax.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def tax(self) -> "TaxService":
        return self.v1.tax

    @property
    @deprecated(
        """
        StripeClient.tax_codes is deprecated, use StripeClient.v1.tax_codes instead.
          All functionality under it has been copied over to StripeClient.v1.tax_codes.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def tax_codes(self) -> "TaxCodeService":
        return self.v1.tax_codes

    @property
    @deprecated(
        """
        StripeClient.tax_ids is deprecated, use StripeClient.v1.tax_ids instead.
          All functionality under it has been copied over to StripeClient.v1.tax_ids.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def tax_ids(self) -> "TaxIdService":
        return self.v1.tax_ids

    @property
    @deprecated(
        """
        StripeClient.tax_rates is deprecated, use StripeClient.v1.tax_rates instead.
          All functionality under it has been copied over to StripeClient.v1.tax_rates.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def tax_rates(self) -> "TaxRateService":
        return self.v1.tax_rates

    @property
    @deprecated(
        """
        StripeClient.terminal is deprecated, use StripeClient.v1.terminal instead.
          All functionality under it has been copied over to StripeClient.v1.terminal.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def terminal(self) -> "TerminalService":
        return self.v1.terminal

    @property
    @deprecated(
        """
        StripeClient.test_helpers is deprecated, use StripeClient.v1.test_helpers instead.
          All functionality under it has been copied over to StripeClient.v1.test_helpers.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def test_helpers(self) -> "TestHelpersService":
        return self.v1.test_helpers

    @property
    @deprecated(
        """
        StripeClient.tokens is deprecated, use StripeClient.v1.tokens instead.
          All functionality under it has been copied over to StripeClient.v1.tokens.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def tokens(self) -> "TokenService":
        return self.v1.tokens

    @property
    @deprecated(
        """
        StripeClient.topups is deprecated, use StripeClient.v1.topups instead.
          All functionality under it has been copied over to StripeClient.v1.topups.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def topups(self) -> "TopupService":
        return self.v1.topups

    @property
    @deprecated(
        """
        StripeClient.transfers is deprecated, use StripeClient.v1.transfers instead.
          All functionality under it has been copied over to StripeClient.v1.transfers.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def transfers(self) -> "TransferService":
        return self.v1.transfers

    @property
    @deprecated(
        """
        StripeClient.treasury is deprecated, use StripeClient.v1.treasury instead.
          All functionality under it has been copied over to StripeClient.v1.treasury.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def treasury(self) -> "TreasuryService":
        return self.v1.treasury

    @property
    @deprecated(
        """
        StripeClient.webhook_endpoints is deprecated, use StripeClient.v1.webhook_endpoints instead.
          All functionality under it has been copied over to StripeClient.v1.webhook_endpoints.
          See [migration guide](https://github.com/stripe/stripe-python/wiki/v1-namespace-in-StripeClient) for more on this and tips on migrating to the new v1 namespace.
        """,
    )
    def webhook_endpoints(self) -> "WebhookEndpointService":
        return self.v1.webhook_endpoints

    # deprecated v1 services: The end of the section generated from our OpenAPI spec
