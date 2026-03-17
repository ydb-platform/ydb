# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._deletable_api_resource import DeletableAPIResource
from stripe._expandable_field import ExpandableField
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
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
    from stripe._invoice import Invoice
    from stripe._payment_method import PaymentMethod
    from stripe._setup_intent import SetupIntent
    from stripe._source import Source
    from stripe._subscription_item import SubscriptionItem
    from stripe._subscription_schedule import SubscriptionSchedule
    from stripe._tax_id import TaxId
    from stripe._tax_rate import TaxRate
    from stripe.params._subscription_cancel_params import (
        SubscriptionCancelParams,
    )
    from stripe.params._subscription_create_params import (
        SubscriptionCreateParams,
    )
    from stripe.params._subscription_delete_discount_params import (
        SubscriptionDeleteDiscountParams,
    )
    from stripe.params._subscription_list_params import SubscriptionListParams
    from stripe.params._subscription_migrate_params import (
        SubscriptionMigrateParams,
    )
    from stripe.params._subscription_modify_params import (
        SubscriptionModifyParams,
    )
    from stripe.params._subscription_resume_params import (
        SubscriptionResumeParams,
    )
    from stripe.params._subscription_retrieve_params import (
        SubscriptionRetrieveParams,
    )
    from stripe.params._subscription_search_params import (
        SubscriptionSearchParams,
    )
    from stripe.test_helpers._test_clock import TestClock


class Subscription(
    CreateableAPIResource["Subscription"],
    DeletableAPIResource["Subscription"],
    ListableAPIResource["Subscription"],
    SearchableAPIResource["Subscription"],
    UpdateableAPIResource["Subscription"],
):
    """
    Subscriptions allow you to charge a customer on a recurring basis.

    Related guide: [Creating subscriptions](https://docs.stripe.com/billing/subscriptions/creating)
    """

    OBJECT_NAME: ClassVar[Literal["subscription"]] = "subscription"

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

        disabled_reason: Optional[Literal["requires_location_inputs"]]
        """
        If Stripe disabled automatic tax, this enum describes why.
        """
        enabled: bool
        """
        Whether Stripe automatically computes tax on this subscription.
        """
        liability: Optional[Liability]
        """
        The account that's liable for tax. If set, the business address and tax registrations required to perform the tax calculation are loaded from this account. The tax transaction is returned in the report of the connected account.
        """
        _inner_class_types = {"liability": Liability}

    class BillingCycleAnchorConfig(StripeObject):
        day_of_month: int
        """
        The day of the month of the billing_cycle_anchor.
        """
        hour: Optional[int]
        """
        The hour of the day of the billing_cycle_anchor.
        """
        minute: Optional[int]
        """
        The minute of the hour of the billing_cycle_anchor.
        """
        month: Optional[int]
        """
        The month to start full cycle billing periods.
        """
        second: Optional[int]
        """
        The second of the minute of the billing_cycle_anchor.
        """

    class BillingMode(StripeObject):
        class Flexible(StripeObject):
            proration_discounts: Optional[Literal["included", "itemized"]]
            """
            Controls how invoices and invoice items display proration amounts and discount amounts.
            """

        flexible: Optional[Flexible]
        """
        Configure behavior for flexible billing mode
        """
        type: Literal["classic", "flexible"]
        """
        Controls how prorations and invoices for subscriptions are calculated and orchestrated.
        """
        updated_at: Optional[int]
        """
        Details on when the current billing_mode was adopted.
        """
        _inner_class_types = {"flexible": Flexible}

    class BillingThresholds(StripeObject):
        amount_gte: Optional[int]
        """
        Monetary threshold that triggers the subscription to create an invoice
        """
        reset_billing_cycle_anchor: Optional[bool]
        """
        Indicates if the `billing_cycle_anchor` should be reset when a threshold is reached. If true, `billing_cycle_anchor` will be updated to the date/time the threshold was last reached; otherwise, the value will remain unchanged. This value may not be `true` if the subscription contains items with plans that have `aggregate_usage=last_ever`.
        """

    class CancellationDetails(StripeObject):
        comment: Optional[str]
        """
        Additional comments about why the user canceled the subscription, if the subscription was canceled explicitly by the user.
        """
        feedback: Optional[
            Literal[
                "customer_service",
                "low_quality",
                "missing_features",
                "other",
                "switched_service",
                "too_complex",
                "too_expensive",
                "unused",
            ]
        ]
        """
        The customer submitted reason for why they canceled, if the subscription was canceled explicitly by the user.
        """
        reason: Optional[
            Literal[
                "cancellation_requested", "payment_disputed", "payment_failed"
            ]
        ]
        """
        Why this subscription was canceled.
        """

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

        account_tax_ids: Optional[List[ExpandableField["TaxId"]]]
        """
        The account tax IDs associated with the subscription. Will be set on invoices generated by the subscription.
        """
        issuer: Issuer
        _inner_class_types = {"issuer": Issuer}

    class PauseCollection(StripeObject):
        behavior: Literal["keep_as_draft", "mark_uncollectible", "void"]
        """
        The payment collection behavior for this subscription while paused. One of `keep_as_draft`, `mark_uncollectible`, or `void`.
        """
        resumes_at: Optional[int]
        """
        The time after which the subscription will resume collecting payments.
        """

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
                class MandateOptions(StripeObject):
                    amount: Optional[int]
                    """
                    Amount to be charged for future payments.
                    """
                    amount_type: Optional[Literal["fixed", "maximum"]]
                    """
                    One of `fixed` or `maximum`. If `fixed`, the `amount` param refers to the exact amount to be charged in future payments. If `maximum`, the amount charged can be up to the value passed for the `amount` param.
                    """
                    description: Optional[str]
                    """
                    A description of the mandate or subscription that is meant to be displayed to the customer.
                    """

                mandate_options: Optional[MandateOptions]
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
                Selected network to process this Subscription on. Depends on the available networks of the card attached to the Subscription. Can be only set confirm-time.
                """
                request_three_d_secure: Optional[
                    Literal["any", "automatic", "challenge"]
                ]
                """
                We strongly recommend that you rely on our SCA Engine to automatically prompt your customers for authentication based on risk level and [other requirements](https://docs.stripe.com/strong-customer-authentication). However, if you wish to request 3D Secure based on logic from your own fraud engine, provide this option. Read our guide on [manually requesting 3D Secure](https://docs.stripe.com/payments/3d-secure/authentication-flow#manual-three-ds) for more information on how this configuration interacts with Radar and our SCA Engine.
                """
                _inner_class_types = {"mandate_options": MandateOptions}

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
            This sub-hash contains details about the Canadian pre-authorized debit payment method options to pass to invoices created by the subscription.
            """
            bancontact: Optional[Bancontact]
            """
            This sub-hash contains details about the Bancontact payment method options to pass to invoices created by the subscription.
            """
            card: Optional[Card]
            """
            This sub-hash contains details about the Card payment method options to pass to invoices created by the subscription.
            """
            customer_balance: Optional[CustomerBalance]
            """
            This sub-hash contains details about the Bank transfer payment method options to pass to invoices created by the subscription.
            """
            konbini: Optional[Konbini]
            """
            This sub-hash contains details about the Konbini payment method options to pass to invoices created by the subscription.
            """
            payto: Optional[Payto]
            """
            This sub-hash contains details about the PayTo payment method options to pass to invoices created by the subscription.
            """
            sepa_debit: Optional[SepaDebit]
            """
            This sub-hash contains details about the SEPA Direct Debit payment method options to pass to invoices created by the subscription.
            """
            us_bank_account: Optional[UsBankAccount]
            """
            This sub-hash contains details about the ACH direct debit payment method options to pass to invoices created by the subscription.
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

        payment_method_options: Optional[PaymentMethodOptions]
        """
        Payment-method-specific configuration to provide to invoices created by the subscription.
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
        The list of payment method types to provide to every invoice created by the subscription. If not set, Stripe attempts to automatically determine the types to use by looking at the invoice's default payment method, the subscription's default payment method, the customer's default payment method, and your [invoice template settings](https://dashboard.stripe.com/settings/billing/invoice).
        """
        save_default_payment_method: Optional[
            Literal["off", "on_subscription"]
        ]
        """
        Configure whether Stripe updates `subscription.default_payment_method` when payment succeeds. Defaults to `off`.
        """
        _inner_class_types = {"payment_method_options": PaymentMethodOptions}

    class PendingInvoiceItemInterval(StripeObject):
        interval: Literal["day", "month", "week", "year"]
        """
        Specifies invoicing frequency. Either `day`, `week`, `month` or `year`.
        """
        interval_count: int
        """
        The number of intervals between invoices. For example, `interval=month` and `interval_count=3` bills every 3 months. Maximum of one year interval allowed (1 year, 12 months, or 52 weeks).
        """

    class PendingUpdate(StripeObject):
        billing_cycle_anchor: Optional[int]
        """
        If the update is applied, determines the date of the first full invoice, and, for plans with `month` or `year` intervals, the day of the month for subsequent invoices. The timestamp is in UTC format.
        """
        expires_at: int
        """
        The point after which the changes reflected by this update will be discarded and no longer applied.
        """
        subscription_items: Optional[List["SubscriptionItem"]]
        """
        List of subscription items, each with an attached plan, that will be set if the update is applied.
        """
        trial_end: Optional[int]
        """
        Unix timestamp representing the end of the trial period the customer will get before being charged for the first time, if the update is applied.
        """
        trial_from_plan: Optional[bool]
        """
        Indicates if a plan's `trial_period_days` should be applied to the subscription. Setting `trial_end` per subscription is preferred, and this defaults to `false`. Setting this flag to `true` together with `trial_end` is not allowed. See [Using trial periods on subscriptions](https://docs.stripe.com/billing/subscriptions/trials) to learn more.
        """

    class TransferData(StripeObject):
        amount_percent: Optional[float]
        """
        A non-negative decimal between 0 and 100, with at most two decimal places. This represents the percentage of the subscription invoice total that will be transferred to the destination account. By default, the entire amount is transferred to the destination.
        """
        destination: ExpandableField["Account"]
        """
        The account where funds from the payment will be transferred to upon payment success.
        """

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
        Defines how a subscription behaves when a trial ends.
        """
        _inner_class_types = {"end_behavior": EndBehavior}

    application: Optional[ExpandableField["Application"]]
    """
    ID of the Connect Application that created the subscription.
    """
    application_fee_percent: Optional[float]
    """
    A non-negative decimal between 0 and 100, with at most two decimal places. This represents the percentage of the subscription invoice total that will be transferred to the application owner's Stripe account.
    """
    automatic_tax: AutomaticTax
    billing_cycle_anchor: int
    """
    The reference point that aligns future [billing cycle](https://docs.stripe.com/subscriptions/billing-cycle) dates. It sets the day of week for `week` intervals, the day of month for `month` and `year` intervals, and the month of year for `year` intervals. The timestamp is in UTC format.
    """
    billing_cycle_anchor_config: Optional[BillingCycleAnchorConfig]
    """
    The fixed values used to calculate the `billing_cycle_anchor`.
    """
    billing_mode: BillingMode
    """
    The billing mode of the subscription.
    """
    billing_thresholds: Optional[BillingThresholds]
    """
    Define thresholds at which an invoice will be sent, and the subscription advanced to a new billing period
    """
    cancel_at: Optional[int]
    """
    A date in the future at which the subscription will automatically get canceled
    """
    cancel_at_period_end: bool
    """
    Whether this subscription will (if `status=active`) or did (if `status=canceled`) cancel at the end of the current billing period.
    """
    canceled_at: Optional[int]
    """
    If the subscription has been canceled, the date of that cancellation. If the subscription was canceled with `cancel_at_period_end`, `canceled_at` will reflect the time of the most recent update request, not the end of the subscription period when the subscription is automatically moved to a canceled state.
    """
    cancellation_details: Optional[CancellationDetails]
    """
    Details about why this subscription was cancelled
    """
    collection_method: Literal["charge_automatically", "send_invoice"]
    """
    Either `charge_automatically`, or `send_invoice`. When charging automatically, Stripe will attempt to pay this subscription at the end of the cycle using the default source attached to the customer. When sending an invoice, Stripe will email your customer an invoice with payment instructions and mark the subscription as `active`.
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    customer: ExpandableField["Customer"]
    """
    ID of the customer who owns the subscription.
    """
    customer_account: Optional[str]
    """
    ID of the account representing the customer who owns the subscription.
    """
    days_until_due: Optional[int]
    """
    Number of days a customer has to pay invoices generated by this subscription. This value will be `null` for subscriptions where `collection_method=charge_automatically`.
    """
    default_payment_method: Optional[ExpandableField["PaymentMethod"]]
    """
    ID of the default payment method for the subscription. It must belong to the customer associated with the subscription. This takes precedence over `default_source`. If neither are set, invoices will use the customer's [invoice_settings.default_payment_method](https://docs.stripe.com/api/customers/object#customer_object-invoice_settings-default_payment_method) or [default_source](https://docs.stripe.com/api/customers/object#customer_object-default_source).
    """
    default_source: Optional[
        ExpandableField[
            Union["Account", "BankAccount", "CardResource", "Source"]
        ]
    ]
    """
    ID of the default payment source for the subscription. It must belong to the customer associated with the subscription and be in a chargeable state. If `default_payment_method` is also set, `default_payment_method` will take precedence. If neither are set, invoices will use the customer's [invoice_settings.default_payment_method](https://docs.stripe.com/api/customers/object#customer_object-invoice_settings-default_payment_method) or [default_source](https://docs.stripe.com/api/customers/object#customer_object-default_source).
    """
    default_tax_rates: Optional[List["TaxRate"]]
    """
    The tax rates that will apply to any subscription item that does not have `tax_rates` set. Invoices created will have their `default_tax_rates` populated from the subscription.
    """
    description: Optional[str]
    """
    The subscription's description, meant to be displayable to the customer. Use this field to optionally store an explanation of the subscription for rendering in Stripe surfaces and certain local payment methods UIs.
    """
    discounts: List[ExpandableField["Discount"]]
    """
    The discounts applied to the subscription. Subscription item discounts are applied before subscription discounts. Use `expand[]=discounts` to expand each discount.
    """
    ended_at: Optional[int]
    """
    If the subscription has ended, the date the subscription ended.
    """
    id: str
    """
    Unique identifier for the object.
    """
    invoice_settings: InvoiceSettings
    items: ListObject["SubscriptionItem"]
    """
    List of subscription items, each with an attached price.
    """
    latest_invoice: Optional[ExpandableField["Invoice"]]
    """
    The most recent invoice this subscription has generated over its lifecycle (for example, when it cycles or is updated).
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    metadata: Dict[str, str]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    next_pending_invoice_item_invoice: Optional[int]
    """
    Specifies the approximate timestamp on which any pending invoice items will be billed according to the schedule provided at `pending_invoice_item_interval`.
    """
    object: Literal["subscription"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    on_behalf_of: Optional[ExpandableField["Account"]]
    """
    The account (if any) the charge was made on behalf of for charges associated with this subscription. See the [Connect documentation](https://docs.stripe.com/connect/subscriptions#on-behalf-of) for details.
    """
    pause_collection: Optional[PauseCollection]
    """
    If specified, payment collection for this subscription will be paused. Note that the subscription status will be unchanged and will not be updated to `paused`. Learn more about [pausing collection](https://docs.stripe.com/billing/subscriptions/pause-payment).
    """
    payment_settings: Optional[PaymentSettings]
    """
    Payment settings passed on to invoices created by the subscription.
    """
    pending_invoice_item_interval: Optional[PendingInvoiceItemInterval]
    """
    Specifies an interval for how often to bill for any pending invoice items. It is analogous to calling [Create an invoice](https://docs.stripe.com/api#create_invoice) for the given subscription at the specified interval.
    """
    pending_setup_intent: Optional[ExpandableField["SetupIntent"]]
    """
    You can use this [SetupIntent](https://docs.stripe.com/api/setup_intents) to collect user authentication when creating a subscription without immediate payment or updating a subscription's payment method, allowing you to optimize for off-session payments. Learn more in the [SCA Migration Guide](https://docs.stripe.com/billing/migration/strong-customer-authentication#scenario-2).
    """
    pending_update: Optional[PendingUpdate]
    """
    If specified, [pending updates](https://docs.stripe.com/billing/subscriptions/pending-updates) that will be applied to the subscription once the `latest_invoice` has been paid.
    """
    schedule: Optional[ExpandableField["SubscriptionSchedule"]]
    """
    The schedule attached to the subscription
    """
    start_date: int
    """
    Date when the subscription was first created. The date might differ from the `created` date due to backdating.
    """
    status: Literal[
        "active",
        "canceled",
        "incomplete",
        "incomplete_expired",
        "past_due",
        "paused",
        "trialing",
        "unpaid",
    ]
    """
    Possible values are `incomplete`, `incomplete_expired`, `trialing`, `active`, `past_due`, `canceled`, `unpaid`, or `paused`.

    For `collection_method=charge_automatically` a subscription moves into `incomplete` if the initial payment attempt fails. A subscription in this status can only have metadata and default_source updated. Once the first invoice is paid, the subscription moves into an `active` status. If the first invoice is not paid within 23 hours, the subscription transitions to `incomplete_expired`. This is a terminal status, the open invoice will be voided and no further invoices will be generated.

    A subscription that is currently in a trial period is `trialing` and moves to `active` when the trial period is over.

    A subscription can only enter a `paused` status [when a trial ends without a payment method](https://docs.stripe.com/billing/subscriptions/trials#create-free-trials-without-payment). A `paused` subscription doesn't generate invoices and can be resumed after your customer adds their payment method. The `paused` status is different from [pausing collection](https://docs.stripe.com/billing/subscriptions/pause-payment), which still generates invoices and leaves the subscription's status unchanged.

    If subscription `collection_method=charge_automatically`, it becomes `past_due` when payment is required but cannot be paid (due to failed payment or awaiting additional user actions). Once Stripe has exhausted all payment retry attempts, the subscription will become `canceled` or `unpaid` (depending on your subscriptions settings).

    If subscription `collection_method=send_invoice` it becomes `past_due` when its invoice is not paid by the due date, and `canceled` or `unpaid` if it is still not paid by an additional deadline after that. Note that when a subscription has a status of `unpaid`, no subsequent invoices will be attempted (invoices will be created, but then immediately automatically closed). After receiving updated payment information from a customer, you may choose to reopen and pay their closed invoices.
    """
    test_clock: Optional[ExpandableField["TestClock"]]
    """
    ID of the test clock this subscription belongs to.
    """
    transfer_data: Optional[TransferData]
    """
    The account (if any) the subscription's payments will be attributed to for tax reporting, and where funds from each payment will be transferred to for each of the subscription's invoices.
    """
    trial_end: Optional[int]
    """
    If the subscription has a trial, the end of that trial.
    """
    trial_settings: Optional[TrialSettings]
    """
    Settings related to subscription trials.
    """
    trial_start: Optional[int]
    """
    If the subscription has a trial, the beginning of that trial.
    """

    @classmethod
    def _cls_cancel(
        cls,
        subscription_exposed_id: str,
        **params: Unpack["SubscriptionCancelParams"],
    ) -> "Subscription":
        """
        Cancels a customer's subscription immediately. The customer won't be charged again for the subscription. After it's canceled, you can no longer update the subscription or its [metadata](https://docs.stripe.com/metadata).

        Any pending invoice items that you've created are still charged at the end of the period, unless manually [deleted](https://docs.stripe.com/api#delete_invoiceitem). If you've set the subscription to cancel at the end of the period, any pending prorations are also left in place and collected at the end of the period. But if the subscription is set to cancel immediately, pending prorations are removed if invoice_now and prorate are both set to true.

        By default, upon subscription cancellation, Stripe stops automatic collection of all finalized invoices for the customer. This is intended to prevent unexpected payment attempts after the customer has canceled a subscription. However, you can resume automatic collection of the invoices manually after subscription cancellation to have us proceed. Or, you could check for unpaid invoices before allowing the customer to cancel the subscription at all.
        """
        return cast(
            "Subscription",
            cls._static_request(
                "delete",
                "/v1/subscriptions/{subscription_exposed_id}".format(
                    subscription_exposed_id=sanitize_id(
                        subscription_exposed_id
                    )
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def cancel(
        subscription_exposed_id: str,
        **params: Unpack["SubscriptionCancelParams"],
    ) -> "Subscription":
        """
        Cancels a customer's subscription immediately. The customer won't be charged again for the subscription. After it's canceled, you can no longer update the subscription or its [metadata](https://docs.stripe.com/metadata).

        Any pending invoice items that you've created are still charged at the end of the period, unless manually [deleted](https://docs.stripe.com/api#delete_invoiceitem). If you've set the subscription to cancel at the end of the period, any pending prorations are also left in place and collected at the end of the period. But if the subscription is set to cancel immediately, pending prorations are removed if invoice_now and prorate are both set to true.

        By default, upon subscription cancellation, Stripe stops automatic collection of all finalized invoices for the customer. This is intended to prevent unexpected payment attempts after the customer has canceled a subscription. However, you can resume automatic collection of the invoices manually after subscription cancellation to have us proceed. Or, you could check for unpaid invoices before allowing the customer to cancel the subscription at all.
        """
        ...

    @overload
    def cancel(
        self, **params: Unpack["SubscriptionCancelParams"]
    ) -> "Subscription":
        """
        Cancels a customer's subscription immediately. The customer won't be charged again for the subscription. After it's canceled, you can no longer update the subscription or its [metadata](https://docs.stripe.com/metadata).

        Any pending invoice items that you've created are still charged at the end of the period, unless manually [deleted](https://docs.stripe.com/api#delete_invoiceitem). If you've set the subscription to cancel at the end of the period, any pending prorations are also left in place and collected at the end of the period. But if the subscription is set to cancel immediately, pending prorations are removed if invoice_now and prorate are both set to true.

        By default, upon subscription cancellation, Stripe stops automatic collection of all finalized invoices for the customer. This is intended to prevent unexpected payment attempts after the customer has canceled a subscription. However, you can resume automatic collection of the invoices manually after subscription cancellation to have us proceed. Or, you could check for unpaid invoices before allowing the customer to cancel the subscription at all.
        """
        ...

    @class_method_variant("_cls_cancel")
    def cancel(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["SubscriptionCancelParams"]
    ) -> "Subscription":
        """
        Cancels a customer's subscription immediately. The customer won't be charged again for the subscription. After it's canceled, you can no longer update the subscription or its [metadata](https://docs.stripe.com/metadata).

        Any pending invoice items that you've created are still charged at the end of the period, unless manually [deleted](https://docs.stripe.com/api#delete_invoiceitem). If you've set the subscription to cancel at the end of the period, any pending prorations are also left in place and collected at the end of the period. But if the subscription is set to cancel immediately, pending prorations are removed if invoice_now and prorate are both set to true.

        By default, upon subscription cancellation, Stripe stops automatic collection of all finalized invoices for the customer. This is intended to prevent unexpected payment attempts after the customer has canceled a subscription. However, you can resume automatic collection of the invoices manually after subscription cancellation to have us proceed. Or, you could check for unpaid invoices before allowing the customer to cancel the subscription at all.
        """
        return cast(
            "Subscription",
            self._request(
                "delete",
                "/v1/subscriptions/{subscription_exposed_id}".format(
                    subscription_exposed_id=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_cancel_async(
        cls,
        subscription_exposed_id: str,
        **params: Unpack["SubscriptionCancelParams"],
    ) -> "Subscription":
        """
        Cancels a customer's subscription immediately. The customer won't be charged again for the subscription. After it's canceled, you can no longer update the subscription or its [metadata](https://docs.stripe.com/metadata).

        Any pending invoice items that you've created are still charged at the end of the period, unless manually [deleted](https://docs.stripe.com/api#delete_invoiceitem). If you've set the subscription to cancel at the end of the period, any pending prorations are also left in place and collected at the end of the period. But if the subscription is set to cancel immediately, pending prorations are removed if invoice_now and prorate are both set to true.

        By default, upon subscription cancellation, Stripe stops automatic collection of all finalized invoices for the customer. This is intended to prevent unexpected payment attempts after the customer has canceled a subscription. However, you can resume automatic collection of the invoices manually after subscription cancellation to have us proceed. Or, you could check for unpaid invoices before allowing the customer to cancel the subscription at all.
        """
        return cast(
            "Subscription",
            await cls._static_request_async(
                "delete",
                "/v1/subscriptions/{subscription_exposed_id}".format(
                    subscription_exposed_id=sanitize_id(
                        subscription_exposed_id
                    )
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def cancel_async(
        subscription_exposed_id: str,
        **params: Unpack["SubscriptionCancelParams"],
    ) -> "Subscription":
        """
        Cancels a customer's subscription immediately. The customer won't be charged again for the subscription. After it's canceled, you can no longer update the subscription or its [metadata](https://docs.stripe.com/metadata).

        Any pending invoice items that you've created are still charged at the end of the period, unless manually [deleted](https://docs.stripe.com/api#delete_invoiceitem). If you've set the subscription to cancel at the end of the period, any pending prorations are also left in place and collected at the end of the period. But if the subscription is set to cancel immediately, pending prorations are removed if invoice_now and prorate are both set to true.

        By default, upon subscription cancellation, Stripe stops automatic collection of all finalized invoices for the customer. This is intended to prevent unexpected payment attempts after the customer has canceled a subscription. However, you can resume automatic collection of the invoices manually after subscription cancellation to have us proceed. Or, you could check for unpaid invoices before allowing the customer to cancel the subscription at all.
        """
        ...

    @overload
    async def cancel_async(
        self, **params: Unpack["SubscriptionCancelParams"]
    ) -> "Subscription":
        """
        Cancels a customer's subscription immediately. The customer won't be charged again for the subscription. After it's canceled, you can no longer update the subscription or its [metadata](https://docs.stripe.com/metadata).

        Any pending invoice items that you've created are still charged at the end of the period, unless manually [deleted](https://docs.stripe.com/api#delete_invoiceitem). If you've set the subscription to cancel at the end of the period, any pending prorations are also left in place and collected at the end of the period. But if the subscription is set to cancel immediately, pending prorations are removed if invoice_now and prorate are both set to true.

        By default, upon subscription cancellation, Stripe stops automatic collection of all finalized invoices for the customer. This is intended to prevent unexpected payment attempts after the customer has canceled a subscription. However, you can resume automatic collection of the invoices manually after subscription cancellation to have us proceed. Or, you could check for unpaid invoices before allowing the customer to cancel the subscription at all.
        """
        ...

    @class_method_variant("_cls_cancel_async")
    async def cancel_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["SubscriptionCancelParams"]
    ) -> "Subscription":
        """
        Cancels a customer's subscription immediately. The customer won't be charged again for the subscription. After it's canceled, you can no longer update the subscription or its [metadata](https://docs.stripe.com/metadata).

        Any pending invoice items that you've created are still charged at the end of the period, unless manually [deleted](https://docs.stripe.com/api#delete_invoiceitem). If you've set the subscription to cancel at the end of the period, any pending prorations are also left in place and collected at the end of the period. But if the subscription is set to cancel immediately, pending prorations are removed if invoice_now and prorate are both set to true.

        By default, upon subscription cancellation, Stripe stops automatic collection of all finalized invoices for the customer. This is intended to prevent unexpected payment attempts after the customer has canceled a subscription. However, you can resume automatic collection of the invoices manually after subscription cancellation to have us proceed. Or, you could check for unpaid invoices before allowing the customer to cancel the subscription at all.
        """
        return cast(
            "Subscription",
            await self._request_async(
                "delete",
                "/v1/subscriptions/{subscription_exposed_id}".format(
                    subscription_exposed_id=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def create(
        cls, **params: Unpack["SubscriptionCreateParams"]
    ) -> "Subscription":
        """
        Creates a new subscription on an existing customer. Each customer can have up to 500 active or scheduled subscriptions.

        When you create a subscription with collection_method=charge_automatically, the first invoice is finalized as part of the request.
        The payment_behavior parameter determines the exact behavior of the initial payment.

        To start subscriptions where the first invoice always begins in a draft status, use [subscription schedules](https://docs.stripe.com/docs/billing/subscriptions/subscription-schedules#managing) instead.
        Schedules provide the flexibility to model more complex billing configurations that change over time.
        """
        return cast(
            "Subscription",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["SubscriptionCreateParams"]
    ) -> "Subscription":
        """
        Creates a new subscription on an existing customer. Each customer can have up to 500 active or scheduled subscriptions.

        When you create a subscription with collection_method=charge_automatically, the first invoice is finalized as part of the request.
        The payment_behavior parameter determines the exact behavior of the initial payment.

        To start subscriptions where the first invoice always begins in a draft status, use [subscription schedules](https://docs.stripe.com/docs/billing/subscriptions/subscription-schedules#managing) instead.
        Schedules provide the flexibility to model more complex billing configurations that change over time.
        """
        return cast(
            "Subscription",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def _cls_delete_discount(
        cls,
        subscription_exposed_id: str,
        **params: Unpack["SubscriptionDeleteDiscountParams"],
    ) -> "Discount":
        """
        Removes the currently applied discount on a subscription.
        """
        return cast(
            "Discount",
            cls._static_request(
                "delete",
                "/v1/subscriptions/{subscription_exposed_id}/discount".format(
                    subscription_exposed_id=sanitize_id(
                        subscription_exposed_id
                    )
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def delete_discount(
        subscription_exposed_id: str,
        **params: Unpack["SubscriptionDeleteDiscountParams"],
    ) -> "Discount":
        """
        Removes the currently applied discount on a subscription.
        """
        ...

    @overload
    def delete_discount(
        self, **params: Unpack["SubscriptionDeleteDiscountParams"]
    ) -> "Discount":
        """
        Removes the currently applied discount on a subscription.
        """
        ...

    @class_method_variant("_cls_delete_discount")
    def delete_discount(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["SubscriptionDeleteDiscountParams"]
    ) -> "Discount":
        """
        Removes the currently applied discount on a subscription.
        """
        return cast(
            "Discount",
            self._request(
                "delete",
                "/v1/subscriptions/{subscription_exposed_id}/discount".format(
                    subscription_exposed_id=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_delete_discount_async(
        cls,
        subscription_exposed_id: str,
        **params: Unpack["SubscriptionDeleteDiscountParams"],
    ) -> "Discount":
        """
        Removes the currently applied discount on a subscription.
        """
        return cast(
            "Discount",
            await cls._static_request_async(
                "delete",
                "/v1/subscriptions/{subscription_exposed_id}/discount".format(
                    subscription_exposed_id=sanitize_id(
                        subscription_exposed_id
                    )
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def delete_discount_async(
        subscription_exposed_id: str,
        **params: Unpack["SubscriptionDeleteDiscountParams"],
    ) -> "Discount":
        """
        Removes the currently applied discount on a subscription.
        """
        ...

    @overload
    async def delete_discount_async(
        self, **params: Unpack["SubscriptionDeleteDiscountParams"]
    ) -> "Discount":
        """
        Removes the currently applied discount on a subscription.
        """
        ...

    @class_method_variant("_cls_delete_discount_async")
    async def delete_discount_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["SubscriptionDeleteDiscountParams"]
    ) -> "Discount":
        """
        Removes the currently applied discount on a subscription.
        """
        return cast(
            "Discount",
            await self._request_async(
                "delete",
                "/v1/subscriptions/{subscription_exposed_id}/discount".format(
                    subscription_exposed_id=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def list(
        cls, **params: Unpack["SubscriptionListParams"]
    ) -> ListObject["Subscription"]:
        """
        By default, returns a list of subscriptions that have not been canceled. In order to list canceled subscriptions, specify status=canceled.
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
        cls, **params: Unpack["SubscriptionListParams"]
    ) -> ListObject["Subscription"]:
        """
        By default, returns a list of subscriptions that have not been canceled. In order to list canceled subscriptions, specify status=canceled.
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
    def _cls_migrate(
        cls, subscription: str, **params: Unpack["SubscriptionMigrateParams"]
    ) -> "Subscription":
        """
        Upgrade the billing_mode of an existing subscription.
        """
        return cast(
            "Subscription",
            cls._static_request(
                "post",
                "/v1/subscriptions/{subscription}/migrate".format(
                    subscription=sanitize_id(subscription)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def migrate(
        subscription: str, **params: Unpack["SubscriptionMigrateParams"]
    ) -> "Subscription":
        """
        Upgrade the billing_mode of an existing subscription.
        """
        ...

    @overload
    def migrate(
        self, **params: Unpack["SubscriptionMigrateParams"]
    ) -> "Subscription":
        """
        Upgrade the billing_mode of an existing subscription.
        """
        ...

    @class_method_variant("_cls_migrate")
    def migrate(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["SubscriptionMigrateParams"]
    ) -> "Subscription":
        """
        Upgrade the billing_mode of an existing subscription.
        """
        return cast(
            "Subscription",
            self._request(
                "post",
                "/v1/subscriptions/{subscription}/migrate".format(
                    subscription=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_migrate_async(
        cls, subscription: str, **params: Unpack["SubscriptionMigrateParams"]
    ) -> "Subscription":
        """
        Upgrade the billing_mode of an existing subscription.
        """
        return cast(
            "Subscription",
            await cls._static_request_async(
                "post",
                "/v1/subscriptions/{subscription}/migrate".format(
                    subscription=sanitize_id(subscription)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def migrate_async(
        subscription: str, **params: Unpack["SubscriptionMigrateParams"]
    ) -> "Subscription":
        """
        Upgrade the billing_mode of an existing subscription.
        """
        ...

    @overload
    async def migrate_async(
        self, **params: Unpack["SubscriptionMigrateParams"]
    ) -> "Subscription":
        """
        Upgrade the billing_mode of an existing subscription.
        """
        ...

    @class_method_variant("_cls_migrate_async")
    async def migrate_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["SubscriptionMigrateParams"]
    ) -> "Subscription":
        """
        Upgrade the billing_mode of an existing subscription.
        """
        return cast(
            "Subscription",
            await self._request_async(
                "post",
                "/v1/subscriptions/{subscription}/migrate".format(
                    subscription=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def modify(
        cls, id: str, **params: Unpack["SubscriptionModifyParams"]
    ) -> "Subscription":
        """
        Updates an existing subscription to match the specified parameters.
        When changing prices or quantities, we optionally prorate the price we charge next month to make up for any price changes.
        To preview how the proration is calculated, use the [create preview](https://docs.stripe.com/docs/api/invoices/create_preview) endpoint.

        By default, we prorate subscription changes. For example, if a customer signs up on May 1 for a 100 price, they'll be billed 100 immediately. If on May 15 they switch to a 200 price, then on June 1 they'll be billed 250 (200 for a renewal of her subscription, plus a 50 prorating adjustment for half of the previous month's 100 difference). Similarly, a downgrade generates a credit that is applied to the next invoice. We also prorate when you make quantity changes.

        Switching prices does not normally change the billing date or generate an immediate charge unless:


        The billing interval is changed (for example, from monthly to yearly).
        The subscription moves from free to paid.
        A trial starts or ends.


        In these cases, we apply a credit for the unused time on the previous price, immediately charge the customer using the new price, and reset the billing date. Learn about how [Stripe immediately attempts payment for subscription changes](https://docs.stripe.com/docs/billing/subscriptions/upgrade-downgrade#immediate-payment).

        If you want to charge for an upgrade immediately, pass proration_behavior as always_invoice to create prorations, automatically invoice the customer for those proration adjustments, and attempt to collect payment. If you pass create_prorations, the prorations are created but not automatically invoiced. If you want to bill the customer for the prorations before the subscription's renewal date, you need to manually [invoice the customer](https://docs.stripe.com/docs/api/invoices/create).

        If you don't want to prorate, set the proration_behavior option to none. With this option, the customer is billed 100 on May 1 and 200 on June 1. Similarly, if you set proration_behavior to none when switching between different billing intervals (for example, from monthly to yearly), we don't generate any credits for the old subscription's unused time. We still reset the billing date and bill immediately for the new subscription.

        Updating the quantity on a subscription many times in an hour may result in [rate limiting. If you need to bill for a frequently changing quantity, consider integrating <a href="/docs/billing/subscriptions/usage-based">usage-based billing](https://docs.stripe.com/docs/rate-limits) instead.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Subscription",
            cls._static_request(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    async def modify_async(
        cls, id: str, **params: Unpack["SubscriptionModifyParams"]
    ) -> "Subscription":
        """
        Updates an existing subscription to match the specified parameters.
        When changing prices or quantities, we optionally prorate the price we charge next month to make up for any price changes.
        To preview how the proration is calculated, use the [create preview](https://docs.stripe.com/docs/api/invoices/create_preview) endpoint.

        By default, we prorate subscription changes. For example, if a customer signs up on May 1 for a 100 price, they'll be billed 100 immediately. If on May 15 they switch to a 200 price, then on June 1 they'll be billed 250 (200 for a renewal of her subscription, plus a 50 prorating adjustment for half of the previous month's 100 difference). Similarly, a downgrade generates a credit that is applied to the next invoice. We also prorate when you make quantity changes.

        Switching prices does not normally change the billing date or generate an immediate charge unless:


        The billing interval is changed (for example, from monthly to yearly).
        The subscription moves from free to paid.
        A trial starts or ends.


        In these cases, we apply a credit for the unused time on the previous price, immediately charge the customer using the new price, and reset the billing date. Learn about how [Stripe immediately attempts payment for subscription changes](https://docs.stripe.com/docs/billing/subscriptions/upgrade-downgrade#immediate-payment).

        If you want to charge for an upgrade immediately, pass proration_behavior as always_invoice to create prorations, automatically invoice the customer for those proration adjustments, and attempt to collect payment. If you pass create_prorations, the prorations are created but not automatically invoiced. If you want to bill the customer for the prorations before the subscription's renewal date, you need to manually [invoice the customer](https://docs.stripe.com/docs/api/invoices/create).

        If you don't want to prorate, set the proration_behavior option to none. With this option, the customer is billed 100 on May 1 and 200 on June 1. Similarly, if you set proration_behavior to none when switching between different billing intervals (for example, from monthly to yearly), we don't generate any credits for the old subscription's unused time. We still reset the billing date and bill immediately for the new subscription.

        Updating the quantity on a subscription many times in an hour may result in [rate limiting. If you need to bill for a frequently changing quantity, consider integrating <a href="/docs/billing/subscriptions/usage-based">usage-based billing](https://docs.stripe.com/docs/rate-limits) instead.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Subscription",
            await cls._static_request_async(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    def _cls_resume(
        cls, subscription: str, **params: Unpack["SubscriptionResumeParams"]
    ) -> "Subscription":
        """
        Initiates resumption of a paused subscription, optionally resetting the billing cycle anchor and creating prorations. If no resumption invoice is generated, the subscription becomes active immediately. If a resumption invoice is generated, the subscription remains paused until the invoice is paid or marked uncollectible. If the invoice is not paid by the expiration date, it is voided and the subscription remains paused.
        """
        return cast(
            "Subscription",
            cls._static_request(
                "post",
                "/v1/subscriptions/{subscription}/resume".format(
                    subscription=sanitize_id(subscription)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def resume(
        subscription: str, **params: Unpack["SubscriptionResumeParams"]
    ) -> "Subscription":
        """
        Initiates resumption of a paused subscription, optionally resetting the billing cycle anchor and creating prorations. If no resumption invoice is generated, the subscription becomes active immediately. If a resumption invoice is generated, the subscription remains paused until the invoice is paid or marked uncollectible. If the invoice is not paid by the expiration date, it is voided and the subscription remains paused.
        """
        ...

    @overload
    def resume(
        self, **params: Unpack["SubscriptionResumeParams"]
    ) -> "Subscription":
        """
        Initiates resumption of a paused subscription, optionally resetting the billing cycle anchor and creating prorations. If no resumption invoice is generated, the subscription becomes active immediately. If a resumption invoice is generated, the subscription remains paused until the invoice is paid or marked uncollectible. If the invoice is not paid by the expiration date, it is voided and the subscription remains paused.
        """
        ...

    @class_method_variant("_cls_resume")
    def resume(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["SubscriptionResumeParams"]
    ) -> "Subscription":
        """
        Initiates resumption of a paused subscription, optionally resetting the billing cycle anchor and creating prorations. If no resumption invoice is generated, the subscription becomes active immediately. If a resumption invoice is generated, the subscription remains paused until the invoice is paid or marked uncollectible. If the invoice is not paid by the expiration date, it is voided and the subscription remains paused.
        """
        return cast(
            "Subscription",
            self._request(
                "post",
                "/v1/subscriptions/{subscription}/resume".format(
                    subscription=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_resume_async(
        cls, subscription: str, **params: Unpack["SubscriptionResumeParams"]
    ) -> "Subscription":
        """
        Initiates resumption of a paused subscription, optionally resetting the billing cycle anchor and creating prorations. If no resumption invoice is generated, the subscription becomes active immediately. If a resumption invoice is generated, the subscription remains paused until the invoice is paid or marked uncollectible. If the invoice is not paid by the expiration date, it is voided and the subscription remains paused.
        """
        return cast(
            "Subscription",
            await cls._static_request_async(
                "post",
                "/v1/subscriptions/{subscription}/resume".format(
                    subscription=sanitize_id(subscription)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def resume_async(
        subscription: str, **params: Unpack["SubscriptionResumeParams"]
    ) -> "Subscription":
        """
        Initiates resumption of a paused subscription, optionally resetting the billing cycle anchor and creating prorations. If no resumption invoice is generated, the subscription becomes active immediately. If a resumption invoice is generated, the subscription remains paused until the invoice is paid or marked uncollectible. If the invoice is not paid by the expiration date, it is voided and the subscription remains paused.
        """
        ...

    @overload
    async def resume_async(
        self, **params: Unpack["SubscriptionResumeParams"]
    ) -> "Subscription":
        """
        Initiates resumption of a paused subscription, optionally resetting the billing cycle anchor and creating prorations. If no resumption invoice is generated, the subscription becomes active immediately. If a resumption invoice is generated, the subscription remains paused until the invoice is paid or marked uncollectible. If the invoice is not paid by the expiration date, it is voided and the subscription remains paused.
        """
        ...

    @class_method_variant("_cls_resume_async")
    async def resume_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["SubscriptionResumeParams"]
    ) -> "Subscription":
        """
        Initiates resumption of a paused subscription, optionally resetting the billing cycle anchor and creating prorations. If no resumption invoice is generated, the subscription becomes active immediately. If a resumption invoice is generated, the subscription remains paused until the invoice is paid or marked uncollectible. If the invoice is not paid by the expiration date, it is voided and the subscription remains paused.
        """
        return cast(
            "Subscription",
            await self._request_async(
                "post",
                "/v1/subscriptions/{subscription}/resume".format(
                    subscription=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["SubscriptionRetrieveParams"]
    ) -> "Subscription":
        """
        Retrieves the subscription with the given ID.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["SubscriptionRetrieveParams"]
    ) -> "Subscription":
        """
        Retrieves the subscription with the given ID.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    @classmethod
    def search(
        cls, *args, **kwargs: Unpack["SubscriptionSearchParams"]
    ) -> SearchResultObject["Subscription"]:
        """
        Search for subscriptions you've previously created using Stripe's [Search Query Language](https://docs.stripe.com/docs/search#search-query-language).
        Don't use search in read-after-write flows where strict consistency is necessary. Under normal operating
        conditions, data is searchable in less than a minute. Occasionally, propagation of new or updated data can be up
        to an hour behind during outages. Search functionality is not available to merchants in India.
        """
        return cls._search(
            search_url="/v1/subscriptions/search", *args, **kwargs
        )

    @classmethod
    async def search_async(
        cls, *args, **kwargs: Unpack["SubscriptionSearchParams"]
    ) -> SearchResultObject["Subscription"]:
        """
        Search for subscriptions you've previously created using Stripe's [Search Query Language](https://docs.stripe.com/docs/search#search-query-language).
        Don't use search in read-after-write flows where strict consistency is necessary. Under normal operating
        conditions, data is searchable in less than a minute. Occasionally, propagation of new or updated data can be up
        to an hour behind during outages. Search functionality is not available to merchants in India.
        """
        return await cls._search_async(
            search_url="/v1/subscriptions/search", *args, **kwargs
        )

    @classmethod
    def search_auto_paging_iter(
        cls, *args, **kwargs: Unpack["SubscriptionSearchParams"]
    ) -> Iterator["Subscription"]:
        return cls.search(*args, **kwargs).auto_paging_iter()

    @classmethod
    async def search_auto_paging_iter_async(
        cls, *args, **kwargs: Unpack["SubscriptionSearchParams"]
    ) -> AsyncIterator["Subscription"]:
        return (await cls.search_async(*args, **kwargs)).auto_paging_iter()

    _inner_class_types = {
        "automatic_tax": AutomaticTax,
        "billing_cycle_anchor_config": BillingCycleAnchorConfig,
        "billing_mode": BillingMode,
        "billing_thresholds": BillingThresholds,
        "cancellation_details": CancellationDetails,
        "invoice_settings": InvoiceSettings,
        "pause_collection": PauseCollection,
        "payment_settings": PaymentSettings,
        "pending_invoice_item_interval": PendingInvoiceItemInterval,
        "pending_update": PendingUpdate,
        "transfer_data": TransferData,
        "trial_settings": TrialSettings,
    }
