# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class SubscriptionScheduleCreateParams(RequestOptions):
    billing_mode: NotRequired["SubscriptionScheduleCreateParamsBillingMode"]
    """
    Controls how prorations and invoices for subscriptions are calculated and orchestrated.
    """
    customer: NotRequired[str]
    """
    The identifier of the customer to create the subscription schedule for.
    """
    customer_account: NotRequired[str]
    """
    The identifier of the account to create the subscription schedule for.
    """
    default_settings: NotRequired[
        "SubscriptionScheduleCreateParamsDefaultSettings"
    ]
    """
    Object representing the subscription schedule's default settings.
    """
    end_behavior: NotRequired[Literal["cancel", "none", "release", "renew"]]
    """
    Behavior of the subscription schedule and underlying subscription when it ends. Possible values are `release` or `cancel` with the default being `release`. `release` will end the subscription schedule and keep the underlying subscription running. `cancel` will end the subscription schedule and cancel the underlying subscription.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    from_subscription: NotRequired[str]
    """
    Migrate an existing subscription to be managed by a subscription schedule. If this parameter is set, a subscription schedule will be created using the subscription's item(s), set to auto-renew using the subscription's interval. When using this parameter, other parameters (such as phase values) cannot be set. To create a subscription schedule with other modifications, we recommend making two separate API calls.
    """
    metadata: NotRequired["Literal['']|Dict[str, str]"]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    phases: NotRequired[List["SubscriptionScheduleCreateParamsPhase"]]
    """
    List representing phases of the subscription schedule. Each phase can be customized to have different durations, plans, and coupons. If there are multiple phases, the `end_date` of one phase will always equal the `start_date` of the next phase.
    """
    start_date: NotRequired["int|Literal['now']"]
    """
    When the subscription schedule starts. We recommend using `now` so that it starts the subscription immediately. You can also use a Unix timestamp to backdate the subscription so that it starts on a past date, or set a future date for the subscription to start on.
    """


class SubscriptionScheduleCreateParamsBillingMode(TypedDict):
    flexible: NotRequired[
        "SubscriptionScheduleCreateParamsBillingModeFlexible"
    ]
    """
    Configure behavior for flexible billing mode.
    """
    type: Literal["classic", "flexible"]
    """
    Controls the calculation and orchestration of prorations and invoices for subscriptions. If no value is passed, the default is `flexible`.
    """


class SubscriptionScheduleCreateParamsBillingModeFlexible(TypedDict):
    proration_discounts: NotRequired[Literal["included", "itemized"]]
    """
    Controls how invoices and invoice items display proration amounts and discount amounts.
    """


class SubscriptionScheduleCreateParamsDefaultSettings(TypedDict):
    application_fee_percent: NotRequired[float]
    """
    A non-negative decimal between 0 and 100, with at most two decimal places. This represents the percentage of the subscription invoice total that will be transferred to the application owner's Stripe account. The request must be made by a platform account on a connected account in order to set an application fee percentage. For more information, see the application fees [documentation](https://stripe.com/docs/connect/subscriptions#collecting-fees-on-subscriptions).
    """
    automatic_tax: NotRequired[
        "SubscriptionScheduleCreateParamsDefaultSettingsAutomaticTax"
    ]
    """
    Default settings for automatic tax computation.
    """
    billing_cycle_anchor: NotRequired[Literal["automatic", "phase_start"]]
    """
    Can be set to `phase_start` to set the anchor to the start of the phase or `automatic` to automatically change it if needed. Cannot be set to `phase_start` if this phase specifies a trial. For more information, see the billing cycle [documentation](https://docs.stripe.com/billing/subscriptions/billing-cycle).
    """
    billing_thresholds: NotRequired[
        "Literal['']|SubscriptionScheduleCreateParamsDefaultSettingsBillingThresholds"
    ]
    """
    Define thresholds at which an invoice will be sent, and the subscription advanced to a new billing period. Pass an empty string to remove previously-defined thresholds.
    """
    collection_method: NotRequired[
        Literal["charge_automatically", "send_invoice"]
    ]
    """
    Either `charge_automatically`, or `send_invoice`. When charging automatically, Stripe will attempt to pay the underlying subscription at the end of each billing cycle using the default source attached to the customer. When sending an invoice, Stripe will email your customer an invoice with payment instructions and mark the subscription as `active`. Defaults to `charge_automatically` on creation.
    """
    default_payment_method: NotRequired[str]
    """
    ID of the default payment method for the subscription schedule. It must belong to the customer associated with the subscription schedule. If not set, invoices will use the default payment method in the customer's invoice settings.
    """
    description: NotRequired["Literal['']|str"]
    """
    Subscription description, meant to be displayable to the customer. Use this field to optionally store an explanation of the subscription for rendering in Stripe surfaces and certain local payment methods UIs.
    """
    invoice_settings: NotRequired[
        "SubscriptionScheduleCreateParamsDefaultSettingsInvoiceSettings"
    ]
    """
    All invoices will be billed using the specified settings.
    """
    on_behalf_of: NotRequired["Literal['']|str"]
    """
    The account on behalf of which to charge, for each of the associated subscription's invoices.
    """
    transfer_data: NotRequired[
        "Literal['']|SubscriptionScheduleCreateParamsDefaultSettingsTransferData"
    ]
    """
    The data with which to automatically create a Transfer for each of the associated subscription's invoices.
    """


class SubscriptionScheduleCreateParamsDefaultSettingsAutomaticTax(TypedDict):
    enabled: bool
    """
    Enabled automatic tax calculation which will automatically compute tax rates on all invoices generated by the subscription.
    """
    liability: NotRequired[
        "SubscriptionScheduleCreateParamsDefaultSettingsAutomaticTaxLiability"
    ]
    """
    The account that's liable for tax. If set, the business address and tax registrations required to perform the tax calculation are loaded from this account. The tax transaction is returned in the report of the connected account.
    """


class SubscriptionScheduleCreateParamsDefaultSettingsAutomaticTaxLiability(
    TypedDict,
):
    account: NotRequired[str]
    """
    The connected account being referenced when `type` is `account`.
    """
    type: Literal["account", "self"]
    """
    Type of the account referenced in the request.
    """


class SubscriptionScheduleCreateParamsDefaultSettingsBillingThresholds(
    TypedDict,
):
    amount_gte: NotRequired[int]
    """
    Monetary threshold that triggers the subscription to advance to a new billing period
    """
    reset_billing_cycle_anchor: NotRequired[bool]
    """
    Indicates if the `billing_cycle_anchor` should be reset when a threshold is reached. If true, `billing_cycle_anchor` will be updated to the date/time the threshold was last reached; otherwise, the value will remain unchanged.
    """


class SubscriptionScheduleCreateParamsDefaultSettingsInvoiceSettings(
    TypedDict
):
    account_tax_ids: NotRequired["Literal['']|List[str]"]
    """
    The account tax IDs associated with the subscription schedule. Will be set on invoices generated by the subscription schedule.
    """
    days_until_due: NotRequired[int]
    """
    Number of days within which a customer must pay invoices generated by this subscription schedule. This value will be `null` for subscription schedules where `collection_method=charge_automatically`.
    """
    issuer: NotRequired[
        "SubscriptionScheduleCreateParamsDefaultSettingsInvoiceSettingsIssuer"
    ]
    """
    The connected account that issues the invoice. The invoice is presented with the branding and support information of the specified account.
    """


class SubscriptionScheduleCreateParamsDefaultSettingsInvoiceSettingsIssuer(
    TypedDict,
):
    account: NotRequired[str]
    """
    The connected account being referenced when `type` is `account`.
    """
    type: Literal["account", "self"]
    """
    Type of the account referenced in the request.
    """


class SubscriptionScheduleCreateParamsDefaultSettingsTransferData(TypedDict):
    amount_percent: NotRequired[float]
    """
    A non-negative decimal between 0 and 100, with at most two decimal places. This represents the percentage of the subscription invoice total that will be transferred to the destination account. By default, the entire amount is transferred to the destination.
    """
    destination: str
    """
    ID of an existing, connected Stripe account.
    """


class SubscriptionScheduleCreateParamsPhase(TypedDict):
    add_invoice_items: NotRequired[
        List["SubscriptionScheduleCreateParamsPhaseAddInvoiceItem"]
    ]
    """
    A list of prices and quantities that will generate invoice items appended to the next invoice for this phase. You may pass up to 20 items.
    """
    application_fee_percent: NotRequired[float]
    """
    A non-negative decimal between 0 and 100, with at most two decimal places. This represents the percentage of the subscription invoice total that will be transferred to the application owner's Stripe account. The request must be made by a platform account on a connected account in order to set an application fee percentage. For more information, see the application fees [documentation](https://stripe.com/docs/connect/subscriptions#collecting-fees-on-subscriptions).
    """
    automatic_tax: NotRequired[
        "SubscriptionScheduleCreateParamsPhaseAutomaticTax"
    ]
    """
    Automatic tax settings for this phase.
    """
    billing_cycle_anchor: NotRequired[Literal["automatic", "phase_start"]]
    """
    Can be set to `phase_start` to set the anchor to the start of the phase or `automatic` to automatically change it if needed. Cannot be set to `phase_start` if this phase specifies a trial. For more information, see the billing cycle [documentation](https://docs.stripe.com/billing/subscriptions/billing-cycle).
    """
    billing_thresholds: NotRequired[
        "Literal['']|SubscriptionScheduleCreateParamsPhaseBillingThresholds"
    ]
    """
    Define thresholds at which an invoice will be sent, and the subscription advanced to a new billing period. Pass an empty string to remove previously-defined thresholds.
    """
    collection_method: NotRequired[
        Literal["charge_automatically", "send_invoice"]
    ]
    """
    Either `charge_automatically`, or `send_invoice`. When charging automatically, Stripe will attempt to pay the underlying subscription at the end of each billing cycle using the default source attached to the customer. When sending an invoice, Stripe will email your customer an invoice with payment instructions and mark the subscription as `active`. Defaults to `charge_automatically` on creation.
    """
    currency: NotRequired[str]
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    default_payment_method: NotRequired[str]
    """
    ID of the default payment method for the subscription schedule. It must belong to the customer associated with the subscription schedule. If not set, invoices will use the default payment method in the customer's invoice settings.
    """
    default_tax_rates: NotRequired["Literal['']|List[str]"]
    """
    A list of [Tax Rate](https://docs.stripe.com/api/tax_rates) ids. These Tax Rates will set the Subscription's [`default_tax_rates`](https://docs.stripe.com/api/subscriptions/create#create_subscription-default_tax_rates), which means they will be the Invoice's [`default_tax_rates`](https://docs.stripe.com/api/invoices/create#create_invoice-default_tax_rates) for any Invoices issued by the Subscription during this Phase.
    """
    description: NotRequired["Literal['']|str"]
    """
    Subscription description, meant to be displayable to the customer. Use this field to optionally store an explanation of the subscription for rendering in Stripe surfaces and certain local payment methods UIs.
    """
    discounts: NotRequired[
        "Literal['']|List[SubscriptionScheduleCreateParamsPhaseDiscount]"
    ]
    """
    The coupons to redeem into discounts for the schedule phase. If not specified, inherits the discount from the subscription's customer. Pass an empty string to avoid inheriting any discounts.
    """
    duration: NotRequired["SubscriptionScheduleCreateParamsPhaseDuration"]
    """
    The number of intervals the phase should last. If set, `end_date` must not be set.
    """
    end_date: NotRequired[int]
    """
    The date at which this phase of the subscription schedule ends. If set, `duration` must not be set.
    """
    invoice_settings: NotRequired[
        "SubscriptionScheduleCreateParamsPhaseInvoiceSettings"
    ]
    """
    All invoices will be billed using the specified settings.
    """
    items: List["SubscriptionScheduleCreateParamsPhaseItem"]
    """
    List of configuration items, each with an attached price, to apply during this phase of the subscription schedule.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to a phase. Metadata on a schedule's phase will update the underlying subscription's `metadata` when the phase is entered, adding new keys and replacing existing keys in the subscription's `metadata`. Individual keys in the subscription's `metadata` can be unset by posting an empty value to them in the phase's `metadata`. To unset all keys in the subscription's `metadata`, update the subscription directly or unset every key individually from the phase's `metadata`.
    """
    on_behalf_of: NotRequired[str]
    """
    The account on behalf of which to charge, for each of the associated subscription's invoices.
    """
    proration_behavior: NotRequired[
        Literal["always_invoice", "create_prorations", "none"]
    ]
    """
    Controls whether the subscription schedule should create [prorations](https://docs.stripe.com/billing/subscriptions/prorations) when transitioning to this phase if there is a difference in billing configuration. It's different from the request-level [proration_behavior](https://docs.stripe.com/api/subscription_schedules/update#update_subscription_schedule-proration_behavior) parameter which controls what happens if the update request affects the billing configuration (item price, quantity, etc.) of the current phase.
    """
    transfer_data: NotRequired[
        "SubscriptionScheduleCreateParamsPhaseTransferData"
    ]
    """
    The data with which to automatically create a Transfer for each of the associated subscription's invoices.
    """
    trial: NotRequired[bool]
    """
    If set to true the entire phase is counted as a trial and the customer will not be charged for any fees.
    """
    trial_end: NotRequired[int]
    """
    Sets the phase to trialing from the start date to this date. Must be before the phase end date, can not be combined with `trial`
    """


class SubscriptionScheduleCreateParamsPhaseAddInvoiceItem(TypedDict):
    discounts: NotRequired[
        List["SubscriptionScheduleCreateParamsPhaseAddInvoiceItemDiscount"]
    ]
    """
    The coupons to redeem into discounts for the item.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    period: NotRequired[
        "SubscriptionScheduleCreateParamsPhaseAddInvoiceItemPeriod"
    ]
    """
    The period associated with this invoice item. If not set, `period.start.type` defaults to `max_item_period_start` and `period.end.type` defaults to `min_item_period_end`.
    """
    price: NotRequired[str]
    """
    The ID of the price object. One of `price` or `price_data` is required.
    """
    price_data: NotRequired[
        "SubscriptionScheduleCreateParamsPhaseAddInvoiceItemPriceData"
    ]
    """
    Data used to generate a new [Price](https://docs.stripe.com/api/prices) object inline. One of `price` or `price_data` is required.
    """
    quantity: NotRequired[int]
    """
    Quantity for this item. Defaults to 1.
    """
    tax_rates: NotRequired["Literal['']|List[str]"]
    """
    The tax rates which apply to the item. When set, the `default_tax_rates` do not apply to this item.
    """


class SubscriptionScheduleCreateParamsPhaseAddInvoiceItemDiscount(TypedDict):
    coupon: NotRequired[str]
    """
    ID of the coupon to create a new discount for.
    """
    discount: NotRequired[str]
    """
    ID of an existing discount on the object (or one of its ancestors) to reuse.
    """
    promotion_code: NotRequired[str]
    """
    ID of the promotion code to create a new discount for.
    """


class SubscriptionScheduleCreateParamsPhaseAddInvoiceItemPeriod(TypedDict):
    end: "SubscriptionScheduleCreateParamsPhaseAddInvoiceItemPeriodEnd"
    """
    End of the invoice item period.
    """
    start: "SubscriptionScheduleCreateParamsPhaseAddInvoiceItemPeriodStart"
    """
    Start of the invoice item period.
    """


class SubscriptionScheduleCreateParamsPhaseAddInvoiceItemPeriodEnd(TypedDict):
    timestamp: NotRequired[int]
    """
    A precise Unix timestamp for the end of the invoice item period. Must be greater than or equal to `period.start`.
    """
    type: Literal["min_item_period_end", "phase_end", "timestamp"]
    """
    Select how to calculate the end of the invoice item period.
    """


class SubscriptionScheduleCreateParamsPhaseAddInvoiceItemPeriodStart(
    TypedDict
):
    timestamp: NotRequired[int]
    """
    A precise Unix timestamp for the start of the invoice item period. Must be less than or equal to `period.end`.
    """
    type: Literal["max_item_period_start", "phase_start", "timestamp"]
    """
    Select how to calculate the start of the invoice item period.
    """


class SubscriptionScheduleCreateParamsPhaseAddInvoiceItemPriceData(TypedDict):
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    product: str
    """
    The ID of the [Product](https://docs.stripe.com/api/products) that this [Price](https://docs.stripe.com/api/prices) will belong to.
    """
    tax_behavior: NotRequired[Literal["exclusive", "inclusive", "unspecified"]]
    """
    Only required if a [default tax behavior](https://docs.stripe.com/tax/products-prices-tax-categories-tax-behavior#setting-a-default-tax-behavior-(recommended)) was not provided in the Stripe Tax settings. Specifies whether the price is considered inclusive of taxes or exclusive of taxes. One of `inclusive`, `exclusive`, or `unspecified`. Once specified as either `inclusive` or `exclusive`, it cannot be changed.
    """
    unit_amount: NotRequired[int]
    """
    A positive integer in cents (or local equivalent) (or 0 for a free price) representing how much to charge or a negative integer representing the amount to credit to the customer.
    """
    unit_amount_decimal: NotRequired[str]
    """
    Same as `unit_amount`, but accepts a decimal value in cents (or local equivalent) with at most 12 decimal places. Only one of `unit_amount` and `unit_amount_decimal` can be set.
    """


class SubscriptionScheduleCreateParamsPhaseAutomaticTax(TypedDict):
    enabled: bool
    """
    Enabled automatic tax calculation which will automatically compute tax rates on all invoices generated by the subscription.
    """
    liability: NotRequired[
        "SubscriptionScheduleCreateParamsPhaseAutomaticTaxLiability"
    ]
    """
    The account that's liable for tax. If set, the business address and tax registrations required to perform the tax calculation are loaded from this account. The tax transaction is returned in the report of the connected account.
    """


class SubscriptionScheduleCreateParamsPhaseAutomaticTaxLiability(TypedDict):
    account: NotRequired[str]
    """
    The connected account being referenced when `type` is `account`.
    """
    type: Literal["account", "self"]
    """
    Type of the account referenced in the request.
    """


class SubscriptionScheduleCreateParamsPhaseBillingThresholds(TypedDict):
    amount_gte: NotRequired[int]
    """
    Monetary threshold that triggers the subscription to advance to a new billing period
    """
    reset_billing_cycle_anchor: NotRequired[bool]
    """
    Indicates if the `billing_cycle_anchor` should be reset when a threshold is reached. If true, `billing_cycle_anchor` will be updated to the date/time the threshold was last reached; otherwise, the value will remain unchanged.
    """


class SubscriptionScheduleCreateParamsPhaseDiscount(TypedDict):
    coupon: NotRequired[str]
    """
    ID of the coupon to create a new discount for.
    """
    discount: NotRequired[str]
    """
    ID of an existing discount on the object (or one of its ancestors) to reuse.
    """
    promotion_code: NotRequired[str]
    """
    ID of the promotion code to create a new discount for.
    """


class SubscriptionScheduleCreateParamsPhaseDuration(TypedDict):
    interval: Literal["day", "month", "week", "year"]
    """
    Specifies phase duration. Either `day`, `week`, `month` or `year`.
    """
    interval_count: NotRequired[int]
    """
    The multiplier applied to the interval.
    """


class SubscriptionScheduleCreateParamsPhaseInvoiceSettings(TypedDict):
    account_tax_ids: NotRequired["Literal['']|List[str]"]
    """
    The account tax IDs associated with this phase of the subscription schedule. Will be set on invoices generated by this phase of the subscription schedule.
    """
    days_until_due: NotRequired[int]
    """
    Number of days within which a customer must pay invoices generated by this subscription schedule. This value will be `null` for subscription schedules where `billing=charge_automatically`.
    """
    issuer: NotRequired[
        "SubscriptionScheduleCreateParamsPhaseInvoiceSettingsIssuer"
    ]
    """
    The connected account that issues the invoice. The invoice is presented with the branding and support information of the specified account.
    """


class SubscriptionScheduleCreateParamsPhaseInvoiceSettingsIssuer(TypedDict):
    account: NotRequired[str]
    """
    The connected account being referenced when `type` is `account`.
    """
    type: Literal["account", "self"]
    """
    Type of the account referenced in the request.
    """


class SubscriptionScheduleCreateParamsPhaseItem(TypedDict):
    billing_thresholds: NotRequired[
        "Literal['']|SubscriptionScheduleCreateParamsPhaseItemBillingThresholds"
    ]
    """
    Define thresholds at which an invoice will be sent, and the subscription advanced to a new billing period. Pass an empty string to remove previously-defined thresholds.
    """
    discounts: NotRequired[
        "Literal['']|List[SubscriptionScheduleCreateParamsPhaseItemDiscount]"
    ]
    """
    The coupons to redeem into discounts for the subscription item.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to a configuration item. Metadata on a configuration item will update the underlying subscription item's `metadata` when the phase is entered, adding new keys and replacing existing keys. Individual keys in the subscription item's `metadata` can be unset by posting an empty value to them in the configuration item's `metadata`. To unset all keys in the subscription item's `metadata`, update the subscription item directly or unset every key individually from the configuration item's `metadata`.
    """
    plan: NotRequired[str]
    """
    The plan ID to subscribe to. You may specify the same ID in `plan` and `price`.
    """
    price: NotRequired[str]
    """
    The ID of the price object.
    """
    price_data: NotRequired[
        "SubscriptionScheduleCreateParamsPhaseItemPriceData"
    ]
    """
    Data used to generate a new [Price](https://docs.stripe.com/api/prices) object inline.
    """
    quantity: NotRequired[int]
    """
    Quantity for the given price. Can be set only if the price's `usage_type` is `licensed` and not `metered`.
    """
    tax_rates: NotRequired["Literal['']|List[str]"]
    """
    A list of [Tax Rate](https://docs.stripe.com/api/tax_rates) ids. These Tax Rates will override the [`default_tax_rates`](https://docs.stripe.com/api/subscriptions/create#create_subscription-default_tax_rates) on the Subscription. When updating, pass an empty string to remove previously-defined tax rates.
    """


class SubscriptionScheduleCreateParamsPhaseItemBillingThresholds(TypedDict):
    usage_gte: int
    """
    Number of units that meets the billing threshold to advance the subscription to a new billing period (e.g., it takes 10 $5 units to meet a $50 [monetary threshold](https://docs.stripe.com/api/subscriptions/update#update_subscription-billing_thresholds-amount_gte))
    """


class SubscriptionScheduleCreateParamsPhaseItemDiscount(TypedDict):
    coupon: NotRequired[str]
    """
    ID of the coupon to create a new discount for.
    """
    discount: NotRequired[str]
    """
    ID of an existing discount on the object (or one of its ancestors) to reuse.
    """
    promotion_code: NotRequired[str]
    """
    ID of the promotion code to create a new discount for.
    """


class SubscriptionScheduleCreateParamsPhaseItemPriceData(TypedDict):
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    product: str
    """
    The ID of the [Product](https://docs.stripe.com/api/products) that this [Price](https://docs.stripe.com/api/prices) will belong to.
    """
    recurring: "SubscriptionScheduleCreateParamsPhaseItemPriceDataRecurring"
    """
    The recurring components of a price such as `interval` and `interval_count`.
    """
    tax_behavior: NotRequired[Literal["exclusive", "inclusive", "unspecified"]]
    """
    Only required if a [default tax behavior](https://docs.stripe.com/tax/products-prices-tax-categories-tax-behavior#setting-a-default-tax-behavior-(recommended)) was not provided in the Stripe Tax settings. Specifies whether the price is considered inclusive of taxes or exclusive of taxes. One of `inclusive`, `exclusive`, or `unspecified`. Once specified as either `inclusive` or `exclusive`, it cannot be changed.
    """
    unit_amount: NotRequired[int]
    """
    A positive integer in cents (or local equivalent) (or 0 for a free price) representing how much to charge.
    """
    unit_amount_decimal: NotRequired[str]
    """
    Same as `unit_amount`, but accepts a decimal value in cents (or local equivalent) with at most 12 decimal places. Only one of `unit_amount` and `unit_amount_decimal` can be set.
    """


class SubscriptionScheduleCreateParamsPhaseItemPriceDataRecurring(TypedDict):
    interval: Literal["day", "month", "week", "year"]
    """
    Specifies billing frequency. Either `day`, `week`, `month` or `year`.
    """
    interval_count: NotRequired[int]
    """
    The number of intervals between subscription billings. For example, `interval=month` and `interval_count=3` bills every 3 months. Maximum of three years interval allowed (3 years, 36 months, or 156 weeks).
    """


class SubscriptionScheduleCreateParamsPhaseTransferData(TypedDict):
    amount_percent: NotRequired[float]
    """
    A non-negative decimal between 0 and 100, with at most two decimal places. This represents the percentage of the subscription invoice total that will be transferred to the destination account. By default, the entire amount is transferred to the destination.
    """
    destination: str
    """
    ID of an existing, connected Stripe account.
    """
