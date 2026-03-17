# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class SubscriptionItemCreateParams(RequestOptions):
    billing_thresholds: NotRequired[
        "Literal['']|SubscriptionItemCreateParamsBillingThresholds"
    ]
    """
    Define thresholds at which an invoice will be sent, and the subscription advanced to a new billing period. Pass an empty string to remove previously-defined thresholds.
    """
    discounts: NotRequired[
        "Literal['']|List[SubscriptionItemCreateParamsDiscount]"
    ]
    """
    The coupons to redeem into discounts for the subscription item.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    payment_behavior: NotRequired[
        Literal[
            "allow_incomplete",
            "default_incomplete",
            "error_if_incomplete",
            "pending_if_incomplete",
        ]
    ]
    """
    Use `allow_incomplete` to transition the subscription to `status=past_due` if a payment is required but cannot be paid. This allows you to manage scenarios where additional user actions are needed to pay a subscription's invoice. For example, SCA regulation may require 3DS authentication to complete payment. See the [SCA Migration Guide](https://docs.stripe.com/billing/migration/strong-customer-authentication) for Billing to learn more. This is the default behavior.

    Use `default_incomplete` to transition the subscription to `status=past_due` when payment is required and await explicit confirmation of the invoice's payment intent. This allows simpler management of scenarios where additional user actions are needed to pay a subscription's invoice. Such as failed payments, [SCA regulation](https://docs.stripe.com/billing/migration/strong-customer-authentication), or collecting a mandate for a bank debit payment method.

    Use `pending_if_incomplete` to update the subscription using [pending updates](https://docs.stripe.com/billing/subscriptions/pending-updates). When you use `pending_if_incomplete` you can only pass the parameters [supported by pending updates](https://docs.stripe.com/billing/pending-updates-reference#supported-attributes).

    Use `error_if_incomplete` if you want Stripe to return an HTTP 402 status code if a subscription's invoice cannot be paid. For example, if a payment method requires 3DS authentication due to SCA regulation and further user action is needed, this parameter does not update the subscription and returns an error instead. This was the default behavior for API versions prior to 2019-03-14. See the [changelog](https://docs.stripe.com/changelog/2019-03-14) to learn more.
    """
    plan: NotRequired[str]
    """
    The identifier of the plan to add to the subscription.
    """
    price: NotRequired[str]
    """
    The ID of the price object.
    """
    price_data: NotRequired["SubscriptionItemCreateParamsPriceData"]
    """
    Data used to generate a new [Price](https://docs.stripe.com/api/prices) object inline.
    """
    proration_behavior: NotRequired[
        Literal["always_invoice", "create_prorations", "none"]
    ]
    """
    Determines how to handle [prorations](https://docs.stripe.com/billing/subscriptions/prorations) when the billing cycle changes (e.g., when switching plans, resetting `billing_cycle_anchor=now`, or starting a trial), or if an item's `quantity` changes. The default value is `create_prorations`.
    """
    proration_date: NotRequired[int]
    """
    If set, the proration will be calculated as though the subscription was updated at the given time. This can be used to apply the same proration that was previewed with the [upcoming invoice](https://api.stripe.com#retrieve_customer_invoice) endpoint.
    """
    quantity: NotRequired[int]
    """
    The quantity you'd like to apply to the subscription item you're creating.
    """
    subscription: str
    """
    The identifier of the subscription to modify.
    """
    tax_rates: NotRequired["Literal['']|List[str]"]
    """
    A list of [Tax Rate](https://docs.stripe.com/api/tax_rates) ids. These Tax Rates will override the [`default_tax_rates`](https://docs.stripe.com/api/subscriptions/create#create_subscription-default_tax_rates) on the Subscription. When updating, pass an empty string to remove previously-defined tax rates.
    """


class SubscriptionItemCreateParamsBillingThresholds(TypedDict):
    usage_gte: int
    """
    Number of units that meets the billing threshold to advance the subscription to a new billing period (e.g., it takes 10 $5 units to meet a $50 [monetary threshold](https://docs.stripe.com/api/subscriptions/update#update_subscription-billing_thresholds-amount_gte))
    """


class SubscriptionItemCreateParamsDiscount(TypedDict):
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


class SubscriptionItemCreateParamsPriceData(TypedDict):
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    product: str
    """
    The ID of the [Product](https://docs.stripe.com/api/products) that this [Price](https://docs.stripe.com/api/prices) will belong to.
    """
    recurring: "SubscriptionItemCreateParamsPriceDataRecurring"
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


class SubscriptionItemCreateParamsPriceDataRecurring(TypedDict):
    interval: Literal["day", "month", "week", "year"]
    """
    Specifies billing frequency. Either `day`, `week`, `month` or `year`.
    """
    interval_count: NotRequired[int]
    """
    The number of intervals between subscription billings. For example, `interval=month` and `interval_count=3` bills every 3 months. Maximum of three years interval allowed (3 years, 36 months, or 156 weeks).
    """
