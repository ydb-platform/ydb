# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List, Union
from typing_extensions import Literal, NotRequired, TypedDict


class ConfigurationCreateParams(RequestOptions):
    business_profile: NotRequired["ConfigurationCreateParamsBusinessProfile"]
    """
    The business information shown to customers in the portal.
    """
    default_return_url: NotRequired["Literal['']|str"]
    """
    The default URL to redirect customers to when they click on the portal's link to return to your website. This can be [overriden](https://docs.stripe.com/api/customer_portal/sessions/create#create_portal_session-return_url) when creating the session.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    features: "ConfigurationCreateParamsFeatures"
    """
    Information about the features available in the portal.
    """
    login_page: NotRequired["ConfigurationCreateParamsLoginPage"]
    """
    The hosted login page for this configuration. Learn more about the portal login page in our [integration docs](https://stripe.com/docs/billing/subscriptions/integrating-customer-portal#share).
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    name: NotRequired["Literal['']|str"]
    """
    The name of the configuration.
    """


class ConfigurationCreateParamsBusinessProfile(TypedDict):
    headline: NotRequired["Literal['']|str"]
    """
    The messaging shown to customers in the portal.
    """
    privacy_policy_url: NotRequired[str]
    """
    A link to the business's publicly available privacy policy.
    """
    terms_of_service_url: NotRequired[str]
    """
    A link to the business's publicly available terms of service.
    """


class ConfigurationCreateParamsFeatures(TypedDict):
    customer_update: NotRequired[
        "ConfigurationCreateParamsFeaturesCustomerUpdate"
    ]
    """
    Information about updating the customer details in the portal.
    """
    invoice_history: NotRequired[
        "ConfigurationCreateParamsFeaturesInvoiceHistory"
    ]
    """
    Information about showing the billing history in the portal.
    """
    payment_method_update: NotRequired[
        "ConfigurationCreateParamsFeaturesPaymentMethodUpdate"
    ]
    """
    Information about updating payment methods in the portal.
    """
    subscription_cancel: NotRequired[
        "ConfigurationCreateParamsFeaturesSubscriptionCancel"
    ]
    """
    Information about canceling subscriptions in the portal.
    """
    subscription_update: NotRequired[
        "ConfigurationCreateParamsFeaturesSubscriptionUpdate"
    ]
    """
    Information about updating subscriptions in the portal.
    """


class ConfigurationCreateParamsFeaturesCustomerUpdate(TypedDict):
    allowed_updates: NotRequired[
        "Literal['']|List[Literal['address', 'email', 'name', 'phone', 'shipping', 'tax_id']]"
    ]
    """
    The types of customer updates that are supported. When empty, customers are not updateable.
    """
    enabled: bool
    """
    Whether the feature is enabled.
    """


class ConfigurationCreateParamsFeaturesInvoiceHistory(TypedDict):
    enabled: bool
    """
    Whether the feature is enabled.
    """


class ConfigurationCreateParamsFeaturesPaymentMethodUpdate(TypedDict):
    enabled: bool
    """
    Whether the feature is enabled.
    """
    payment_method_configuration: NotRequired["Literal['']|str"]
    """
    The [Payment Method Configuration](https://docs.stripe.com/api/payment_method_configurations) to use for this portal session. When specified, customers will be able to update their payment method to one of the options specified by the payment method configuration. If not set or set to an empty string, the default payment method configuration is used.
    """


class ConfigurationCreateParamsFeaturesSubscriptionCancel(TypedDict):
    cancellation_reason: NotRequired[
        "ConfigurationCreateParamsFeaturesSubscriptionCancelCancellationReason"
    ]
    """
    Whether the cancellation reasons will be collected in the portal and which options are exposed to the customer
    """
    enabled: bool
    """
    Whether the feature is enabled.
    """
    mode: NotRequired[Literal["at_period_end", "immediately"]]
    """
    Whether to cancel subscriptions immediately or at the end of the billing period.
    """
    proration_behavior: NotRequired[
        Literal["always_invoice", "create_prorations", "none"]
    ]
    """
    Whether to create prorations when canceling subscriptions. Possible values are `none` and `create_prorations`, which is only compatible with `mode=immediately`. Passing `always_invoice` will result in an error. No prorations are generated when canceling a subscription at the end of its natural billing period.
    """


class ConfigurationCreateParamsFeaturesSubscriptionCancelCancellationReason(
    TypedDict,
):
    enabled: bool
    """
    Whether the feature is enabled.
    """
    options: Union[
        Literal[""],
        List[
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
        ],
    ]
    """
    Which cancellation reasons will be given as options to the customer.
    """


class ConfigurationCreateParamsFeaturesSubscriptionUpdate(TypedDict):
    billing_cycle_anchor: NotRequired[Literal["now", "unchanged"]]
    """
    Determines the value to use for the billing cycle anchor on subscription updates. Valid values are `now` or `unchanged`, and the default value is `unchanged`. Setting the value to `now` resets the subscription's billing cycle anchor to the current time (in UTC). For more information, see the billing cycle [documentation](https://docs.stripe.com/billing/subscriptions/billing-cycle).
    """
    default_allowed_updates: NotRequired[
        "Literal['']|List[Literal['price', 'promotion_code', 'quantity']]"
    ]
    """
    The types of subscription updates that are supported. When empty, subscriptions are not updateable.
    """
    enabled: bool
    """
    Whether the feature is enabled.
    """
    products: NotRequired[
        "Literal['']|List[ConfigurationCreateParamsFeaturesSubscriptionUpdateProduct]"
    ]
    """
    The list of up to 10 products that support subscription updates.
    """
    proration_behavior: NotRequired[
        Literal["always_invoice", "create_prorations", "none"]
    ]
    """
    Determines how to handle prorations resulting from subscription updates. Valid values are `none`, `create_prorations`, and `always_invoice`.
    """
    schedule_at_period_end: NotRequired[
        "ConfigurationCreateParamsFeaturesSubscriptionUpdateScheduleAtPeriodEnd"
    ]
    """
    Setting to control when an update should be scheduled at the end of the period instead of applying immediately.
    """
    trial_update_behavior: NotRequired[Literal["continue_trial", "end_trial"]]
    """
    The behavior when updating a subscription that is trialing.
    """


class ConfigurationCreateParamsFeaturesSubscriptionUpdateProduct(TypedDict):
    adjustable_quantity: NotRequired[
        "ConfigurationCreateParamsFeaturesSubscriptionUpdateProductAdjustableQuantity"
    ]
    """
    Control whether the quantity of the product can be adjusted.
    """
    prices: List[str]
    """
    The list of price IDs for the product that a subscription can be updated to.
    """
    product: str
    """
    The product id.
    """


class ConfigurationCreateParamsFeaturesSubscriptionUpdateProductAdjustableQuantity(
    TypedDict,
):
    enabled: bool
    """
    Set to true if the quantity can be adjusted to any non-negative integer.
    """
    maximum: NotRequired[int]
    """
    The maximum quantity that can be set for the product.
    """
    minimum: NotRequired[int]
    """
    The minimum quantity that can be set for the product.
    """


class ConfigurationCreateParamsFeaturesSubscriptionUpdateScheduleAtPeriodEnd(
    TypedDict,
):
    conditions: NotRequired[
        List[
            "ConfigurationCreateParamsFeaturesSubscriptionUpdateScheduleAtPeriodEndCondition"
        ]
    ]
    """
    List of conditions. When any condition is true, the update will be scheduled at the end of the current period.
    """


class ConfigurationCreateParamsFeaturesSubscriptionUpdateScheduleAtPeriodEndCondition(
    TypedDict,
):
    type: Literal["decreasing_item_amount", "shortening_interval"]
    """
    The type of condition.
    """


class ConfigurationCreateParamsLoginPage(TypedDict):
    enabled: bool
    """
    Set to `true` to generate a shareable URL [`login_page.url`](https://docs.stripe.com/api/customer_portal/configuration#portal_configuration_object-login_page-url) that will take your customers to a hosted login page for the customer portal.
    """
