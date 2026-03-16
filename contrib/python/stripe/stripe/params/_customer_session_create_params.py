# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class CustomerSessionCreateParams(RequestOptions):
    components: "CustomerSessionCreateParamsComponents"
    """
    Configuration for each component. At least 1 component must be enabled.
    """
    customer: NotRequired[str]
    """
    The ID of an existing customer for which to create the Customer Session.
    """
    customer_account: NotRequired[str]
    """
    The ID of an existing Account for which to create the Customer Session.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """


class CustomerSessionCreateParamsComponents(TypedDict):
    buy_button: NotRequired["CustomerSessionCreateParamsComponentsBuyButton"]
    """
    Configuration for buy button.
    """
    customer_sheet: NotRequired[
        "CustomerSessionCreateParamsComponentsCustomerSheet"
    ]
    """
    Configuration for the customer sheet.
    """
    mobile_payment_element: NotRequired[
        "CustomerSessionCreateParamsComponentsMobilePaymentElement"
    ]
    """
    Configuration for the mobile payment element.
    """
    payment_element: NotRequired[
        "CustomerSessionCreateParamsComponentsPaymentElement"
    ]
    """
    Configuration for the Payment Element.
    """
    pricing_table: NotRequired[
        "CustomerSessionCreateParamsComponentsPricingTable"
    ]
    """
    Configuration for the pricing table.
    """


class CustomerSessionCreateParamsComponentsBuyButton(TypedDict):
    enabled: bool
    """
    Whether the buy button is enabled.
    """


class CustomerSessionCreateParamsComponentsCustomerSheet(TypedDict):
    enabled: bool
    """
    Whether the customer sheet is enabled.
    """
    features: NotRequired[
        "CustomerSessionCreateParamsComponentsCustomerSheetFeatures"
    ]
    """
    This hash defines whether the customer sheet supports certain features.
    """


class CustomerSessionCreateParamsComponentsCustomerSheetFeatures(TypedDict):
    payment_method_allow_redisplay_filters: NotRequired[
        List[Literal["always", "limited", "unspecified"]]
    ]
    """
    A list of [`allow_redisplay`](https://docs.stripe.com/api/payment_methods/object#payment_method_object-allow_redisplay) values that controls which saved payment methods the customer sheet displays by filtering to only show payment methods with an `allow_redisplay` value that is present in this list.

    If not specified, defaults to ["always"]. In order to display all saved payment methods, specify ["always", "limited", "unspecified"].
    """
    payment_method_remove: NotRequired[Literal["disabled", "enabled"]]
    """
    Controls whether the customer sheet displays the option to remove a saved payment method."

    Allowing buyers to remove their saved payment methods impacts subscriptions that depend on that payment method. Removing the payment method detaches the [`customer` object](https://docs.stripe.com/api/payment_methods/object#payment_method_object-customer) from that [PaymentMethod](https://docs.stripe.com/api/payment_methods).
    """


class CustomerSessionCreateParamsComponentsMobilePaymentElement(TypedDict):
    enabled: bool
    """
    Whether the mobile payment element is enabled.
    """
    features: NotRequired[
        "CustomerSessionCreateParamsComponentsMobilePaymentElementFeatures"
    ]
    """
    This hash defines whether the mobile payment element supports certain features.
    """


class CustomerSessionCreateParamsComponentsMobilePaymentElementFeatures(
    TypedDict,
):
    payment_method_allow_redisplay_filters: NotRequired[
        List[Literal["always", "limited", "unspecified"]]
    ]
    """
    A list of [`allow_redisplay`](https://docs.stripe.com/api/payment_methods/object#payment_method_object-allow_redisplay) values that controls which saved payment methods the mobile payment element displays by filtering to only show payment methods with an `allow_redisplay` value that is present in this list.

    If not specified, defaults to ["always"]. In order to display all saved payment methods, specify ["always", "limited", "unspecified"].
    """
    payment_method_redisplay: NotRequired[Literal["disabled", "enabled"]]
    """
    Controls whether or not the mobile payment element shows saved payment methods.
    """
    payment_method_remove: NotRequired[Literal["disabled", "enabled"]]
    """
    Controls whether the mobile payment element displays the option to remove a saved payment method."

    Allowing buyers to remove their saved payment methods impacts subscriptions that depend on that payment method. Removing the payment method detaches the [`customer` object](https://docs.stripe.com/api/payment_methods/object#payment_method_object-customer) from that [PaymentMethod](https://docs.stripe.com/api/payment_methods).
    """
    payment_method_save: NotRequired[Literal["disabled", "enabled"]]
    """
    Controls whether the mobile payment element displays a checkbox offering to save a new payment method.

    If a customer checks the box, the [`allow_redisplay`](https://docs.stripe.com/api/payment_methods/object#payment_method_object-allow_redisplay) value on the PaymentMethod is set to `'always'` at confirmation time. For PaymentIntents, the [`setup_future_usage`](https://docs.stripe.com/api/payment_intents/object#payment_intent_object-setup_future_usage) value is also set to the value defined in `payment_method_save_usage`.
    """
    payment_method_save_allow_redisplay_override: NotRequired[
        Literal["always", "limited", "unspecified"]
    ]
    """
    Allows overriding the value of allow_override when saving a new payment method when payment_method_save is set to disabled. Use values: "always", "limited", or "unspecified".

    If not specified, defaults to `nil` (no override value).
    """


class CustomerSessionCreateParamsComponentsPaymentElement(TypedDict):
    enabled: bool
    """
    Whether the Payment Element is enabled.
    """
    features: NotRequired[
        "CustomerSessionCreateParamsComponentsPaymentElementFeatures"
    ]
    """
    This hash defines whether the Payment Element supports certain features.
    """


class CustomerSessionCreateParamsComponentsPaymentElementFeatures(TypedDict):
    payment_method_allow_redisplay_filters: NotRequired[
        List[Literal["always", "limited", "unspecified"]]
    ]
    """
    A list of [`allow_redisplay`](https://docs.stripe.com/api/payment_methods/object#payment_method_object-allow_redisplay) values that controls which saved payment methods the Payment Element displays by filtering to only show payment methods with an `allow_redisplay` value that is present in this list.

    If not specified, defaults to ["always"]. In order to display all saved payment methods, specify ["always", "limited", "unspecified"].
    """
    payment_method_redisplay: NotRequired[Literal["disabled", "enabled"]]
    """
    Controls whether or not the Payment Element shows saved payment methods. This parameter defaults to `disabled`.
    """
    payment_method_redisplay_limit: NotRequired[int]
    """
    Determines the max number of saved payment methods for the Payment Element to display. This parameter defaults to `3`. The maximum redisplay limit is `10`.
    """
    payment_method_remove: NotRequired[Literal["disabled", "enabled"]]
    """
    Controls whether the Payment Element displays the option to remove a saved payment method. This parameter defaults to `disabled`.

    Allowing buyers to remove their saved payment methods impacts subscriptions that depend on that payment method. Removing the payment method detaches the [`customer` object](https://docs.stripe.com/api/payment_methods/object#payment_method_object-customer) from that [PaymentMethod](https://docs.stripe.com/api/payment_methods).
    """
    payment_method_save: NotRequired[Literal["disabled", "enabled"]]
    """
    Controls whether the Payment Element displays a checkbox offering to save a new payment method. This parameter defaults to `disabled`.

    If a customer checks the box, the [`allow_redisplay`](https://docs.stripe.com/api/payment_methods/object#payment_method_object-allow_redisplay) value on the PaymentMethod is set to `'always'` at confirmation time. For PaymentIntents, the [`setup_future_usage`](https://docs.stripe.com/api/payment_intents/object#payment_intent_object-setup_future_usage) value is also set to the value defined in `payment_method_save_usage`.
    """
    payment_method_save_usage: NotRequired[
        Literal["off_session", "on_session"]
    ]
    """
    When using PaymentIntents and the customer checks the save checkbox, this field determines the [`setup_future_usage`](https://docs.stripe.com/api/payment_intents/object#payment_intent_object-setup_future_usage) value used to confirm the PaymentIntent.

    When using SetupIntents, directly configure the [`usage`](https://docs.stripe.com/api/setup_intents/object#setup_intent_object-usage) value on SetupIntent creation.
    """


class CustomerSessionCreateParamsComponentsPricingTable(TypedDict):
    enabled: bool
    """
    Whether the pricing table is enabled.
    """
