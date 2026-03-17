# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._expandable_field import ExpandableField
from stripe._stripe_object import StripeObject
from typing import ClassVar, List, Optional, cast
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._customer import Customer
    from stripe.params._customer_session_create_params import (
        CustomerSessionCreateParams,
    )


class CustomerSession(CreateableAPIResource["CustomerSession"]):
    """
    A Customer Session allows you to grant Stripe's frontend SDKs (like Stripe.js) client-side access
    control over a Customer.

    Related guides: [Customer Session with the Payment Element](https://docs.stripe.com/payments/accept-a-payment-deferred?platform=web&type=payment#save-payment-methods),
    [Customer Session with the Pricing Table](https://docs.stripe.com/payments/checkout/pricing-table#customer-session),
    [Customer Session with the Buy Button](https://docs.stripe.com/payment-links/buy-button#pass-an-existing-customer).
    """

    OBJECT_NAME: ClassVar[Literal["customer_session"]] = "customer_session"

    class Components(StripeObject):
        class BuyButton(StripeObject):
            enabled: bool
            """
            Whether the buy button is enabled.
            """

        class CustomerSheet(StripeObject):
            class Features(StripeObject):
                payment_method_allow_redisplay_filters: Optional[
                    List[Literal["always", "limited", "unspecified"]]
                ]
                """
                A list of [`allow_redisplay`](https://docs.stripe.com/api/payment_methods/object#payment_method_object-allow_redisplay) values that controls which saved payment methods the customer sheet displays by filtering to only show payment methods with an `allow_redisplay` value that is present in this list.

                If not specified, defaults to ["always"]. In order to display all saved payment methods, specify ["always", "limited", "unspecified"].
                """
                payment_method_remove: Optional[Literal["disabled", "enabled"]]
                """
                Controls whether the customer sheet displays the option to remove a saved payment method."

                Allowing buyers to remove their saved payment methods impacts subscriptions that depend on that payment method. Removing the payment method detaches the [`customer` object](https://docs.stripe.com/api/payment_methods/object#payment_method_object-customer) from that [PaymentMethod](https://docs.stripe.com/api/payment_methods).
                """

            enabled: bool
            """
            Whether the customer sheet is enabled.
            """
            features: Optional[Features]
            """
            This hash defines whether the customer sheet supports certain features.
            """
            _inner_class_types = {"features": Features}

        class MobilePaymentElement(StripeObject):
            class Features(StripeObject):
                payment_method_allow_redisplay_filters: Optional[
                    List[Literal["always", "limited", "unspecified"]]
                ]
                """
                A list of [`allow_redisplay`](https://docs.stripe.com/api/payment_methods/object#payment_method_object-allow_redisplay) values that controls which saved payment methods the mobile payment element displays by filtering to only show payment methods with an `allow_redisplay` value that is present in this list.

                If not specified, defaults to ["always"]. In order to display all saved payment methods, specify ["always", "limited", "unspecified"].
                """
                payment_method_redisplay: Optional[
                    Literal["disabled", "enabled"]
                ]
                """
                Controls whether or not the mobile payment element shows saved payment methods.
                """
                payment_method_remove: Optional[Literal["disabled", "enabled"]]
                """
                Controls whether the mobile payment element displays the option to remove a saved payment method."

                Allowing buyers to remove their saved payment methods impacts subscriptions that depend on that payment method. Removing the payment method detaches the [`customer` object](https://docs.stripe.com/api/payment_methods/object#payment_method_object-customer) from that [PaymentMethod](https://docs.stripe.com/api/payment_methods).
                """
                payment_method_save: Optional[Literal["disabled", "enabled"]]
                """
                Controls whether the mobile payment element displays a checkbox offering to save a new payment method.

                If a customer checks the box, the [`allow_redisplay`](https://docs.stripe.com/api/payment_methods/object#payment_method_object-allow_redisplay) value on the PaymentMethod is set to `'always'` at confirmation time. For PaymentIntents, the [`setup_future_usage`](https://docs.stripe.com/api/payment_intents/object#payment_intent_object-setup_future_usage) value is also set to the value defined in `payment_method_save_usage`.
                """
                payment_method_save_allow_redisplay_override: Optional[
                    Literal["always", "limited", "unspecified"]
                ]
                """
                Allows overriding the value of allow_override when saving a new payment method when payment_method_save is set to disabled. Use values: "always", "limited", or "unspecified".

                If not specified, defaults to `nil` (no override value).
                """

            enabled: bool
            """
            Whether the mobile payment element is enabled.
            """
            features: Optional[Features]
            """
            This hash defines whether the mobile payment element supports certain features.
            """
            _inner_class_types = {"features": Features}

        class PaymentElement(StripeObject):
            class Features(StripeObject):
                payment_method_allow_redisplay_filters: List[
                    Literal["always", "limited", "unspecified"]
                ]
                """
                A list of [`allow_redisplay`](https://docs.stripe.com/api/payment_methods/object#payment_method_object-allow_redisplay) values that controls which saved payment methods the Payment Element displays by filtering to only show payment methods with an `allow_redisplay` value that is present in this list.

                If not specified, defaults to ["always"]. In order to display all saved payment methods, specify ["always", "limited", "unspecified"].
                """
                payment_method_redisplay: Literal["disabled", "enabled"]
                """
                Controls whether or not the Payment Element shows saved payment methods. This parameter defaults to `disabled`.
                """
                payment_method_redisplay_limit: Optional[int]
                """
                Determines the max number of saved payment methods for the Payment Element to display. This parameter defaults to `3`. The maximum redisplay limit is `10`.
                """
                payment_method_remove: Literal["disabled", "enabled"]
                """
                Controls whether the Payment Element displays the option to remove a saved payment method. This parameter defaults to `disabled`.

                Allowing buyers to remove their saved payment methods impacts subscriptions that depend on that payment method. Removing the payment method detaches the [`customer` object](https://docs.stripe.com/api/payment_methods/object#payment_method_object-customer) from that [PaymentMethod](https://docs.stripe.com/api/payment_methods).
                """
                payment_method_save: Literal["disabled", "enabled"]
                """
                Controls whether the Payment Element displays a checkbox offering to save a new payment method. This parameter defaults to `disabled`.

                If a customer checks the box, the [`allow_redisplay`](https://docs.stripe.com/api/payment_methods/object#payment_method_object-allow_redisplay) value on the PaymentMethod is set to `'always'` at confirmation time. For PaymentIntents, the [`setup_future_usage`](https://docs.stripe.com/api/payment_intents/object#payment_intent_object-setup_future_usage) value is also set to the value defined in `payment_method_save_usage`.
                """
                payment_method_save_usage: Optional[
                    Literal["off_session", "on_session"]
                ]
                """
                When using PaymentIntents and the customer checks the save checkbox, this field determines the [`setup_future_usage`](https://docs.stripe.com/api/payment_intents/object#payment_intent_object-setup_future_usage) value used to confirm the PaymentIntent.

                When using SetupIntents, directly configure the [`usage`](https://docs.stripe.com/api/setup_intents/object#setup_intent_object-usage) value on SetupIntent creation.
                """

            enabled: bool
            """
            Whether the Payment Element is enabled.
            """
            features: Optional[Features]
            """
            This hash defines whether the Payment Element supports certain features.
            """
            _inner_class_types = {"features": Features}

        class PricingTable(StripeObject):
            enabled: bool
            """
            Whether the pricing table is enabled.
            """

        buy_button: BuyButton
        """
        This hash contains whether the buy button is enabled.
        """
        customer_sheet: CustomerSheet
        """
        This hash contains whether the customer sheet is enabled and the features it supports.
        """
        mobile_payment_element: MobilePaymentElement
        """
        This hash contains whether the mobile payment element is enabled and the features it supports.
        """
        payment_element: PaymentElement
        """
        This hash contains whether the Payment Element is enabled and the features it supports.
        """
        pricing_table: PricingTable
        """
        This hash contains whether the pricing table is enabled.
        """
        _inner_class_types = {
            "buy_button": BuyButton,
            "customer_sheet": CustomerSheet,
            "mobile_payment_element": MobilePaymentElement,
            "payment_element": PaymentElement,
            "pricing_table": PricingTable,
        }

    client_secret: str
    """
    The client secret of this Customer Session. Used on the client to set up secure access to the given `customer`.

    The client secret can be used to provide access to `customer` from your frontend. It should not be stored, logged, or exposed to anyone other than the relevant customer. Make sure that you have TLS enabled on any page that includes the client secret.
    """
    components: Optional[Components]
    """
    Configuration for the components supported by this Customer Session.
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    customer: ExpandableField["Customer"]
    """
    The Customer the Customer Session was created for.
    """
    customer_account: Optional[str]
    """
    The Account that the Customer Session was created for.
    """
    expires_at: int
    """
    The timestamp at which this Customer Session will expire.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    object: Literal["customer_session"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """

    @classmethod
    def create(
        cls, **params: Unpack["CustomerSessionCreateParams"]
    ) -> "CustomerSession":
        """
        Creates a Customer Session object that includes a single-use client secret that you can use on your front-end to grant client-side API access for certain customer resources.
        """
        return cast(
            "CustomerSession",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["CustomerSessionCreateParams"]
    ) -> "CustomerSession":
        """
        Creates a Customer Session object that includes a single-use client secret that you can use on your front-end to grant client-side API access for certain customer resources.
        """
        return cast(
            "CustomerSession",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    _inner_class_types = {"components": Components}
