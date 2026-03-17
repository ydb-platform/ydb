# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._expandable_field import ExpandableField
from stripe._stripe_object import StripeObject
from typing import ClassVar, List, Optional, cast
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.billing_portal._configuration import Configuration
    from stripe.params.billing_portal._session_create_params import (
        SessionCreateParams,
    )


class Session(CreateableAPIResource["Session"]):
    """
    The Billing customer portal is a Stripe-hosted UI for subscription and
    billing management.

    A portal configuration describes the functionality and features that you
    want to provide to your customers through the portal.

    A portal session describes the instantiation of the customer portal for
    a particular customer. By visiting the session's URL, the customer
    can manage their subscriptions and billing details. For security reasons,
    sessions are short-lived and will expire if the customer does not visit the URL.
    Create sessions on-demand when customers intend to manage their subscriptions
    and billing details.

    Related guide: [Customer management](https://docs.stripe.com/customer-management)
    """

    OBJECT_NAME: ClassVar[Literal["billing_portal.session"]] = (
        "billing_portal.session"
    )

    class Flow(StripeObject):
        class AfterCompletion(StripeObject):
            class HostedConfirmation(StripeObject):
                custom_message: Optional[str]
                """
                A custom message to display to the customer after the flow is completed.
                """

            class Redirect(StripeObject):
                return_url: str
                """
                The URL the customer will be redirected to after the flow is completed.
                """

            hosted_confirmation: Optional[HostedConfirmation]
            """
            Configuration when `after_completion.type=hosted_confirmation`.
            """
            redirect: Optional[Redirect]
            """
            Configuration when `after_completion.type=redirect`.
            """
            type: Literal["hosted_confirmation", "portal_homepage", "redirect"]
            """
            The specified type of behavior after the flow is completed.
            """
            _inner_class_types = {
                "hosted_confirmation": HostedConfirmation,
                "redirect": Redirect,
            }

        class SubscriptionCancel(StripeObject):
            class Retention(StripeObject):
                class CouponOffer(StripeObject):
                    coupon: str
                    """
                    The ID of the coupon to be offered.
                    """

                coupon_offer: Optional[CouponOffer]
                """
                Configuration when `retention.type=coupon_offer`.
                """
                type: Literal["coupon_offer"]
                """
                Type of retention strategy that will be used.
                """
                _inner_class_types = {"coupon_offer": CouponOffer}

            retention: Optional[Retention]
            """
            Specify a retention strategy to be used in the cancellation flow.
            """
            subscription: str
            """
            The ID of the subscription to be canceled.
            """
            _inner_class_types = {"retention": Retention}

        class SubscriptionUpdate(StripeObject):
            subscription: str
            """
            The ID of the subscription to be updated.
            """

        class SubscriptionUpdateConfirm(StripeObject):
            class Discount(StripeObject):
                coupon: Optional[str]
                """
                The ID of the coupon to apply to this subscription update.
                """
                promotion_code: Optional[str]
                """
                The ID of a promotion code to apply to this subscription update.
                """

            class Item(StripeObject):
                id: Optional[str]
                """
                The ID of the [subscription item](https://docs.stripe.com/api/subscriptions/object#subscription_object-items-data-id) to be updated.
                """
                price: Optional[str]
                """
                The price the customer should subscribe to through this flow. The price must also be included in the configuration's [`features.subscription_update.products`](https://docs.stripe.com/api/customer_portal/configuration#portal_configuration_object-features-subscription_update-products).
                """
                quantity: Optional[int]
                """
                [Quantity](https://docs.stripe.com/subscriptions/quantities) for this item that the customer should subscribe to through this flow.
                """

            discounts: Optional[List[Discount]]
            """
            The coupon or promotion code to apply to this subscription update.
            """
            items: List[Item]
            """
            The [subscription item](https://docs.stripe.com/api/subscription_items) to be updated through this flow. Currently, only up to one may be specified and subscriptions with multiple items are not updatable.
            """
            subscription: str
            """
            The ID of the subscription to be updated.
            """
            _inner_class_types = {"discounts": Discount, "items": Item}

        after_completion: AfterCompletion
        subscription_cancel: Optional[SubscriptionCancel]
        """
        Configuration when `flow.type=subscription_cancel`.
        """
        subscription_update: Optional[SubscriptionUpdate]
        """
        Configuration when `flow.type=subscription_update`.
        """
        subscription_update_confirm: Optional[SubscriptionUpdateConfirm]
        """
        Configuration when `flow.type=subscription_update_confirm`.
        """
        type: Literal[
            "payment_method_update",
            "subscription_cancel",
            "subscription_update",
            "subscription_update_confirm",
        ]
        """
        Type of flow that the customer will go through.
        """
        _inner_class_types = {
            "after_completion": AfterCompletion,
            "subscription_cancel": SubscriptionCancel,
            "subscription_update": SubscriptionUpdate,
            "subscription_update_confirm": SubscriptionUpdateConfirm,
        }

    configuration: ExpandableField["Configuration"]
    """
    The configuration used by this session, describing the features available.
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    customer: str
    """
    The ID of the customer for this session.
    """
    customer_account: Optional[str]
    """
    The ID of the account for this session.
    """
    flow: Optional[Flow]
    """
    Information about a specific flow for the customer to go through. See the [docs](https://docs.stripe.com/customer-management/portal-deep-links) to learn more about using customer portal deep links and flows.
    """
    id: str
    """
    Unique identifier for the object.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    locale: Optional[
        Literal[
            "auto",
            "bg",
            "cs",
            "da",
            "de",
            "el",
            "en",
            "en-AU",
            "en-CA",
            "en-GB",
            "en-IE",
            "en-IN",
            "en-NZ",
            "en-SG",
            "es",
            "es-419",
            "et",
            "fi",
            "fil",
            "fr",
            "fr-CA",
            "hr",
            "hu",
            "id",
            "it",
            "ja",
            "ko",
            "lt",
            "lv",
            "ms",
            "mt",
            "nb",
            "nl",
            "pl",
            "pt",
            "pt-BR",
            "ro",
            "ru",
            "sk",
            "sl",
            "sv",
            "th",
            "tr",
            "vi",
            "zh",
            "zh-HK",
            "zh-TW",
        ]
    ]
    """
    The IETF language tag of the locale Customer Portal is displayed in. If blank or auto, the customer's `preferred_locales` or browser's locale is used.
    """
    object: Literal["billing_portal.session"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    on_behalf_of: Optional[str]
    """
    The account for which the session was created on behalf of. When specified, only subscriptions and invoices with this `on_behalf_of` account appear in the portal. For more information, see the [docs](https://docs.stripe.com/connect/separate-charges-and-transfers#settlement-merchant). Use the [Accounts API](https://docs.stripe.com/api/accounts/object#account_object-settings-branding) to modify the `on_behalf_of` account's branding settings, which the portal displays.
    """
    return_url: Optional[str]
    """
    The URL to redirect customers to when they click on the portal's link to return to your website.
    """
    url: str
    """
    The short-lived URL of the session that gives customers access to the customer portal.
    """

    @classmethod
    def create(cls, **params: Unpack["SessionCreateParams"]) -> "Session":
        """
        Creates a session of the customer portal.
        """
        return cast(
            "Session",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["SessionCreateParams"]
    ) -> "Session":
        """
        Creates a session of the customer portal.
        """
        return cast(
            "Session",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    _inner_class_types = {"flow": Flow}
