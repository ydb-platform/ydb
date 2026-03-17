# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._expandable_field import ExpandableField
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._stripe_object import StripeObject
from stripe._updateable_api_resource import UpdateableAPIResource
from stripe._util import sanitize_id
from typing import ClassVar, Dict, List, Optional, cast
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._application import Application
    from stripe.params.billing_portal._configuration_create_params import (
        ConfigurationCreateParams,
    )
    from stripe.params.billing_portal._configuration_list_params import (
        ConfigurationListParams,
    )
    from stripe.params.billing_portal._configuration_modify_params import (
        ConfigurationModifyParams,
    )
    from stripe.params.billing_portal._configuration_retrieve_params import (
        ConfigurationRetrieveParams,
    )


class Configuration(
    CreateableAPIResource["Configuration"],
    ListableAPIResource["Configuration"],
    UpdateableAPIResource["Configuration"],
):
    """
    A portal configuration describes the functionality and behavior you embed in a portal session. Related guide: [Configure the customer portal](https://docs.stripe.com/customer-management/configure-portal).
    """

    OBJECT_NAME: ClassVar[Literal["billing_portal.configuration"]] = (
        "billing_portal.configuration"
    )

    class BusinessProfile(StripeObject):
        headline: Optional[str]
        """
        The messaging shown to customers in the portal.
        """
        privacy_policy_url: Optional[str]
        """
        A link to the business's publicly available privacy policy.
        """
        terms_of_service_url: Optional[str]
        """
        A link to the business's publicly available terms of service.
        """

    class Features(StripeObject):
        class CustomerUpdate(StripeObject):
            allowed_updates: List[
                Literal[
                    "address", "email", "name", "phone", "shipping", "tax_id"
                ]
            ]
            """
            The types of customer updates that are supported. When empty, customers are not updateable.
            """
            enabled: bool
            """
            Whether the feature is enabled.
            """

        class InvoiceHistory(StripeObject):
            enabled: bool
            """
            Whether the feature is enabled.
            """

        class PaymentMethodUpdate(StripeObject):
            enabled: bool
            """
            Whether the feature is enabled.
            """
            payment_method_configuration: Optional[str]
            """
            The [Payment Method Configuration](https://docs.stripe.com/api/payment_method_configurations) to use for this portal session. When specified, customers will be able to update their payment method to one of the options specified by the payment method configuration. If not set, the default payment method configuration is used.
            """

        class SubscriptionCancel(StripeObject):
            class CancellationReason(StripeObject):
                enabled: bool
                """
                Whether the feature is enabled.
                """
                options: List[
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
                Which cancellation reasons will be given as options to the customer.
                """

            cancellation_reason: CancellationReason
            enabled: bool
            """
            Whether the feature is enabled.
            """
            mode: Literal["at_period_end", "immediately"]
            """
            Whether to cancel subscriptions immediately or at the end of the billing period.
            """
            proration_behavior: Literal[
                "always_invoice", "create_prorations", "none"
            ]
            """
            Whether to create prorations when canceling subscriptions. Possible values are `none` and `create_prorations`.
            """
            _inner_class_types = {"cancellation_reason": CancellationReason}

        class SubscriptionUpdate(StripeObject):
            class Product(StripeObject):
                class AdjustableQuantity(StripeObject):
                    enabled: bool
                    """
                    If true, the quantity can be adjusted to any non-negative integer.
                    """
                    maximum: Optional[int]
                    """
                    The maximum quantity that can be set for the product.
                    """
                    minimum: int
                    """
                    The minimum quantity that can be set for the product.
                    """

                adjustable_quantity: AdjustableQuantity
                prices: List[str]
                """
                The list of price IDs which, when subscribed to, a subscription can be updated.
                """
                product: str
                """
                The product ID.
                """
                _inner_class_types = {
                    "adjustable_quantity": AdjustableQuantity
                }

            class ScheduleAtPeriodEnd(StripeObject):
                class Condition(StripeObject):
                    type: Literal[
                        "decreasing_item_amount", "shortening_interval"
                    ]
                    """
                    The type of condition.
                    """

                conditions: List[Condition]
                """
                List of conditions. When any condition is true, an update will be scheduled at the end of the current period.
                """
                _inner_class_types = {"conditions": Condition}

            billing_cycle_anchor: Optional[Literal["now", "unchanged"]]
            """
            Determines the value to use for the billing cycle anchor on subscription updates. Valid values are `now` or `unchanged`, and the default value is `unchanged`. Setting the value to `now` resets the subscription's billing cycle anchor to the current time (in UTC). For more information, see the billing cycle [documentation](https://docs.stripe.com/billing/subscriptions/billing-cycle).
            """
            default_allowed_updates: List[
                Literal["price", "promotion_code", "quantity"]
            ]
            """
            The types of subscription updates that are supported for items listed in the `products` attribute. When empty, subscriptions are not updateable.
            """
            enabled: bool
            """
            Whether the feature is enabled.
            """
            products: Optional[List[Product]]
            """
            The list of up to 10 products that support subscription updates.
            """
            proration_behavior: Literal[
                "always_invoice", "create_prorations", "none"
            ]
            """
            Determines how to handle prorations resulting from subscription updates. Valid values are `none`, `create_prorations`, and `always_invoice`. Defaults to a value of `none` if you don't set it during creation.
            """
            schedule_at_period_end: ScheduleAtPeriodEnd
            trial_update_behavior: Literal["continue_trial", "end_trial"]
            """
            Determines how handle updates to trialing subscriptions. Valid values are `end_trial` and `continue_trial`. Defaults to a value of `end_trial` if you don't set it during creation.
            """
            _inner_class_types = {
                "products": Product,
                "schedule_at_period_end": ScheduleAtPeriodEnd,
            }

        customer_update: CustomerUpdate
        invoice_history: InvoiceHistory
        payment_method_update: PaymentMethodUpdate
        subscription_cancel: SubscriptionCancel
        subscription_update: SubscriptionUpdate
        _inner_class_types = {
            "customer_update": CustomerUpdate,
            "invoice_history": InvoiceHistory,
            "payment_method_update": PaymentMethodUpdate,
            "subscription_cancel": SubscriptionCancel,
            "subscription_update": SubscriptionUpdate,
        }

    class LoginPage(StripeObject):
        enabled: bool
        """
        If `true`, a shareable `url` will be generated that will take your customers to a hosted login page for the customer portal.

        If `false`, the previously generated `url`, if any, will be deactivated.
        """
        url: Optional[str]
        """
        A shareable URL to the hosted portal login page. Your customers will be able to log in with their [email](https://docs.stripe.com/api/customers/object#customer_object-email) and receive a link to their customer portal.
        """

    active: bool
    """
    Whether the configuration is active and can be used to create portal sessions.
    """
    application: Optional[ExpandableField["Application"]]
    """
    ID of the Connect Application that created the configuration.
    """
    business_profile: BusinessProfile
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    default_return_url: Optional[str]
    """
    The default URL to redirect customers to when they click on the portal's link to return to your website. This can be [overriden](https://docs.stripe.com/api/customer_portal/sessions/create#create_portal_session-return_url) when creating the session.
    """
    features: Features
    id: str
    """
    Unique identifier for the object.
    """
    is_default: bool
    """
    Whether the configuration is the default. If `true`, this configuration can be managed in the Dashboard and portal sessions will use this configuration unless it is overriden when creating the session.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    login_page: LoginPage
    metadata: Optional[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    name: Optional[str]
    """
    The name of the configuration.
    """
    object: Literal["billing_portal.configuration"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    updated: int
    """
    Time at which the object was last updated. Measured in seconds since the Unix epoch.
    """

    @classmethod
    def create(
        cls, **params: Unpack["ConfigurationCreateParams"]
    ) -> "Configuration":
        """
        Creates a configuration that describes the functionality and behavior of a PortalSession
        """
        return cast(
            "Configuration",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["ConfigurationCreateParams"]
    ) -> "Configuration":
        """
        Creates a configuration that describes the functionality and behavior of a PortalSession
        """
        return cast(
            "Configuration",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def list(
        cls, **params: Unpack["ConfigurationListParams"]
    ) -> ListObject["Configuration"]:
        """
        Returns a list of configurations that describe the functionality of the customer portal.
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
        cls, **params: Unpack["ConfigurationListParams"]
    ) -> ListObject["Configuration"]:
        """
        Returns a list of configurations that describe the functionality of the customer portal.
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
    def modify(
        cls, id: str, **params: Unpack["ConfigurationModifyParams"]
    ) -> "Configuration":
        """
        Updates a configuration that describes the functionality of the customer portal.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Configuration",
            cls._static_request(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    async def modify_async(
        cls, id: str, **params: Unpack["ConfigurationModifyParams"]
    ) -> "Configuration":
        """
        Updates a configuration that describes the functionality of the customer portal.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Configuration",
            await cls._static_request_async(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["ConfigurationRetrieveParams"]
    ) -> "Configuration":
        """
        Retrieves a configuration that describes the functionality of the customer portal.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["ConfigurationRetrieveParams"]
    ) -> "Configuration":
        """
        Retrieves a configuration that describes the functionality of the customer portal.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    _inner_class_types = {
        "business_profile": BusinessProfile,
        "features": Features,
        "login_page": LoginPage,
    }
