# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._deletable_api_resource import DeletableAPIResource
from stripe._expandable_field import ExpandableField
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._stripe_object import StripeObject
from stripe._updateable_api_resource import UpdateableAPIResource
from stripe._util import class_method_variant, sanitize_id
from typing import ClassVar, Dict, List, Optional, cast, overload
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._discount import Discount
    from stripe._plan import Plan
    from stripe._price import Price
    from stripe._tax_rate import TaxRate
    from stripe.params._subscription_item_create_params import (
        SubscriptionItemCreateParams,
    )
    from stripe.params._subscription_item_delete_params import (
        SubscriptionItemDeleteParams,
    )
    from stripe.params._subscription_item_list_params import (
        SubscriptionItemListParams,
    )
    from stripe.params._subscription_item_modify_params import (
        SubscriptionItemModifyParams,
    )
    from stripe.params._subscription_item_retrieve_params import (
        SubscriptionItemRetrieveParams,
    )


class SubscriptionItem(
    CreateableAPIResource["SubscriptionItem"],
    DeletableAPIResource["SubscriptionItem"],
    ListableAPIResource["SubscriptionItem"],
    UpdateableAPIResource["SubscriptionItem"],
):
    """
    Subscription items allow you to create customer subscriptions with more than
    one plan, making it easy to represent complex billing relationships.
    """

    OBJECT_NAME: ClassVar[Literal["subscription_item"]] = "subscription_item"

    class BillingThresholds(StripeObject):
        usage_gte: Optional[int]
        """
        Usage threshold that triggers the subscription to create an invoice
        """

    billing_thresholds: Optional[BillingThresholds]
    """
    Define thresholds at which an invoice will be sent, and the related subscription advanced to a new billing period
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    current_period_end: int
    """
    The end time of this subscription item's current billing period.
    """
    current_period_start: int
    """
    The start time of this subscription item's current billing period.
    """
    deleted: Optional[Literal[True]]
    """
    Always true for a deleted object
    """
    discounts: List[ExpandableField["Discount"]]
    """
    The discounts applied to the subscription item. Subscription item discounts are applied before subscription discounts. Use `expand[]=discounts` to expand each discount.
    """
    id: str
    """
    Unique identifier for the object.
    """
    metadata: Dict[str, str]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    object: Literal["subscription_item"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    plan: "Plan"
    """
    You can now model subscriptions more flexibly using the [Prices API](https://api.stripe.com#prices). It replaces the Plans API and is backwards compatible to simplify your migration.

    Plans define the base price, currency, and billing cycle for recurring purchases of products.
    [Products](https://api.stripe.com#products) help you track inventory or provisioning, and plans help you track pricing. Different physical goods or levels of service should be represented by products, and pricing options should be represented by plans. This approach lets you change prices without having to change your provisioning scheme.

    For example, you might have a single "gold" product that has plans for $10/month, $100/year, €9/month, and €90/year.

    Related guides: [Set up a subscription](https://docs.stripe.com/billing/subscriptions/set-up-subscription) and more about [products and prices](https://docs.stripe.com/products-prices/overview).
    """
    price: "Price"
    """
    Prices define the unit cost, currency, and (optional) billing cycle for both recurring and one-time purchases of products.
    [Products](https://api.stripe.com#products) help you track inventory or provisioning, and prices help you track payment terms. Different physical goods or levels of service should be represented by products, and pricing options should be represented by prices. This approach lets you change prices without having to change your provisioning scheme.

    For example, you might have a single "gold" product that has prices for $10/month, $100/year, and €9 once.

    Related guides: [Set up a subscription](https://docs.stripe.com/billing/subscriptions/set-up-subscription), [create an invoice](https://docs.stripe.com/billing/invoices/create), and more about [products and prices](https://docs.stripe.com/products-prices/overview).
    """
    quantity: Optional[int]
    """
    The [quantity](https://docs.stripe.com/subscriptions/quantities) of the plan to which the customer should be subscribed.
    """
    subscription: str
    """
    The `subscription` this `subscription_item` belongs to.
    """
    tax_rates: Optional[List["TaxRate"]]
    """
    The tax rates which apply to this `subscription_item`. When set, the `default_tax_rates` on the subscription do not apply to this `subscription_item`.
    """

    @classmethod
    def create(
        cls, **params: Unpack["SubscriptionItemCreateParams"]
    ) -> "SubscriptionItem":
        """
        Adds a new item to an existing subscription. No existing items will be changed or replaced.
        """
        return cast(
            "SubscriptionItem",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["SubscriptionItemCreateParams"]
    ) -> "SubscriptionItem":
        """
        Adds a new item to an existing subscription. No existing items will be changed or replaced.
        """
        return cast(
            "SubscriptionItem",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def _cls_delete(
        cls, sid: str, **params: Unpack["SubscriptionItemDeleteParams"]
    ) -> "SubscriptionItem":
        """
        Deletes an item from the subscription. Removing a subscription item from a subscription will not cancel the subscription.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(sid))
        return cast(
            "SubscriptionItem",
            cls._static_request(
                "delete",
                url,
                params=params,
            ),
        )

    @overload
    @staticmethod
    def delete(
        sid: str, **params: Unpack["SubscriptionItemDeleteParams"]
    ) -> "SubscriptionItem":
        """
        Deletes an item from the subscription. Removing a subscription item from a subscription will not cancel the subscription.
        """
        ...

    @overload
    def delete(
        self, **params: Unpack["SubscriptionItemDeleteParams"]
    ) -> "SubscriptionItem":
        """
        Deletes an item from the subscription. Removing a subscription item from a subscription will not cancel the subscription.
        """
        ...

    @class_method_variant("_cls_delete")
    def delete(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["SubscriptionItemDeleteParams"]
    ) -> "SubscriptionItem":
        """
        Deletes an item from the subscription. Removing a subscription item from a subscription will not cancel the subscription.
        """
        return self._request_and_refresh(
            "delete",
            self.instance_url(),
            params=params,
        )

    @classmethod
    async def _cls_delete_async(
        cls, sid: str, **params: Unpack["SubscriptionItemDeleteParams"]
    ) -> "SubscriptionItem":
        """
        Deletes an item from the subscription. Removing a subscription item from a subscription will not cancel the subscription.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(sid))
        return cast(
            "SubscriptionItem",
            await cls._static_request_async(
                "delete",
                url,
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def delete_async(
        sid: str, **params: Unpack["SubscriptionItemDeleteParams"]
    ) -> "SubscriptionItem":
        """
        Deletes an item from the subscription. Removing a subscription item from a subscription will not cancel the subscription.
        """
        ...

    @overload
    async def delete_async(
        self, **params: Unpack["SubscriptionItemDeleteParams"]
    ) -> "SubscriptionItem":
        """
        Deletes an item from the subscription. Removing a subscription item from a subscription will not cancel the subscription.
        """
        ...

    @class_method_variant("_cls_delete_async")
    async def delete_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["SubscriptionItemDeleteParams"]
    ) -> "SubscriptionItem":
        """
        Deletes an item from the subscription. Removing a subscription item from a subscription will not cancel the subscription.
        """
        return await self._request_and_refresh_async(
            "delete",
            self.instance_url(),
            params=params,
        )

    @classmethod
    def list(
        cls, **params: Unpack["SubscriptionItemListParams"]
    ) -> ListObject["SubscriptionItem"]:
        """
        Returns a list of your subscription items for a given subscription.
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
        cls, **params: Unpack["SubscriptionItemListParams"]
    ) -> ListObject["SubscriptionItem"]:
        """
        Returns a list of your subscription items for a given subscription.
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
        cls, id: str, **params: Unpack["SubscriptionItemModifyParams"]
    ) -> "SubscriptionItem":
        """
        Updates the plan or quantity of an item on a current subscription.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "SubscriptionItem",
            cls._static_request(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    async def modify_async(
        cls, id: str, **params: Unpack["SubscriptionItemModifyParams"]
    ) -> "SubscriptionItem":
        """
        Updates the plan or quantity of an item on a current subscription.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "SubscriptionItem",
            await cls._static_request_async(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["SubscriptionItemRetrieveParams"]
    ) -> "SubscriptionItem":
        """
        Retrieves the subscription item with the given ID.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["SubscriptionItemRetrieveParams"]
    ) -> "SubscriptionItem":
        """
        Retrieves the subscription item with the given ID.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    _inner_class_types = {"billing_thresholds": BillingThresholds}
