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
    from stripe._product import Product
    from stripe.params._plan_create_params import PlanCreateParams
    from stripe.params._plan_delete_params import PlanDeleteParams
    from stripe.params._plan_list_params import PlanListParams
    from stripe.params._plan_modify_params import PlanModifyParams
    from stripe.params._plan_retrieve_params import PlanRetrieveParams


class Plan(
    CreateableAPIResource["Plan"],
    DeletableAPIResource["Plan"],
    ListableAPIResource["Plan"],
    UpdateableAPIResource["Plan"],
):
    """
    You can now model subscriptions more flexibly using the [Prices API](https://api.stripe.com#prices). It replaces the Plans API and is backwards compatible to simplify your migration.

    Plans define the base price, currency, and billing cycle for recurring purchases of products.
    [Products](https://api.stripe.com#products) help you track inventory or provisioning, and plans help you track pricing. Different physical goods or levels of service should be represented by products, and pricing options should be represented by plans. This approach lets you change prices without having to change your provisioning scheme.

    For example, you might have a single "gold" product that has plans for $10/month, $100/year, €9/month, and €90/year.

    Related guides: [Set up a subscription](https://docs.stripe.com/billing/subscriptions/set-up-subscription) and more about [products and prices](https://docs.stripe.com/products-prices/overview).
    """

    OBJECT_NAME: ClassVar[Literal["plan"]] = "plan"

    class Tier(StripeObject):
        flat_amount: Optional[int]
        """
        Price for the entire tier.
        """
        flat_amount_decimal: Optional[str]
        """
        Same as `flat_amount`, but contains a decimal value with at most 12 decimal places.
        """
        unit_amount: Optional[int]
        """
        Per unit price for units relevant to the tier.
        """
        unit_amount_decimal: Optional[str]
        """
        Same as `unit_amount`, but contains a decimal value with at most 12 decimal places.
        """
        up_to: Optional[int]
        """
        Up to and including to this quantity will be contained in the tier.
        """

    class TransformUsage(StripeObject):
        divide_by: int
        """
        Divide usage by this number.
        """
        round: Literal["down", "up"]
        """
        After division, either round the result `up` or `down`.
        """

    active: bool
    """
    Whether the plan can be used for new purchases.
    """
    amount: Optional[int]
    """
    The unit amount in cents (or local equivalent) to be charged, represented as a whole integer if possible. Only set if `billing_scheme=per_unit`.
    """
    amount_decimal: Optional[str]
    """
    The unit amount in cents (or local equivalent) to be charged, represented as a decimal string with at most 12 decimal places. Only set if `billing_scheme=per_unit`.
    """
    billing_scheme: Literal["per_unit", "tiered"]
    """
    Describes how to compute the price per period. Either `per_unit` or `tiered`. `per_unit` indicates that the fixed amount (specified in `amount`) will be charged per unit in `quantity` (for plans with `usage_type=licensed`), or per unit of total usage (for plans with `usage_type=metered`). `tiered` indicates that the unit pricing will be computed using a tiering strategy as defined using the `tiers` and `tiers_mode` attributes.
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    deleted: Optional[Literal[True]]
    """
    Always true for a deleted object
    """
    id: str
    """
    Unique identifier for the object.
    """
    interval: Literal["day", "month", "week", "year"]
    """
    The frequency at which a subscription is billed. One of `day`, `week`, `month` or `year`.
    """
    interval_count: int
    """
    The number of intervals (specified in the `interval` attribute) between subscription billings. For example, `interval=month` and `interval_count=3` bills every 3 months.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    metadata: Optional[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    meter: Optional[str]
    """
    The meter tracking the usage of a metered price
    """
    nickname: Optional[str]
    """
    A brief description of the plan, hidden from customers.
    """
    object: Literal["plan"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    product: Optional[ExpandableField["Product"]]
    """
    The product whose pricing this plan determines.
    """
    tiers: Optional[List[Tier]]
    """
    Each element represents a pricing tier. This parameter requires `billing_scheme` to be set to `tiered`. See also the documentation for `billing_scheme`.
    """
    tiers_mode: Optional[Literal["graduated", "volume"]]
    """
    Defines if the tiering price should be `graduated` or `volume` based. In `volume`-based tiering, the maximum quantity within a period determines the per unit price. In `graduated` tiering, pricing can change as the quantity grows.
    """
    transform_usage: Optional[TransformUsage]
    """
    Apply a transformation to the reported usage or set quantity before computing the amount billed. Cannot be combined with `tiers`.
    """
    trial_period_days: Optional[int]
    """
    Default number of trial days when subscribing a customer to this plan using [`trial_from_plan=true`](https://docs.stripe.com/api#create_subscription-trial_from_plan).
    """
    usage_type: Literal["licensed", "metered"]
    """
    Configures how the quantity per period should be determined. Can be either `metered` or `licensed`. `licensed` automatically bills the `quantity` set when adding it to a subscription. `metered` aggregates the total usage based on usage records. Defaults to `licensed`.
    """

    @classmethod
    def create(cls, **params: Unpack["PlanCreateParams"]) -> "Plan":
        """
        You can now model subscriptions more flexibly using the [Prices API](https://docs.stripe.com/api#prices). It replaces the Plans API and is backwards compatible to simplify your migration.
        """
        return cast(
            "Plan",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["PlanCreateParams"]
    ) -> "Plan":
        """
        You can now model subscriptions more flexibly using the [Prices API](https://docs.stripe.com/api#prices). It replaces the Plans API and is backwards compatible to simplify your migration.
        """
        return cast(
            "Plan",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def _cls_delete(
        cls, sid: str, **params: Unpack["PlanDeleteParams"]
    ) -> "Plan":
        """
        Deleting plans means new subscribers can't be added. Existing subscribers aren't affected.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(sid))
        return cast(
            "Plan",
            cls._static_request(
                "delete",
                url,
                params=params,
            ),
        )

    @overload
    @staticmethod
    def delete(sid: str, **params: Unpack["PlanDeleteParams"]) -> "Plan":
        """
        Deleting plans means new subscribers can't be added. Existing subscribers aren't affected.
        """
        ...

    @overload
    def delete(self, **params: Unpack["PlanDeleteParams"]) -> "Plan":
        """
        Deleting plans means new subscribers can't be added. Existing subscribers aren't affected.
        """
        ...

    @class_method_variant("_cls_delete")
    def delete(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["PlanDeleteParams"]
    ) -> "Plan":
        """
        Deleting plans means new subscribers can't be added. Existing subscribers aren't affected.
        """
        return self._request_and_refresh(
            "delete",
            self.instance_url(),
            params=params,
        )

    @classmethod
    async def _cls_delete_async(
        cls, sid: str, **params: Unpack["PlanDeleteParams"]
    ) -> "Plan":
        """
        Deleting plans means new subscribers can't be added. Existing subscribers aren't affected.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(sid))
        return cast(
            "Plan",
            await cls._static_request_async(
                "delete",
                url,
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def delete_async(
        sid: str, **params: Unpack["PlanDeleteParams"]
    ) -> "Plan":
        """
        Deleting plans means new subscribers can't be added. Existing subscribers aren't affected.
        """
        ...

    @overload
    async def delete_async(
        self, **params: Unpack["PlanDeleteParams"]
    ) -> "Plan":
        """
        Deleting plans means new subscribers can't be added. Existing subscribers aren't affected.
        """
        ...

    @class_method_variant("_cls_delete_async")
    async def delete_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["PlanDeleteParams"]
    ) -> "Plan":
        """
        Deleting plans means new subscribers can't be added. Existing subscribers aren't affected.
        """
        return await self._request_and_refresh_async(
            "delete",
            self.instance_url(),
            params=params,
        )

    @classmethod
    def list(cls, **params: Unpack["PlanListParams"]) -> ListObject["Plan"]:
        """
        Returns a list of your plans.
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
        cls, **params: Unpack["PlanListParams"]
    ) -> ListObject["Plan"]:
        """
        Returns a list of your plans.
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
    def modify(cls, id: str, **params: Unpack["PlanModifyParams"]) -> "Plan":
        """
        Updates the specified plan by setting the values of the parameters passed. Any parameters not provided are left unchanged. By design, you cannot change a plan's ID, amount, currency, or billing cycle.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Plan",
            cls._static_request(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    async def modify_async(
        cls, id: str, **params: Unpack["PlanModifyParams"]
    ) -> "Plan":
        """
        Updates the specified plan by setting the values of the parameters passed. Any parameters not provided are left unchanged. By design, you cannot change a plan's ID, amount, currency, or billing cycle.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Plan",
            await cls._static_request_async(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["PlanRetrieveParams"]
    ) -> "Plan":
        """
        Retrieves the plan with the given ID.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["PlanRetrieveParams"]
    ) -> "Plan":
        """
        Retrieves the plan with the given ID.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    _inner_class_types = {"tiers": Tier, "transform_usage": TransformUsage}
