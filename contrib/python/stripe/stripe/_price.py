# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._expandable_field import ExpandableField
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._search_result_object import SearchResultObject
from stripe._searchable_api_resource import SearchableAPIResource
from stripe._stripe_object import StripeObject
from stripe._updateable_api_resource import UpdateableAPIResource
from stripe._util import sanitize_id
from typing import (
    AsyncIterator,
    ClassVar,
    Dict,
    Iterator,
    List,
    Optional,
    cast,
)
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._product import Product
    from stripe.params._price_create_params import PriceCreateParams
    from stripe.params._price_list_params import PriceListParams
    from stripe.params._price_modify_params import PriceModifyParams
    from stripe.params._price_retrieve_params import PriceRetrieveParams
    from stripe.params._price_search_params import PriceSearchParams


class Price(
    CreateableAPIResource["Price"],
    ListableAPIResource["Price"],
    SearchableAPIResource["Price"],
    UpdateableAPIResource["Price"],
):
    """
    Prices define the unit cost, currency, and (optional) billing cycle for both recurring and one-time purchases of products.
    [Products](https://api.stripe.com#products) help you track inventory or provisioning, and prices help you track payment terms. Different physical goods or levels of service should be represented by products, and pricing options should be represented by prices. This approach lets you change prices without having to change your provisioning scheme.

    For example, you might have a single "gold" product that has prices for $10/month, $100/year, and €9 once.

    Related guides: [Set up a subscription](https://docs.stripe.com/billing/subscriptions/set-up-subscription), [create an invoice](https://docs.stripe.com/billing/invoices/create), and more about [products and prices](https://docs.stripe.com/products-prices/overview).
    """

    OBJECT_NAME: ClassVar[Literal["price"]] = "price"

    class CurrencyOptions(StripeObject):
        class CustomUnitAmount(StripeObject):
            maximum: Optional[int]
            """
            The maximum unit amount the customer can specify for this item.
            """
            minimum: Optional[int]
            """
            The minimum unit amount the customer can specify for this item. Must be at least the minimum charge amount.
            """
            preset: Optional[int]
            """
            The starting unit amount which can be updated by the customer.
            """

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

        custom_unit_amount: Optional[CustomUnitAmount]
        """
        When set, provides configuration for the amount to be adjusted by the customer during Checkout Sessions and Payment Links.
        """
        tax_behavior: Optional[
            Literal["exclusive", "inclusive", "unspecified"]
        ]
        """
        Only required if a [default tax behavior](https://docs.stripe.com/tax/products-prices-tax-categories-tax-behavior#setting-a-default-tax-behavior-(recommended)) was not provided in the Stripe Tax settings. Specifies whether the price is considered inclusive of taxes or exclusive of taxes. One of `inclusive`, `exclusive`, or `unspecified`. Once specified as either `inclusive` or `exclusive`, it cannot be changed.
        """
        tiers: Optional[List[Tier]]
        """
        Each element represents a pricing tier. This parameter requires `billing_scheme` to be set to `tiered`. See also the documentation for `billing_scheme`.
        """
        unit_amount: Optional[int]
        """
        The unit amount in cents (or local equivalent) to be charged, represented as a whole integer if possible. Only set if `billing_scheme=per_unit`.
        """
        unit_amount_decimal: Optional[str]
        """
        The unit amount in cents (or local equivalent) to be charged, represented as a decimal string with at most 12 decimal places. Only set if `billing_scheme=per_unit`.
        """
        _inner_class_types = {
            "custom_unit_amount": CustomUnitAmount,
            "tiers": Tier,
        }

    class CustomUnitAmount(StripeObject):
        maximum: Optional[int]
        """
        The maximum unit amount the customer can specify for this item.
        """
        minimum: Optional[int]
        """
        The minimum unit amount the customer can specify for this item. Must be at least the minimum charge amount.
        """
        preset: Optional[int]
        """
        The starting unit amount which can be updated by the customer.
        """

    class Recurring(StripeObject):
        interval: Literal["day", "month", "week", "year"]
        """
        The frequency at which a subscription is billed. One of `day`, `week`, `month` or `year`.
        """
        interval_count: int
        """
        The number of intervals (specified in the `interval` attribute) between subscription billings. For example, `interval=month` and `interval_count=3` bills every 3 months.
        """
        meter: Optional[str]
        """
        The meter tracking the usage of a metered price
        """
        trial_period_days: Optional[int]
        """
        Default number of trial days when subscribing a customer to this price using [`trial_from_plan=true`](https://docs.stripe.com/api#create_subscription-trial_from_plan).
        """
        usage_type: Literal["licensed", "metered"]
        """
        Configures how the quantity per period should be determined. Can be either `metered` or `licensed`. `licensed` automatically bills the `quantity` set when adding it to a subscription. `metered` aggregates the total usage based on usage records. Defaults to `licensed`.
        """

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

    class TransformQuantity(StripeObject):
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
    Whether the price can be used for new purchases.
    """
    billing_scheme: Literal["per_unit", "tiered"]
    """
    Describes how to compute the price per period. Either `per_unit` or `tiered`. `per_unit` indicates that the fixed amount (specified in `unit_amount` or `unit_amount_decimal`) will be charged per unit in `quantity` (for prices with `usage_type=licensed`), or per unit of total usage (for prices with `usage_type=metered`). `tiered` indicates that the unit pricing will be computed using a tiering strategy as defined using the `tiers` and `tiers_mode` attributes.
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    currency_options: Optional[Dict[str, CurrencyOptions]]
    """
    Prices defined in each available currency option. Each key must be a three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html) and a [supported currency](https://stripe.com/docs/currencies).
    """
    custom_unit_amount: Optional[CustomUnitAmount]
    """
    When set, provides configuration for the amount to be adjusted by the customer during Checkout Sessions and Payment Links.
    """
    deleted: Optional[Literal[True]]
    """
    Always true for a deleted object
    """
    id: str
    """
    Unique identifier for the object.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    lookup_key: Optional[str]
    """
    A lookup key used to retrieve prices dynamically from a static string. This may be up to 200 characters.
    """
    metadata: Dict[str, str]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    nickname: Optional[str]
    """
    A brief description of the price, hidden from customers.
    """
    object: Literal["price"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    product: ExpandableField["Product"]
    """
    The ID of the product this price is associated with.
    """
    recurring: Optional[Recurring]
    """
    The recurring components of a price such as `interval` and `usage_type`.
    """
    tax_behavior: Optional[Literal["exclusive", "inclusive", "unspecified"]]
    """
    Only required if a [default tax behavior](https://docs.stripe.com/tax/products-prices-tax-categories-tax-behavior#setting-a-default-tax-behavior-(recommended)) was not provided in the Stripe Tax settings. Specifies whether the price is considered inclusive of taxes or exclusive of taxes. One of `inclusive`, `exclusive`, or `unspecified`. Once specified as either `inclusive` or `exclusive`, it cannot be changed.
    """
    tiers: Optional[List[Tier]]
    """
    Each element represents a pricing tier. This parameter requires `billing_scheme` to be set to `tiered`. See also the documentation for `billing_scheme`.
    """
    tiers_mode: Optional[Literal["graduated", "volume"]]
    """
    Defines if the tiering price should be `graduated` or `volume` based. In `volume`-based tiering, the maximum quantity within a period determines the per unit price. In `graduated` tiering, pricing can change as the quantity grows.
    """
    transform_quantity: Optional[TransformQuantity]
    """
    Apply a transformation to the reported usage or set quantity before computing the amount billed. Cannot be combined with `tiers`.
    """
    type: Literal["one_time", "recurring"]
    """
    One of `one_time` or `recurring` depending on whether the price is for a one-time purchase or a recurring (subscription) purchase.
    """
    unit_amount: Optional[int]
    """
    The unit amount in cents (or local equivalent) to be charged, represented as a whole integer if possible. Only set if `billing_scheme=per_unit`.
    """
    unit_amount_decimal: Optional[str]
    """
    The unit amount in cents (or local equivalent) to be charged, represented as a decimal string with at most 12 decimal places. Only set if `billing_scheme=per_unit`.
    """

    @classmethod
    def create(cls, **params: Unpack["PriceCreateParams"]) -> "Price":
        """
        Creates a new [Price for an existing <a href="https://docs.stripe.com/api/products">Product](https://docs.stripe.com/api/prices). The Price can be recurring or one-time.
        """
        return cast(
            "Price",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["PriceCreateParams"]
    ) -> "Price":
        """
        Creates a new [Price for an existing <a href="https://docs.stripe.com/api/products">Product](https://docs.stripe.com/api/prices). The Price can be recurring or one-time.
        """
        return cast(
            "Price",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def list(cls, **params: Unpack["PriceListParams"]) -> ListObject["Price"]:
        """
        Returns a list of your active prices, excluding [inline prices](https://docs.stripe.com/docs/products-prices/pricing-models#inline-pricing). For the list of inactive prices, set active to false.
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
        cls, **params: Unpack["PriceListParams"]
    ) -> ListObject["Price"]:
        """
        Returns a list of your active prices, excluding [inline prices](https://docs.stripe.com/docs/products-prices/pricing-models#inline-pricing). For the list of inactive prices, set active to false.
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
    def modify(cls, id: str, **params: Unpack["PriceModifyParams"]) -> "Price":
        """
        Updates the specified price by setting the values of the parameters passed. Any parameters not provided are left unchanged.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Price",
            cls._static_request(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    async def modify_async(
        cls, id: str, **params: Unpack["PriceModifyParams"]
    ) -> "Price":
        """
        Updates the specified price by setting the values of the parameters passed. Any parameters not provided are left unchanged.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Price",
            await cls._static_request_async(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["PriceRetrieveParams"]
    ) -> "Price":
        """
        Retrieves the price with the given ID.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["PriceRetrieveParams"]
    ) -> "Price":
        """
        Retrieves the price with the given ID.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    @classmethod
    def search(
        cls, *args, **kwargs: Unpack["PriceSearchParams"]
    ) -> SearchResultObject["Price"]:
        """
        Search for prices you've previously created using Stripe's [Search Query Language](https://docs.stripe.com/docs/search#search-query-language).
        Don't use search in read-after-write flows where strict consistency is necessary. Under normal operating
        conditions, data is searchable in less than a minute. Occasionally, propagation of new or updated data can be up
        to an hour behind during outages. Search functionality is not available to merchants in India.
        """
        return cls._search(search_url="/v1/prices/search", *args, **kwargs)

    @classmethod
    async def search_async(
        cls, *args, **kwargs: Unpack["PriceSearchParams"]
    ) -> SearchResultObject["Price"]:
        """
        Search for prices you've previously created using Stripe's [Search Query Language](https://docs.stripe.com/docs/search#search-query-language).
        Don't use search in read-after-write flows where strict consistency is necessary. Under normal operating
        conditions, data is searchable in less than a minute. Occasionally, propagation of new or updated data can be up
        to an hour behind during outages. Search functionality is not available to merchants in India.
        """
        return await cls._search_async(
            search_url="/v1/prices/search", *args, **kwargs
        )

    @classmethod
    def search_auto_paging_iter(
        cls, *args, **kwargs: Unpack["PriceSearchParams"]
    ) -> Iterator["Price"]:
        return cls.search(*args, **kwargs).auto_paging_iter()

    @classmethod
    async def search_auto_paging_iter_async(
        cls, *args, **kwargs: Unpack["PriceSearchParams"]
    ) -> AsyncIterator["Price"]:
        return (await cls.search_async(*args, **kwargs)).auto_paging_iter()

    _inner_class_types = {
        "currency_options": CurrencyOptions,
        "custom_unit_amount": CustomUnitAmount,
        "recurring": Recurring,
        "tiers": Tier,
        "transform_quantity": TransformQuantity,
    }
