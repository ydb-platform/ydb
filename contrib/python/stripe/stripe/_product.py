# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._deletable_api_resource import DeletableAPIResource
from stripe._expandable_field import ExpandableField
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._nested_resource_class_methods import nested_resource_class_methods
from stripe._search_result_object import SearchResultObject
from stripe._searchable_api_resource import SearchableAPIResource
from stripe._stripe_object import StripeObject
from stripe._updateable_api_resource import UpdateableAPIResource
from stripe._util import class_method_variant, sanitize_id
from typing import (
    AsyncIterator,
    ClassVar,
    Dict,
    Iterator,
    List,
    Optional,
    cast,
    overload,
)
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._price import Price
    from stripe._product_feature import ProductFeature
    from stripe._tax_code import TaxCode
    from stripe.params._product_create_feature_params import (
        ProductCreateFeatureParams,
    )
    from stripe.params._product_create_params import ProductCreateParams
    from stripe.params._product_delete_feature_params import (
        ProductDeleteFeatureParams,
    )
    from stripe.params._product_delete_params import ProductDeleteParams
    from stripe.params._product_list_features_params import (
        ProductListFeaturesParams,
    )
    from stripe.params._product_list_params import ProductListParams
    from stripe.params._product_modify_params import ProductModifyParams
    from stripe.params._product_retrieve_feature_params import (
        ProductRetrieveFeatureParams,
    )
    from stripe.params._product_retrieve_params import ProductRetrieveParams
    from stripe.params._product_search_params import ProductSearchParams


@nested_resource_class_methods("feature")
class Product(
    CreateableAPIResource["Product"],
    DeletableAPIResource["Product"],
    ListableAPIResource["Product"],
    SearchableAPIResource["Product"],
    UpdateableAPIResource["Product"],
):
    """
    Products describe the specific goods or services you offer to your customers.
    For example, you might offer a Standard and Premium version of your goods or service; each version would be a separate Product.
    They can be used in conjunction with [Prices](https://api.stripe.com#prices) to configure pricing in Payment Links, Checkout, and Subscriptions.

    Related guides: [Set up a subscription](https://docs.stripe.com/billing/subscriptions/set-up-subscription),
    [share a Payment Link](https://docs.stripe.com/payment-links),
    [accept payments with Checkout](https://docs.stripe.com/payments/accept-a-payment#create-product-prices-upfront),
    and more about [Products and Prices](https://docs.stripe.com/products-prices/overview)
    """

    OBJECT_NAME: ClassVar[Literal["product"]] = "product"

    class MarketingFeature(StripeObject):
        name: Optional[str]
        """
        The marketing feature name. Up to 80 characters long.
        """

    class PackageDimensions(StripeObject):
        height: float
        """
        Height, in inches.
        """
        length: float
        """
        Length, in inches.
        """
        weight: float
        """
        Weight, in ounces.
        """
        width: float
        """
        Width, in inches.
        """

    active: bool
    """
    Whether the product is currently available for purchase.
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    default_price: Optional[ExpandableField["Price"]]
    """
    The ID of the [Price](https://docs.stripe.com/api/prices) object that is the default price for this product.
    """
    deleted: Optional[Literal[True]]
    """
    Always true for a deleted object
    """
    description: Optional[str]
    """
    The product's description, meant to be displayable to the customer. Use this field to optionally store a long form explanation of the product being sold for your own rendering purposes.
    """
    id: str
    """
    Unique identifier for the object.
    """
    images: List[str]
    """
    A list of up to 8 URLs of images for this product, meant to be displayable to the customer.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    marketing_features: List[MarketingFeature]
    """
    A list of up to 15 marketing features for this product. These are displayed in [pricing tables](https://docs.stripe.com/payments/checkout/pricing-table).
    """
    metadata: Dict[str, str]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    name: str
    """
    The product's name, meant to be displayable to the customer.
    """
    object: Literal["product"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    package_dimensions: Optional[PackageDimensions]
    """
    The dimensions of this product for shipping purposes.
    """
    shippable: Optional[bool]
    """
    Whether this product is shipped (i.e., physical goods).
    """
    statement_descriptor: Optional[str]
    """
    Extra information about a product which will appear on your customer's credit card statement. In the case that multiple products are billed at once, the first statement descriptor will be used. Only used for subscription payments.
    """
    tax_code: Optional[ExpandableField["TaxCode"]]
    """
    A [tax code](https://docs.stripe.com/tax/tax-categories) ID.
    """
    type: Literal["good", "service"]
    """
    The type of the product. The product is either of type `good`, which is eligible for use with Orders and SKUs, or `service`, which is eligible for use with Subscriptions and Plans.
    """
    unit_label: Optional[str]
    """
    A label that represents units of this product. When set, this will be included in customers' receipts, invoices, Checkout, and the customer portal.
    """
    updated: int
    """
    Time at which the object was last updated. Measured in seconds since the Unix epoch.
    """
    url: Optional[str]
    """
    A URL of a publicly-accessible webpage for this product.
    """

    @classmethod
    def create(cls, **params: Unpack["ProductCreateParams"]) -> "Product":
        """
        Creates a new product object.
        """
        return cast(
            "Product",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["ProductCreateParams"]
    ) -> "Product":
        """
        Creates a new product object.
        """
        return cast(
            "Product",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def _cls_delete(
        cls, sid: str, **params: Unpack["ProductDeleteParams"]
    ) -> "Product":
        """
        Delete a product. Deleting a product is only possible if it has no prices associated with it. Additionally, deleting a product with type=good is only possible if it has no SKUs associated with it.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(sid))
        return cast(
            "Product",
            cls._static_request(
                "delete",
                url,
                params=params,
            ),
        )

    @overload
    @staticmethod
    def delete(sid: str, **params: Unpack["ProductDeleteParams"]) -> "Product":
        """
        Delete a product. Deleting a product is only possible if it has no prices associated with it. Additionally, deleting a product with type=good is only possible if it has no SKUs associated with it.
        """
        ...

    @overload
    def delete(self, **params: Unpack["ProductDeleteParams"]) -> "Product":
        """
        Delete a product. Deleting a product is only possible if it has no prices associated with it. Additionally, deleting a product with type=good is only possible if it has no SKUs associated with it.
        """
        ...

    @class_method_variant("_cls_delete")
    def delete(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["ProductDeleteParams"]
    ) -> "Product":
        """
        Delete a product. Deleting a product is only possible if it has no prices associated with it. Additionally, deleting a product with type=good is only possible if it has no SKUs associated with it.
        """
        return self._request_and_refresh(
            "delete",
            self.instance_url(),
            params=params,
        )

    @classmethod
    async def _cls_delete_async(
        cls, sid: str, **params: Unpack["ProductDeleteParams"]
    ) -> "Product":
        """
        Delete a product. Deleting a product is only possible if it has no prices associated with it. Additionally, deleting a product with type=good is only possible if it has no SKUs associated with it.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(sid))
        return cast(
            "Product",
            await cls._static_request_async(
                "delete",
                url,
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def delete_async(
        sid: str, **params: Unpack["ProductDeleteParams"]
    ) -> "Product":
        """
        Delete a product. Deleting a product is only possible if it has no prices associated with it. Additionally, deleting a product with type=good is only possible if it has no SKUs associated with it.
        """
        ...

    @overload
    async def delete_async(
        self, **params: Unpack["ProductDeleteParams"]
    ) -> "Product":
        """
        Delete a product. Deleting a product is only possible if it has no prices associated with it. Additionally, deleting a product with type=good is only possible if it has no SKUs associated with it.
        """
        ...

    @class_method_variant("_cls_delete_async")
    async def delete_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["ProductDeleteParams"]
    ) -> "Product":
        """
        Delete a product. Deleting a product is only possible if it has no prices associated with it. Additionally, deleting a product with type=good is only possible if it has no SKUs associated with it.
        """
        return await self._request_and_refresh_async(
            "delete",
            self.instance_url(),
            params=params,
        )

    @classmethod
    def list(
        cls, **params: Unpack["ProductListParams"]
    ) -> ListObject["Product"]:
        """
        Returns a list of your products. The products are returned sorted by creation date, with the most recently created products appearing first.
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
        cls, **params: Unpack["ProductListParams"]
    ) -> ListObject["Product"]:
        """
        Returns a list of your products. The products are returned sorted by creation date, with the most recently created products appearing first.
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
        cls, id: str, **params: Unpack["ProductModifyParams"]
    ) -> "Product":
        """
        Updates the specific product by setting the values of the parameters passed. Any parameters not provided will be left unchanged.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Product",
            cls._static_request(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    async def modify_async(
        cls, id: str, **params: Unpack["ProductModifyParams"]
    ) -> "Product":
        """
        Updates the specific product by setting the values of the parameters passed. Any parameters not provided will be left unchanged.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Product",
            await cls._static_request_async(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["ProductRetrieveParams"]
    ) -> "Product":
        """
        Retrieves the details of an existing product. Supply the unique product ID from either a product creation request or the product list, and Stripe will return the corresponding product information.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["ProductRetrieveParams"]
    ) -> "Product":
        """
        Retrieves the details of an existing product. Supply the unique product ID from either a product creation request or the product list, and Stripe will return the corresponding product information.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    @classmethod
    def search(
        cls, *args, **kwargs: Unpack["ProductSearchParams"]
    ) -> SearchResultObject["Product"]:
        """
        Search for products you've previously created using Stripe's [Search Query Language](https://docs.stripe.com/docs/search#search-query-language).
        Don't use search in read-after-write flows where strict consistency is necessary. Under normal operating
        conditions, data is searchable in less than a minute. Occasionally, propagation of new or updated data can be up
        to an hour behind during outages. Search functionality is not available to merchants in India.
        """
        return cls._search(search_url="/v1/products/search", *args, **kwargs)

    @classmethod
    async def search_async(
        cls, *args, **kwargs: Unpack["ProductSearchParams"]
    ) -> SearchResultObject["Product"]:
        """
        Search for products you've previously created using Stripe's [Search Query Language](https://docs.stripe.com/docs/search#search-query-language).
        Don't use search in read-after-write flows where strict consistency is necessary. Under normal operating
        conditions, data is searchable in less than a minute. Occasionally, propagation of new or updated data can be up
        to an hour behind during outages. Search functionality is not available to merchants in India.
        """
        return await cls._search_async(
            search_url="/v1/products/search", *args, **kwargs
        )

    @classmethod
    def search_auto_paging_iter(
        cls, *args, **kwargs: Unpack["ProductSearchParams"]
    ) -> Iterator["Product"]:
        return cls.search(*args, **kwargs).auto_paging_iter()

    @classmethod
    async def search_auto_paging_iter_async(
        cls, *args, **kwargs: Unpack["ProductSearchParams"]
    ) -> AsyncIterator["Product"]:
        return (await cls.search_async(*args, **kwargs)).auto_paging_iter()

    @classmethod
    def delete_feature(
        cls,
        product: str,
        id: str,
        **params: Unpack["ProductDeleteFeatureParams"],
    ) -> "ProductFeature":
        """
        Deletes the feature attachment to a product
        """
        return cast(
            "ProductFeature",
            cls._static_request(
                "delete",
                "/v1/products/{product}/features/{id}".format(
                    product=sanitize_id(product), id=sanitize_id(id)
                ),
                params=params,
            ),
        )

    @classmethod
    async def delete_feature_async(
        cls,
        product: str,
        id: str,
        **params: Unpack["ProductDeleteFeatureParams"],
    ) -> "ProductFeature":
        """
        Deletes the feature attachment to a product
        """
        return cast(
            "ProductFeature",
            await cls._static_request_async(
                "delete",
                "/v1/products/{product}/features/{id}".format(
                    product=sanitize_id(product), id=sanitize_id(id)
                ),
                params=params,
            ),
        )

    @classmethod
    def retrieve_feature(
        cls,
        product: str,
        id: str,
        **params: Unpack["ProductRetrieveFeatureParams"],
    ) -> "ProductFeature":
        """
        Retrieves a product_feature, which represents a feature attachment to a product
        """
        return cast(
            "ProductFeature",
            cls._static_request(
                "get",
                "/v1/products/{product}/features/{id}".format(
                    product=sanitize_id(product), id=sanitize_id(id)
                ),
                params=params,
            ),
        )

    @classmethod
    async def retrieve_feature_async(
        cls,
        product: str,
        id: str,
        **params: Unpack["ProductRetrieveFeatureParams"],
    ) -> "ProductFeature":
        """
        Retrieves a product_feature, which represents a feature attachment to a product
        """
        return cast(
            "ProductFeature",
            await cls._static_request_async(
                "get",
                "/v1/products/{product}/features/{id}".format(
                    product=sanitize_id(product), id=sanitize_id(id)
                ),
                params=params,
            ),
        )

    @classmethod
    def list_features(
        cls, product: str, **params: Unpack["ProductListFeaturesParams"]
    ) -> ListObject["ProductFeature"]:
        """
        Retrieve a list of features for a product
        """
        return cast(
            ListObject["ProductFeature"],
            cls._static_request(
                "get",
                "/v1/products/{product}/features".format(
                    product=sanitize_id(product)
                ),
                params=params,
            ),
        )

    @classmethod
    async def list_features_async(
        cls, product: str, **params: Unpack["ProductListFeaturesParams"]
    ) -> ListObject["ProductFeature"]:
        """
        Retrieve a list of features for a product
        """
        return cast(
            ListObject["ProductFeature"],
            await cls._static_request_async(
                "get",
                "/v1/products/{product}/features".format(
                    product=sanitize_id(product)
                ),
                params=params,
            ),
        )

    @classmethod
    def create_feature(
        cls, product: str, **params: Unpack["ProductCreateFeatureParams"]
    ) -> "ProductFeature":
        """
        Creates a product_feature, which represents a feature attachment to a product
        """
        return cast(
            "ProductFeature",
            cls._static_request(
                "post",
                "/v1/products/{product}/features".format(
                    product=sanitize_id(product)
                ),
                params=params,
            ),
        )

    @classmethod
    async def create_feature_async(
        cls, product: str, **params: Unpack["ProductCreateFeatureParams"]
    ) -> "ProductFeature":
        """
        Creates a product_feature, which represents a feature attachment to a product
        """
        return cast(
            "ProductFeature",
            await cls._static_request_async(
                "post",
                "/v1/products/{product}/features".format(
                    product=sanitize_id(product)
                ),
                params=params,
            ),
        )

    _inner_class_types = {
        "marketing_features": MarketingFeature,
        "package_dimensions": PackageDimensions,
    }
