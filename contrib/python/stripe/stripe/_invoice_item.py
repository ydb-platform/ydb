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
    from stripe._customer import Customer
    from stripe._discount import Discount
    from stripe._invoice import Invoice
    from stripe._price import Price
    from stripe._tax_rate import TaxRate
    from stripe.params._invoice_item_create_params import (
        InvoiceItemCreateParams,
    )
    from stripe.params._invoice_item_delete_params import (
        InvoiceItemDeleteParams,
    )
    from stripe.params._invoice_item_list_params import InvoiceItemListParams
    from stripe.params._invoice_item_modify_params import (
        InvoiceItemModifyParams,
    )
    from stripe.params._invoice_item_retrieve_params import (
        InvoiceItemRetrieveParams,
    )
    from stripe.test_helpers._test_clock import TestClock


class InvoiceItem(
    CreateableAPIResource["InvoiceItem"],
    DeletableAPIResource["InvoiceItem"],
    ListableAPIResource["InvoiceItem"],
    UpdateableAPIResource["InvoiceItem"],
):
    """
    Invoice Items represent the component lines of an [invoice](https://docs.stripe.com/api/invoices). When you create an invoice item with an `invoice` field, it is attached to the specified invoice and included as [an invoice line item](https://docs.stripe.com/api/invoices/line_item) within [invoice.lines](https://docs.stripe.com/api/invoices/object#invoice_object-lines).

    Invoice Items can be created before you are ready to actually send the invoice. This can be particularly useful when combined
    with a [subscription](https://docs.stripe.com/api/subscriptions). Sometimes you want to add a charge or credit to a customer, but actually charge
    or credit the customer's card only at the end of a regular billing cycle. This is useful for combining several charges
    (to minimize per-transaction fees), or for having Stripe tabulate your usage-based billing totals.

    Related guides: [Integrate with the Invoicing API](https://docs.stripe.com/invoicing/integration), [Subscription Invoices](https://docs.stripe.com/billing/invoices/subscription#adding-upcoming-invoice-items).
    """

    OBJECT_NAME: ClassVar[Literal["invoiceitem"]] = "invoiceitem"

    class Parent(StripeObject):
        class SubscriptionDetails(StripeObject):
            subscription: str
            """
            The subscription that generated this invoice item
            """
            subscription_item: Optional[str]
            """
            The subscription item that generated this invoice item
            """

        subscription_details: Optional[SubscriptionDetails]
        """
        Details about the subscription that generated this invoice item
        """
        type: Literal["subscription_details"]
        """
        The type of parent that generated this invoice item
        """
        _inner_class_types = {"subscription_details": SubscriptionDetails}

    class Period(StripeObject):
        end: int
        """
        The end of the period, which must be greater than or equal to the start. This value is inclusive.
        """
        start: int
        """
        The start of the period. This value is inclusive.
        """

    class Pricing(StripeObject):
        class PriceDetails(StripeObject):
            price: ExpandableField["Price"]
            """
            The ID of the price this item is associated with.
            """
            product: str
            """
            The ID of the product this item is associated with.
            """

        price_details: Optional[PriceDetails]
        type: Literal["price_details"]
        """
        The type of the pricing details.
        """
        unit_amount_decimal: Optional[str]
        """
        The unit amount (in the `currency` specified) of the item which contains a decimal value with at most 12 decimal places.
        """
        _inner_class_types = {"price_details": PriceDetails}

    class ProrationDetails(StripeObject):
        class DiscountAmount(StripeObject):
            amount: int
            """
            The amount, in cents (or local equivalent), of the discount.
            """
            discount: ExpandableField["Discount"]
            """
            The discount that was applied to get this discount amount.
            """

        discount_amounts: List[DiscountAmount]
        """
        Discount amounts applied when the proration was created.
        """
        _inner_class_types = {"discount_amounts": DiscountAmount}

    amount: int
    """
    Amount (in the `currency` specified) of the invoice item. This should always be equal to `unit_amount * quantity`.
    """
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    customer: ExpandableField["Customer"]
    """
    The ID of the customer to bill for this invoice item.
    """
    customer_account: Optional[str]
    """
    The ID of the account to bill for this invoice item.
    """
    date: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    deleted: Optional[Literal[True]]
    """
    Always true for a deleted object
    """
    description: Optional[str]
    """
    An arbitrary string attached to the object. Often useful for displaying to users.
    """
    discountable: bool
    """
    If true, discounts will apply to this invoice item. Always false for prorations.
    """
    discounts: Optional[List[ExpandableField["Discount"]]]
    """
    The discounts which apply to the invoice item. Item discounts are applied before invoice discounts. Use `expand[]=discounts` to expand each discount.
    """
    id: str
    """
    Unique identifier for the object.
    """
    invoice: Optional[ExpandableField["Invoice"]]
    """
    The ID of the invoice this invoice item belongs to.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    metadata: Optional[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    net_amount: Optional[int]
    """
    The amount after discounts, but before credits and taxes. This field is `null` for `discountable=true` items.
    """
    object: Literal["invoiceitem"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    parent: Optional[Parent]
    """
    The parent that generated this invoice item.
    """
    period: Period
    pricing: Optional[Pricing]
    """
    The pricing information of the invoice item.
    """
    proration: bool
    """
    Whether the invoice item was created automatically as a proration adjustment when the customer switched plans.
    """
    proration_details: Optional[ProrationDetails]
    quantity: int
    """
    Quantity of units for the invoice item. If the invoice item is a proration, the quantity of the subscription that the proration was computed for.
    """
    tax_rates: Optional[List["TaxRate"]]
    """
    The tax rates which apply to the invoice item. When set, the `default_tax_rates` on the invoice do not apply to this invoice item.
    """
    test_clock: Optional[ExpandableField["TestClock"]]
    """
    ID of the test clock this invoice item belongs to.
    """

    @classmethod
    def create(
        cls, **params: Unpack["InvoiceItemCreateParams"]
    ) -> "InvoiceItem":
        """
        Creates an item to be added to a draft invoice (up to 250 items per invoice). If no invoice is specified, the item will be on the next invoice created for the customer specified.
        """
        return cast(
            "InvoiceItem",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["InvoiceItemCreateParams"]
    ) -> "InvoiceItem":
        """
        Creates an item to be added to a draft invoice (up to 250 items per invoice). If no invoice is specified, the item will be on the next invoice created for the customer specified.
        """
        return cast(
            "InvoiceItem",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def _cls_delete(
        cls, sid: str, **params: Unpack["InvoiceItemDeleteParams"]
    ) -> "InvoiceItem":
        """
        Deletes an invoice item, removing it from an invoice. Deleting invoice items is only possible when they're not attached to invoices, or if it's attached to a draft invoice.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(sid))
        return cast(
            "InvoiceItem",
            cls._static_request(
                "delete",
                url,
                params=params,
            ),
        )

    @overload
    @staticmethod
    def delete(
        sid: str, **params: Unpack["InvoiceItemDeleteParams"]
    ) -> "InvoiceItem":
        """
        Deletes an invoice item, removing it from an invoice. Deleting invoice items is only possible when they're not attached to invoices, or if it's attached to a draft invoice.
        """
        ...

    @overload
    def delete(
        self, **params: Unpack["InvoiceItemDeleteParams"]
    ) -> "InvoiceItem":
        """
        Deletes an invoice item, removing it from an invoice. Deleting invoice items is only possible when they're not attached to invoices, or if it's attached to a draft invoice.
        """
        ...

    @class_method_variant("_cls_delete")
    def delete(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["InvoiceItemDeleteParams"]
    ) -> "InvoiceItem":
        """
        Deletes an invoice item, removing it from an invoice. Deleting invoice items is only possible when they're not attached to invoices, or if it's attached to a draft invoice.
        """
        return self._request_and_refresh(
            "delete",
            self.instance_url(),
            params=params,
        )

    @classmethod
    async def _cls_delete_async(
        cls, sid: str, **params: Unpack["InvoiceItemDeleteParams"]
    ) -> "InvoiceItem":
        """
        Deletes an invoice item, removing it from an invoice. Deleting invoice items is only possible when they're not attached to invoices, or if it's attached to a draft invoice.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(sid))
        return cast(
            "InvoiceItem",
            await cls._static_request_async(
                "delete",
                url,
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def delete_async(
        sid: str, **params: Unpack["InvoiceItemDeleteParams"]
    ) -> "InvoiceItem":
        """
        Deletes an invoice item, removing it from an invoice. Deleting invoice items is only possible when they're not attached to invoices, or if it's attached to a draft invoice.
        """
        ...

    @overload
    async def delete_async(
        self, **params: Unpack["InvoiceItemDeleteParams"]
    ) -> "InvoiceItem":
        """
        Deletes an invoice item, removing it from an invoice. Deleting invoice items is only possible when they're not attached to invoices, or if it's attached to a draft invoice.
        """
        ...

    @class_method_variant("_cls_delete_async")
    async def delete_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["InvoiceItemDeleteParams"]
    ) -> "InvoiceItem":
        """
        Deletes an invoice item, removing it from an invoice. Deleting invoice items is only possible when they're not attached to invoices, or if it's attached to a draft invoice.
        """
        return await self._request_and_refresh_async(
            "delete",
            self.instance_url(),
            params=params,
        )

    @classmethod
    def list(
        cls, **params: Unpack["InvoiceItemListParams"]
    ) -> ListObject["InvoiceItem"]:
        """
        Returns a list of your invoice items. Invoice items are returned sorted by creation date, with the most recently created invoice items appearing first.
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
        cls, **params: Unpack["InvoiceItemListParams"]
    ) -> ListObject["InvoiceItem"]:
        """
        Returns a list of your invoice items. Invoice items are returned sorted by creation date, with the most recently created invoice items appearing first.
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
        cls, id: str, **params: Unpack["InvoiceItemModifyParams"]
    ) -> "InvoiceItem":
        """
        Updates the amount or description of an invoice item on an upcoming invoice. Updating an invoice item is only possible before the invoice it's attached to is closed.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "InvoiceItem",
            cls._static_request(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    async def modify_async(
        cls, id: str, **params: Unpack["InvoiceItemModifyParams"]
    ) -> "InvoiceItem":
        """
        Updates the amount or description of an invoice item on an upcoming invoice. Updating an invoice item is only possible before the invoice it's attached to is closed.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "InvoiceItem",
            await cls._static_request_async(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["InvoiceItemRetrieveParams"]
    ) -> "InvoiceItem":
        """
        Retrieves the invoice item with the given ID.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["InvoiceItemRetrieveParams"]
    ) -> "InvoiceItem":
        """
        Retrieves the invoice item with the given ID.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    _inner_class_types = {
        "parent": Parent,
        "period": Period,
        "pricing": Pricing,
        "proration_details": ProrationDetails,
    }
