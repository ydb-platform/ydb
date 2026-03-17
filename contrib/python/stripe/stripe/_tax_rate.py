# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._stripe_object import StripeObject
from stripe._updateable_api_resource import UpdateableAPIResource
from stripe._util import sanitize_id
from typing import ClassVar, Dict, Optional, cast
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params._tax_rate_create_params import TaxRateCreateParams
    from stripe.params._tax_rate_list_params import TaxRateListParams
    from stripe.params._tax_rate_modify_params import TaxRateModifyParams
    from stripe.params._tax_rate_retrieve_params import TaxRateRetrieveParams


class TaxRate(
    CreateableAPIResource["TaxRate"],
    ListableAPIResource["TaxRate"],
    UpdateableAPIResource["TaxRate"],
):
    """
    Tax rates can be applied to [invoices](https://docs.stripe.com/invoicing/taxes/tax-rates), [subscriptions](https://docs.stripe.com/billing/taxes/tax-rates) and [Checkout Sessions](https://docs.stripe.com/payments/checkout/use-manual-tax-rates) to collect tax.

    Related guide: [Tax rates](https://docs.stripe.com/billing/taxes/tax-rates)
    """

    OBJECT_NAME: ClassVar[Literal["tax_rate"]] = "tax_rate"

    class FlatAmount(StripeObject):
        amount: int
        """
        Amount of the tax when the `rate_type` is `flat_amount`. This positive integer represents how much to charge in the smallest currency unit (e.g., 100 cents to charge $1.00 or 100 to charge ¥100, a zero-decimal currency). The amount value supports up to eight digits (e.g., a value of 99999999 for a USD charge of $999,999.99).
        """
        currency: str
        """
        Three-letter ISO currency code, in lowercase.
        """

    active: bool
    """
    Defaults to `true`. When set to `false`, this tax rate cannot be used with new applications or Checkout Sessions, but will still work for subscriptions and invoices that already have it set.
    """
    country: Optional[str]
    """
    Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    description: Optional[str]
    """
    An arbitrary string attached to the tax rate for your internal use only. It will not be visible to your customers.
    """
    display_name: str
    """
    The display name of the tax rates as it will appear to your customer on their receipt email, PDF, and the hosted invoice page.
    """
    effective_percentage: Optional[float]
    """
    Actual/effective tax rate percentage out of 100. For tax calculations with automatic_tax[enabled]=true,
    this percentage reflects the rate actually used to calculate tax based on the product's taxability
    and whether the user is registered to collect taxes in the corresponding jurisdiction.
    """
    flat_amount: Optional[FlatAmount]
    """
    The amount of the tax rate when the `rate_type` is `flat_amount`. Tax rates with `rate_type` `percentage` can vary based on the transaction, resulting in this field being `null`. This field exposes the amount and currency of the flat tax rate.
    """
    id: str
    """
    Unique identifier for the object.
    """
    inclusive: bool
    """
    This specifies if the tax rate is inclusive or exclusive.
    """
    jurisdiction: Optional[str]
    """
    The jurisdiction for the tax rate. You can use this label field for tax reporting purposes. It also appears on your customer's invoice.
    """
    jurisdiction_level: Optional[
        Literal["city", "country", "county", "district", "multiple", "state"]
    ]
    """
    The level of the jurisdiction that imposes this tax rate. Will be `null` for manually defined tax rates.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    metadata: Optional[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    object: Literal["tax_rate"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    percentage: float
    """
    Tax rate percentage out of 100. For tax calculations with automatic_tax[enabled]=true, this percentage includes the statutory tax rate of non-taxable jurisdictions.
    """
    rate_type: Optional[Literal["flat_amount", "percentage"]]
    """
    Indicates the type of tax rate applied to the taxable amount. This value can be `null` when no tax applies to the location. This field is only present for TaxRates created by Stripe Tax.
    """
    state: Optional[str]
    """
    [ISO 3166-2 subdivision code](https://en.wikipedia.org/wiki/ISO_3166-2), without country prefix. For example, "NY" for New York, United States.
    """
    tax_type: Optional[
        Literal[
            "amusement_tax",
            "communications_tax",
            "gst",
            "hst",
            "igst",
            "jct",
            "lease_tax",
            "pst",
            "qst",
            "retail_delivery_fee",
            "rst",
            "sales_tax",
            "service_tax",
            "vat",
        ]
    ]
    """
    The high-level tax type, such as `vat` or `sales_tax`.
    """

    @classmethod
    def create(cls, **params: Unpack["TaxRateCreateParams"]) -> "TaxRate":
        """
        Creates a new tax rate.
        """
        return cast(
            "TaxRate",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["TaxRateCreateParams"]
    ) -> "TaxRate":
        """
        Creates a new tax rate.
        """
        return cast(
            "TaxRate",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def list(
        cls, **params: Unpack["TaxRateListParams"]
    ) -> ListObject["TaxRate"]:
        """
        Returns a list of your tax rates. Tax rates are returned sorted by creation date, with the most recently created tax rates appearing first.
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
        cls, **params: Unpack["TaxRateListParams"]
    ) -> ListObject["TaxRate"]:
        """
        Returns a list of your tax rates. Tax rates are returned sorted by creation date, with the most recently created tax rates appearing first.
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
        cls, id: str, **params: Unpack["TaxRateModifyParams"]
    ) -> "TaxRate":
        """
        Updates an existing tax rate.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "TaxRate",
            cls._static_request(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    async def modify_async(
        cls, id: str, **params: Unpack["TaxRateModifyParams"]
    ) -> "TaxRate":
        """
        Updates an existing tax rate.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "TaxRate",
            await cls._static_request_async(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["TaxRateRetrieveParams"]
    ) -> "TaxRate":
        """
        Retrieves a tax rate with the given ID
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["TaxRateRetrieveParams"]
    ) -> "TaxRate":
        """
        Retrieves a tax rate with the given ID
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    _inner_class_types = {"flat_amount": FlatAmount}
