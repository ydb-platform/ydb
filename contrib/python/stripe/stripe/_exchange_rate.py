# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._util import deprecated
from typing import ClassVar, Dict
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params._exchange_rate_list_params import ExchangeRateListParams
    from stripe.params._exchange_rate_retrieve_params import (
        ExchangeRateRetrieveParams,
    )


class ExchangeRate(ListableAPIResource["ExchangeRate"]):
    """
    [Deprecated] The `ExchangeRate` APIs are deprecated. Please use the [FX Quotes API](https://docs.stripe.com/payments/currencies/localize-prices/fx-quotes-api) instead.

    `ExchangeRate` objects allow you to determine the rates that Stripe is currently
    using to convert from one currency to another. Since this number is variable
    throughout the day, there are various reasons why you might want to know the current
    rate (for example, to dynamically price an item for a user with a default
    payment in a foreign currency).

    Please refer to our [Exchange Rates API](https://docs.stripe.com/fx-rates) guide for more details.

    *[Note: this integration path is supported but no longer recommended]* Additionally,
    you can guarantee that a charge is made with an exchange rate that you expect is
    current. To do so, you must pass in the exchange_rate to charges endpoints. If the
    value is no longer up to date, the charge won't go through. Please refer to our
    [Using with charges](https://docs.stripe.com/exchange-rates) guide for more details.

    -----

    &nbsp;

    *This Exchange Rates API is a Beta Service and is subject to Stripe's terms of service. You may use the API solely for the purpose of transacting on Stripe. For example, the API may be queried in order to:*

    - *localize prices for processing payments on Stripe*
    - *reconcile Stripe transactions*
    - *determine how much money to send to a connected account*
    - *determine app fees to charge a connected account*

    *Using this Exchange Rates API beta for any purpose other than to transact on Stripe is strictly prohibited and constitutes a violation of Stripe's terms of service.*
    """

    OBJECT_NAME: ClassVar[Literal["exchange_rate"]] = "exchange_rate"
    id: str
    """
    Unique identifier for the object. Represented as the three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html) in lowercase.
    """
    object: Literal["exchange_rate"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    rates: Dict[str, float]
    """
    Hash where the keys are supported currencies and the values are the exchange rate at which the base id currency converts to the key currency.
    """

    @classmethod
    @deprecated(
        "This method is deprecated, please refer to the description for details.",
    )
    def list(
        cls, **params: Unpack["ExchangeRateListParams"]
    ) -> ListObject["ExchangeRate"]:
        """
        [Deprecated] The ExchangeRate APIs are deprecated. Please use the [FX Quotes API](https://docs.stripe.com/payments/currencies/localize-prices/fx-quotes-api) instead.

        Returns a list of objects that contain the rates at which foreign currencies are converted to one another. Only shows the currencies for which Stripe supports.
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
    @deprecated(
        "This method is deprecated, please refer to the description for details.",
    )
    async def list_async(
        cls, **params: Unpack["ExchangeRateListParams"]
    ) -> ListObject["ExchangeRate"]:
        """
        [Deprecated] The ExchangeRate APIs are deprecated. Please use the [FX Quotes API](https://docs.stripe.com/payments/currencies/localize-prices/fx-quotes-api) instead.

        Returns a list of objects that contain the rates at which foreign currencies are converted to one another. Only shows the currencies for which Stripe supports.
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
    @deprecated(
        "This method is deprecated, please refer to the description for details.",
    )
    def retrieve(
        cls, id: str, **params: Unpack["ExchangeRateRetrieveParams"]
    ) -> "ExchangeRate":
        """
        [Deprecated] The ExchangeRate APIs are deprecated. Please use the [FX Quotes API](https://docs.stripe.com/payments/currencies/localize-prices/fx-quotes-api) instead.

        Retrieves the exchange rates from the given currency to every supported currency.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    @deprecated(
        "This method is deprecated, please refer to the description for details.",
    )
    async def retrieve_async(
        cls, id: str, **params: Unpack["ExchangeRateRetrieveParams"]
    ) -> "ExchangeRate":
        """
        [Deprecated] The ExchangeRate APIs are deprecated. Please use the [FX Quotes API](https://docs.stripe.com/payments/currencies/localize-prices/fx-quotes-api) instead.

        Retrieves the exchange rates from the given currency to every supported currency.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance
