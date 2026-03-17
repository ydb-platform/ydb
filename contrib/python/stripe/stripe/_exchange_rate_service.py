# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._exchange_rate import ExchangeRate
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.params._exchange_rate_list_params import ExchangeRateListParams
    from stripe.params._exchange_rate_retrieve_params import (
        ExchangeRateRetrieveParams,
    )


class ExchangeRateService(StripeService):
    def list(
        self,
        params: Optional["ExchangeRateListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[ExchangeRate]":
        """
        [Deprecated] The ExchangeRate APIs are deprecated. Please use the [FX Quotes API](https://docs.stripe.com/payments/currencies/localize-prices/fx-quotes-api) instead.

        Returns a list of objects that contain the rates at which foreign currencies are converted to one another. Only shows the currencies for which Stripe supports.
        """
        return cast(
            "ListObject[ExchangeRate]",
            self._request(
                "get",
                "/v1/exchange_rates",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["ExchangeRateListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[ExchangeRate]":
        """
        [Deprecated] The ExchangeRate APIs are deprecated. Please use the [FX Quotes API](https://docs.stripe.com/payments/currencies/localize-prices/fx-quotes-api) instead.

        Returns a list of objects that contain the rates at which foreign currencies are converted to one another. Only shows the currencies for which Stripe supports.
        """
        return cast(
            "ListObject[ExchangeRate]",
            await self._request_async(
                "get",
                "/v1/exchange_rates",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        rate_id: str,
        params: Optional["ExchangeRateRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ExchangeRate":
        """
        [Deprecated] The ExchangeRate APIs are deprecated. Please use the [FX Quotes API](https://docs.stripe.com/payments/currencies/localize-prices/fx-quotes-api) instead.

        Retrieves the exchange rates from the given currency to every supported currency.
        """
        return cast(
            "ExchangeRate",
            self._request(
                "get",
                "/v1/exchange_rates/{rate_id}".format(
                    rate_id=sanitize_id(rate_id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        rate_id: str,
        params: Optional["ExchangeRateRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ExchangeRate":
        """
        [Deprecated] The ExchangeRate APIs are deprecated. Please use the [FX Quotes API](https://docs.stripe.com/payments/currencies/localize-prices/fx-quotes-api) instead.

        Retrieves the exchange rates from the given currency to every supported currency.
        """
        return cast(
            "ExchangeRate",
            await self._request_async(
                "get",
                "/v1/exchange_rates/{rate_id}".format(
                    rate_id=sanitize_id(rate_id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
