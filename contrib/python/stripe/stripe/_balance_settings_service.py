# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._balance_settings import BalanceSettings
    from stripe._request_options import RequestOptions
    from stripe.params._balance_settings_retrieve_params import (
        BalanceSettingsRetrieveParams,
    )
    from stripe.params._balance_settings_update_params import (
        BalanceSettingsUpdateParams,
    )


class BalanceSettingsService(StripeService):
    def retrieve(
        self,
        params: Optional["BalanceSettingsRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "BalanceSettings":
        """
        Retrieves balance settings for a given connected account.
         Related guide: [Making API calls for connected accounts](https://docs.stripe.com/connect/authentication)
        """
        return cast(
            "BalanceSettings",
            self._request(
                "get",
                "/v1/balance_settings",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        params: Optional["BalanceSettingsRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "BalanceSettings":
        """
        Retrieves balance settings for a given connected account.
         Related guide: [Making API calls for connected accounts](https://docs.stripe.com/connect/authentication)
        """
        return cast(
            "BalanceSettings",
            await self._request_async(
                "get",
                "/v1/balance_settings",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        params: Optional["BalanceSettingsUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "BalanceSettings":
        """
        Updates balance settings for a given connected account.
         Related guide: [Making API calls for connected accounts](https://docs.stripe.com/connect/authentication)
        """
        return cast(
            "BalanceSettings",
            self._request(
                "post",
                "/v1/balance_settings",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        params: Optional["BalanceSettingsUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "BalanceSettings":
        """
        Updates balance settings for a given connected account.
         Related guide: [Making API calls for connected accounts](https://docs.stripe.com/connect/authentication)
        """
        return cast(
            "BalanceSettings",
            await self._request_async(
                "post",
                "/v1/balance_settings",
                base_address="api",
                params=params,
                options=options,
            ),
        )
