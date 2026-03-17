# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._request_options import RequestOptions
    from stripe.params.tax._settings_retrieve_params import (
        SettingsRetrieveParams,
    )
    from stripe.params.tax._settings_update_params import SettingsUpdateParams
    from stripe.tax._settings import Settings


class SettingsService(StripeService):
    def retrieve(
        self,
        params: Optional["SettingsRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Settings":
        """
        Retrieves Tax Settings for a merchant.
        """
        return cast(
            "Settings",
            self._request(
                "get",
                "/v1/tax/settings",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        params: Optional["SettingsRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Settings":
        """
        Retrieves Tax Settings for a merchant.
        """
        return cast(
            "Settings",
            await self._request_async(
                "get",
                "/v1/tax/settings",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        params: Optional["SettingsUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Settings":
        """
        Updates Tax Settings parameters used in tax calculations. All parameters are editable but none can be removed once set.
        """
        return cast(
            "Settings",
            self._request(
                "post",
                "/v1/tax/settings",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        params: Optional["SettingsUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Settings":
        """
        Updates Tax Settings parameters used in tax calculations. All parameters are editable but none can be removed once set.
        """
        return cast(
            "Settings",
            await self._request_async(
                "post",
                "/v1/tax/settings",
                base_address="api",
                params=params,
                options=options,
            ),
        )
