# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._account_link import AccountLink
    from stripe._request_options import RequestOptions
    from stripe.params._account_link_create_params import (
        AccountLinkCreateParams,
    )


class AccountLinkService(StripeService):
    def create(
        self,
        params: "AccountLinkCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "AccountLink":
        """
        Creates an AccountLink object that includes a single-use Stripe URL that the platform can redirect their user to in order to take them through the Connect Onboarding flow.
        """
        return cast(
            "AccountLink",
            self._request(
                "post",
                "/v1/account_links",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "AccountLinkCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "AccountLink":
        """
        Creates an AccountLink object that includes a single-use Stripe URL that the platform can redirect their user to in order to take them through the Connect Onboarding flow.
        """
        return cast(
            "AccountLink",
            await self._request_async(
                "post",
                "/v1/account_links",
                base_address="api",
                params=params,
                options=options,
            ),
        )
