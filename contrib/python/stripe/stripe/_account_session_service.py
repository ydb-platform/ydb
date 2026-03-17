# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._account_session import AccountSession
    from stripe._request_options import RequestOptions
    from stripe.params._account_session_create_params import (
        AccountSessionCreateParams,
    )


class AccountSessionService(StripeService):
    def create(
        self,
        params: "AccountSessionCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "AccountSession":
        """
        Creates a AccountSession object that includes a single-use token that the platform can use on their front-end to grant client-side API access.
        """
        return cast(
            "AccountSession",
            self._request(
                "post",
                "/v1/account_sessions",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "AccountSessionCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "AccountSession":
        """
        Creates a AccountSession object that includes a single-use token that the platform can use on their front-end to grant client-side API access.
        """
        return cast(
            "AccountSession",
            await self._request_async(
                "post",
                "/v1/account_sessions",
                base_address="api",
                params=params,
                options=options,
            ),
        )
