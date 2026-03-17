# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._login_link import LoginLink
    from stripe._request_options import RequestOptions
    from stripe.params._account_login_link_create_params import (
        AccountLoginLinkCreateParams,
    )


class AccountLoginLinkService(StripeService):
    def create(
        self,
        account: str,
        params: Optional["AccountLoginLinkCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "LoginLink":
        """
        Creates a login link for a connected account to access the Express Dashboard.

        You can only create login links for accounts that use the [Express Dashboard](https://docs.stripe.com/connect/express-dashboard) and are connected to your platform.
        """
        return cast(
            "LoginLink",
            self._request(
                "post",
                "/v1/accounts/{account}/login_links".format(
                    account=sanitize_id(account),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        account: str,
        params: Optional["AccountLoginLinkCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "LoginLink":
        """
        Creates a login link for a connected account to access the Express Dashboard.

        You can only create login links for accounts that use the [Express Dashboard](https://docs.stripe.com/connect/express-dashboard) and are connected to your platform.
        """
        return cast(
            "LoginLink",
            await self._request_async(
                "post",
                "/v1/accounts/{account}/login_links".format(
                    account=sanitize_id(account),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
