# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._request_options import RequestOptions
    from stripe.params.terminal._connection_token_create_params import (
        ConnectionTokenCreateParams,
    )
    from stripe.terminal._connection_token import ConnectionToken


class ConnectionTokenService(StripeService):
    def create(
        self,
        params: Optional["ConnectionTokenCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ConnectionToken":
        """
        To connect to a reader the Stripe Terminal SDK needs to retrieve a short-lived connection token from Stripe, proxied through your server. On your backend, add an endpoint that creates and returns a connection token.
        """
        return cast(
            "ConnectionToken",
            self._request(
                "post",
                "/v1/terminal/connection_tokens",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: Optional["ConnectionTokenCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ConnectionToken":
        """
        To connect to a reader the Stripe Terminal SDK needs to retrieve a short-lived connection token from Stripe, proxied through your server. On your backend, add an endpoint that creates and returns a connection token.
        """
        return cast(
            "ConnectionToken",
            await self._request_async(
                "post",
                "/v1/terminal/connection_tokens",
                base_address="api",
                params=params,
                options=options,
            ),
        )
