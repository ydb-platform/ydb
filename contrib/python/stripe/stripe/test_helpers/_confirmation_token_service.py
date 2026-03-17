# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._confirmation_token import ConfirmationToken
    from stripe._request_options import RequestOptions
    from stripe.params.test_helpers._confirmation_token_create_params import (
        ConfirmationTokenCreateParams,
    )


class ConfirmationTokenService(StripeService):
    def create(
        self,
        params: Optional["ConfirmationTokenCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ConfirmationToken":
        """
        Creates a test mode Confirmation Token server side for your integration tests.
        """
        return cast(
            "ConfirmationToken",
            self._request(
                "post",
                "/v1/test_helpers/confirmation_tokens",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: Optional["ConfirmationTokenCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ConfirmationToken":
        """
        Creates a test mode Confirmation Token server side for your integration tests.
        """
        return cast(
            "ConfirmationToken",
            await self._request_async(
                "post",
                "/v1/test_helpers/confirmation_tokens",
                base_address="api",
                params=params,
                options=options,
            ),
        )
