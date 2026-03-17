# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._confirmation_token import ConfirmationToken
    from stripe._request_options import RequestOptions
    from stripe.params._confirmation_token_retrieve_params import (
        ConfirmationTokenRetrieveParams,
    )


class ConfirmationTokenService(StripeService):
    def retrieve(
        self,
        confirmation_token: str,
        params: Optional["ConfirmationTokenRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ConfirmationToken":
        """
        Retrieves an existing ConfirmationToken object
        """
        return cast(
            "ConfirmationToken",
            self._request(
                "get",
                "/v1/confirmation_tokens/{confirmation_token}".format(
                    confirmation_token=sanitize_id(confirmation_token),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        confirmation_token: str,
        params: Optional["ConfirmationTokenRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ConfirmationToken":
        """
        Retrieves an existing ConfirmationToken object
        """
        return cast(
            "ConfirmationToken",
            await self._request_async(
                "get",
                "/v1/confirmation_tokens/{confirmation_token}".format(
                    confirmation_token=sanitize_id(confirmation_token),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
