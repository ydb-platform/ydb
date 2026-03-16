# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._request_options import RequestOptions
    from stripe._token import Token
    from stripe.params._token_create_params import TokenCreateParams
    from stripe.params._token_retrieve_params import TokenRetrieveParams


class TokenService(StripeService):
    def retrieve(
        self,
        token: str,
        params: Optional["TokenRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Token":
        """
        Retrieves the token with the given ID.
        """
        return cast(
            "Token",
            self._request(
                "get",
                "/v1/tokens/{token}".format(token=sanitize_id(token)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        token: str,
        params: Optional["TokenRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Token":
        """
        Retrieves the token with the given ID.
        """
        return cast(
            "Token",
            await self._request_async(
                "get",
                "/v1/tokens/{token}".format(token=sanitize_id(token)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: Optional["TokenCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Token":
        """
        Creates a single-use token that represents a bank account's details.
        You can use this token with any v1 API method in place of a bank account dictionary. You can only use this token once. To do so, attach it to a [connected account](https://docs.stripe.com/api#accounts) where [controller.requirement_collection](https://docs.stripe.com/api/accounts/object#account_object-controller-requirement_collection) is application, which includes Custom accounts.
        """
        return cast(
            "Token",
            self._request(
                "post",
                "/v1/tokens",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: Optional["TokenCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Token":
        """
        Creates a single-use token that represents a bank account's details.
        You can use this token with any v1 API method in place of a bank account dictionary. You can only use this token once. To do so, attach it to a [connected account](https://docs.stripe.com/api#accounts) where [controller.requirement_collection](https://docs.stripe.com/api/accounts/object#account_object-controller-requirement_collection) is application, which includes Custom accounts.
        """
        return cast(
            "Token",
            await self._request_async(
                "post",
                "/v1/tokens",
                base_address="api",
                params=params,
                options=options,
            ),
        )
