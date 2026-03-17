# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.issuing._token import Token
    from stripe.params.issuing._token_list_params import TokenListParams
    from stripe.params.issuing._token_retrieve_params import (
        TokenRetrieveParams,
    )
    from stripe.params.issuing._token_update_params import TokenUpdateParams


class TokenService(StripeService):
    def list(
        self,
        params: "TokenListParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Token]":
        """
        Lists all Issuing Token objects for a given card.
        """
        return cast(
            "ListObject[Token]",
            self._request(
                "get",
                "/v1/issuing/tokens",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: "TokenListParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Token]":
        """
        Lists all Issuing Token objects for a given card.
        """
        return cast(
            "ListObject[Token]",
            await self._request_async(
                "get",
                "/v1/issuing/tokens",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        token: str,
        params: Optional["TokenRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Token":
        """
        Retrieves an Issuing Token object.
        """
        return cast(
            "Token",
            self._request(
                "get",
                "/v1/issuing/tokens/{token}".format(token=sanitize_id(token)),
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
        Retrieves an Issuing Token object.
        """
        return cast(
            "Token",
            await self._request_async(
                "get",
                "/v1/issuing/tokens/{token}".format(token=sanitize_id(token)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        token: str,
        params: "TokenUpdateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Token":
        """
        Attempts to update the specified Issuing Token object to the status specified.
        """
        return cast(
            "Token",
            self._request(
                "post",
                "/v1/issuing/tokens/{token}".format(token=sanitize_id(token)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        token: str,
        params: "TokenUpdateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Token":
        """
        Attempts to update the specified Issuing Token object to the status specified.
        """
        return cast(
            "Token",
            await self._request_async(
                "post",
                "/v1/issuing/tokens/{token}".format(token=sanitize_id(token)),
                base_address="api",
                params=params,
                options=options,
            ),
        )
