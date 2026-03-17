# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.apps._secret import Secret
    from stripe.params.apps._secret_create_params import SecretCreateParams
    from stripe.params.apps._secret_delete_where_params import (
        SecretDeleteWhereParams,
    )
    from stripe.params.apps._secret_find_params import SecretFindParams
    from stripe.params.apps._secret_list_params import SecretListParams


class SecretService(StripeService):
    def list(
        self,
        params: "SecretListParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Secret]":
        """
        List all secrets stored on the given scope.
        """
        return cast(
            "ListObject[Secret]",
            self._request(
                "get",
                "/v1/apps/secrets",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: "SecretListParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Secret]":
        """
        List all secrets stored on the given scope.
        """
        return cast(
            "ListObject[Secret]",
            await self._request_async(
                "get",
                "/v1/apps/secrets",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: "SecretCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Secret":
        """
        Create or replace a secret in the secret store.
        """
        return cast(
            "Secret",
            self._request(
                "post",
                "/v1/apps/secrets",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "SecretCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Secret":
        """
        Create or replace a secret in the secret store.
        """
        return cast(
            "Secret",
            await self._request_async(
                "post",
                "/v1/apps/secrets",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def find(
        self,
        params: "SecretFindParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Secret":
        """
        Finds a secret in the secret store by name and scope.
        """
        return cast(
            "Secret",
            self._request(
                "get",
                "/v1/apps/secrets/find",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def find_async(
        self,
        params: "SecretFindParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Secret":
        """
        Finds a secret in the secret store by name and scope.
        """
        return cast(
            "Secret",
            await self._request_async(
                "get",
                "/v1/apps/secrets/find",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def delete_where(
        self,
        params: "SecretDeleteWhereParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Secret":
        """
        Deletes a secret from the secret store by name and scope.
        """
        return cast(
            "Secret",
            self._request(
                "post",
                "/v1/apps/secrets/delete",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def delete_where_async(
        self,
        params: "SecretDeleteWhereParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Secret":
        """
        Deletes a secret from the secret store by name and scope.
        """
        return cast(
            "Secret",
            await self._request_async(
                "post",
                "/v1/apps/secrets/delete",
                base_address="api",
                params=params,
                options=options,
            ),
        )
