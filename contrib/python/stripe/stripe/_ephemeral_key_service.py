# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._ephemeral_key import EphemeralKey
    from stripe._request_options import RequestOptions
    from stripe.params._ephemeral_key_create_params import (
        EphemeralKeyCreateParams,
    )
    from stripe.params._ephemeral_key_delete_params import (
        EphemeralKeyDeleteParams,
    )


class EphemeralKeyService(StripeService):
    def delete(
        self,
        key: str,
        params: Optional["EphemeralKeyDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "EphemeralKey":
        """
        Invalidates a short-lived API key for a given resource.
        """
        return cast(
            "EphemeralKey",
            self._request(
                "delete",
                "/v1/ephemeral_keys/{key}".format(key=sanitize_id(key)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def delete_async(
        self,
        key: str,
        params: Optional["EphemeralKeyDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "EphemeralKey":
        """
        Invalidates a short-lived API key for a given resource.
        """
        return cast(
            "EphemeralKey",
            await self._request_async(
                "delete",
                "/v1/ephemeral_keys/{key}".format(key=sanitize_id(key)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: Optional["EphemeralKeyCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "EphemeralKey":
        """
        Creates a short-lived API key for a given resource.
        """
        return cast(
            "EphemeralKey",
            self._request(
                "post",
                "/v1/ephemeral_keys",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: Optional["EphemeralKeyCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "EphemeralKey":
        """
        Creates a short-lived API key for a given resource.
        """
        return cast(
            "EphemeralKey",
            await self._request_async(
                "post",
                "/v1/ephemeral_keys",
                base_address="api",
                params=params,
                options=options,
            ),
        )
