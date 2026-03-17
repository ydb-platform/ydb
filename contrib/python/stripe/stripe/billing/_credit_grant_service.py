# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.billing._credit_grant import CreditGrant
    from stripe.params.billing._credit_grant_create_params import (
        CreditGrantCreateParams,
    )
    from stripe.params.billing._credit_grant_expire_params import (
        CreditGrantExpireParams,
    )
    from stripe.params.billing._credit_grant_list_params import (
        CreditGrantListParams,
    )
    from stripe.params.billing._credit_grant_retrieve_params import (
        CreditGrantRetrieveParams,
    )
    from stripe.params.billing._credit_grant_update_params import (
        CreditGrantUpdateParams,
    )
    from stripe.params.billing._credit_grant_void_grant_params import (
        CreditGrantVoidGrantParams,
    )


class CreditGrantService(StripeService):
    def list(
        self,
        params: Optional["CreditGrantListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[CreditGrant]":
        """
        Retrieve a list of credit grants.
        """
        return cast(
            "ListObject[CreditGrant]",
            self._request(
                "get",
                "/v1/billing/credit_grants",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["CreditGrantListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[CreditGrant]":
        """
        Retrieve a list of credit grants.
        """
        return cast(
            "ListObject[CreditGrant]",
            await self._request_async(
                "get",
                "/v1/billing/credit_grants",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: "CreditGrantCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "CreditGrant":
        """
        Creates a credit grant.
        """
        return cast(
            "CreditGrant",
            self._request(
                "post",
                "/v1/billing/credit_grants",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "CreditGrantCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "CreditGrant":
        """
        Creates a credit grant.
        """
        return cast(
            "CreditGrant",
            await self._request_async(
                "post",
                "/v1/billing/credit_grants",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        id: str,
        params: Optional["CreditGrantRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "CreditGrant":
        """
        Retrieves a credit grant.
        """
        return cast(
            "CreditGrant",
            self._request(
                "get",
                "/v1/billing/credit_grants/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        id: str,
        params: Optional["CreditGrantRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "CreditGrant":
        """
        Retrieves a credit grant.
        """
        return cast(
            "CreditGrant",
            await self._request_async(
                "get",
                "/v1/billing/credit_grants/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        id: str,
        params: Optional["CreditGrantUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "CreditGrant":
        """
        Updates a credit grant.
        """
        return cast(
            "CreditGrant",
            self._request(
                "post",
                "/v1/billing/credit_grants/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        id: str,
        params: Optional["CreditGrantUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "CreditGrant":
        """
        Updates a credit grant.
        """
        return cast(
            "CreditGrant",
            await self._request_async(
                "post",
                "/v1/billing/credit_grants/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def expire(
        self,
        id: str,
        params: Optional["CreditGrantExpireParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "CreditGrant":
        """
        Expires a credit grant.
        """
        return cast(
            "CreditGrant",
            self._request(
                "post",
                "/v1/billing/credit_grants/{id}/expire".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def expire_async(
        self,
        id: str,
        params: Optional["CreditGrantExpireParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "CreditGrant":
        """
        Expires a credit grant.
        """
        return cast(
            "CreditGrant",
            await self._request_async(
                "post",
                "/v1/billing/credit_grants/{id}/expire".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def void_grant(
        self,
        id: str,
        params: Optional["CreditGrantVoidGrantParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "CreditGrant":
        """
        Voids a credit grant.
        """
        return cast(
            "CreditGrant",
            self._request(
                "post",
                "/v1/billing/credit_grants/{id}/void".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def void_grant_async(
        self,
        id: str,
        params: Optional["CreditGrantVoidGrantParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "CreditGrant":
        """
        Voids a credit grant.
        """
        return cast(
            "CreditGrant",
            await self._request_async(
                "post",
                "/v1/billing/credit_grants/{id}/void".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
