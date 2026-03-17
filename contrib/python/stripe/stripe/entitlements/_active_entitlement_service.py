# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.entitlements._active_entitlement import ActiveEntitlement
    from stripe.params.entitlements._active_entitlement_list_params import (
        ActiveEntitlementListParams,
    )
    from stripe.params.entitlements._active_entitlement_retrieve_params import (
        ActiveEntitlementRetrieveParams,
    )


class ActiveEntitlementService(StripeService):
    def list(
        self,
        params: "ActiveEntitlementListParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[ActiveEntitlement]":
        """
        Retrieve a list of active entitlements for a customer
        """
        return cast(
            "ListObject[ActiveEntitlement]",
            self._request(
                "get",
                "/v1/entitlements/active_entitlements",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: "ActiveEntitlementListParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[ActiveEntitlement]":
        """
        Retrieve a list of active entitlements for a customer
        """
        return cast(
            "ListObject[ActiveEntitlement]",
            await self._request_async(
                "get",
                "/v1/entitlements/active_entitlements",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        id: str,
        params: Optional["ActiveEntitlementRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ActiveEntitlement":
        """
        Retrieve an active entitlement
        """
        return cast(
            "ActiveEntitlement",
            self._request(
                "get",
                "/v1/entitlements/active_entitlements/{id}".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        id: str,
        params: Optional["ActiveEntitlementRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ActiveEntitlement":
        """
        Retrieve an active entitlement
        """
        return cast(
            "ActiveEntitlement",
            await self._request_async(
                "get",
                "/v1/entitlements/active_entitlements/{id}".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
