# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._capability import Capability
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.params._account_capability_list_params import (
        AccountCapabilityListParams,
    )
    from stripe.params._account_capability_retrieve_params import (
        AccountCapabilityRetrieveParams,
    )
    from stripe.params._account_capability_update_params import (
        AccountCapabilityUpdateParams,
    )


class AccountCapabilityService(StripeService):
    def list(
        self,
        account: str,
        params: Optional["AccountCapabilityListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Capability]":
        """
        Returns a list of capabilities associated with the account. The capabilities are returned sorted by creation date, with the most recent capability appearing first.
        """
        return cast(
            "ListObject[Capability]",
            self._request(
                "get",
                "/v1/accounts/{account}/capabilities".format(
                    account=sanitize_id(account),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        account: str,
        params: Optional["AccountCapabilityListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Capability]":
        """
        Returns a list of capabilities associated with the account. The capabilities are returned sorted by creation date, with the most recent capability appearing first.
        """
        return cast(
            "ListObject[Capability]",
            await self._request_async(
                "get",
                "/v1/accounts/{account}/capabilities".format(
                    account=sanitize_id(account),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        account: str,
        capability: str,
        params: Optional["AccountCapabilityRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Capability":
        """
        Retrieves information about the specified Account Capability.
        """
        return cast(
            "Capability",
            self._request(
                "get",
                "/v1/accounts/{account}/capabilities/{capability}".format(
                    account=sanitize_id(account),
                    capability=sanitize_id(capability),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        account: str,
        capability: str,
        params: Optional["AccountCapabilityRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Capability":
        """
        Retrieves information about the specified Account Capability.
        """
        return cast(
            "Capability",
            await self._request_async(
                "get",
                "/v1/accounts/{account}/capabilities/{capability}".format(
                    account=sanitize_id(account),
                    capability=sanitize_id(capability),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        account: str,
        capability: str,
        params: Optional["AccountCapabilityUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Capability":
        """
        Updates an existing Account Capability. Request or remove a capability by updating its requested parameter.
        """
        return cast(
            "Capability",
            self._request(
                "post",
                "/v1/accounts/{account}/capabilities/{capability}".format(
                    account=sanitize_id(account),
                    capability=sanitize_id(capability),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        account: str,
        capability: str,
        params: Optional["AccountCapabilityUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Capability":
        """
        Updates an existing Account Capability. Request or remove a capability by updating its requested parameter.
        """
        return cast(
            "Capability",
            await self._request_async(
                "post",
                "/v1/accounts/{account}/capabilities/{capability}".format(
                    account=sanitize_id(account),
                    capability=sanitize_id(capability),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
