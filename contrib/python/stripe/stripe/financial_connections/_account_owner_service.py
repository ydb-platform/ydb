# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.financial_connections._account_owner import AccountOwner
    from stripe.params.financial_connections._account_owner_list_params import (
        AccountOwnerListParams,
    )


class AccountOwnerService(StripeService):
    def list(
        self,
        account: str,
        params: "AccountOwnerListParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[AccountOwner]":
        """
        Lists all owners for a given Account
        """
        return cast(
            "ListObject[AccountOwner]",
            self._request(
                "get",
                "/v1/financial_connections/accounts/{account}/owners".format(
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
        params: "AccountOwnerListParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[AccountOwner]":
        """
        Lists all owners for a given Account
        """
        return cast(
            "ListObject[AccountOwner]",
            await self._request_async(
                "get",
                "/v1/financial_connections/accounts/{account}/owners".format(
                    account=sanitize_id(account),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
