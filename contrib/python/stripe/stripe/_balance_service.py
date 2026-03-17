# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._balance import Balance
    from stripe._request_options import RequestOptions
    from stripe.params._balance_retrieve_params import BalanceRetrieveParams


class BalanceService(StripeService):
    def retrieve(
        self,
        params: Optional["BalanceRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Balance":
        """
        Retrieves the current account balance, based on the authentication that was used to make the request.
         For a sample request, see [Accounting for negative balances](https://docs.stripe.com/docs/connect/account-balances#accounting-for-negative-balances).
        """
        return cast(
            "Balance",
            self._request(
                "get",
                "/v1/balance",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        params: Optional["BalanceRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Balance":
        """
        Retrieves the current account balance, based on the authentication that was used to make the request.
         For a sample request, see [Accounting for negative balances](https://docs.stripe.com/docs/connect/account-balances#accounting-for-negative-balances).
        """
        return cast(
            "Balance",
            await self._request_async(
                "get",
                "/v1/balance",
                base_address="api",
                params=params,
                options=options,
            ),
        )
