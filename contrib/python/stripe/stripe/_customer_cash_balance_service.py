# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._cash_balance import CashBalance
    from stripe._request_options import RequestOptions
    from stripe.params._customer_cash_balance_retrieve_params import (
        CustomerCashBalanceRetrieveParams,
    )
    from stripe.params._customer_cash_balance_update_params import (
        CustomerCashBalanceUpdateParams,
    )


class CustomerCashBalanceService(StripeService):
    def retrieve(
        self,
        customer: str,
        params: Optional["CustomerCashBalanceRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "CashBalance":
        """
        Retrieves a customer's cash balance.
        """
        return cast(
            "CashBalance",
            self._request(
                "get",
                "/v1/customers/{customer}/cash_balance".format(
                    customer=sanitize_id(customer),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        customer: str,
        params: Optional["CustomerCashBalanceRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "CashBalance":
        """
        Retrieves a customer's cash balance.
        """
        return cast(
            "CashBalance",
            await self._request_async(
                "get",
                "/v1/customers/{customer}/cash_balance".format(
                    customer=sanitize_id(customer),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        customer: str,
        params: Optional["CustomerCashBalanceUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "CashBalance":
        """
        Changes the settings on a customer's cash balance.
        """
        return cast(
            "CashBalance",
            self._request(
                "post",
                "/v1/customers/{customer}/cash_balance".format(
                    customer=sanitize_id(customer),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        customer: str,
        params: Optional["CustomerCashBalanceUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "CashBalance":
        """
        Changes the settings on a customer's cash balance.
        """
        return cast(
            "CashBalance",
            await self._request_async(
                "post",
                "/v1/customers/{customer}/cash_balance".format(
                    customer=sanitize_id(customer),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
