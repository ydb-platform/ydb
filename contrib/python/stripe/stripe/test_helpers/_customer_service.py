# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._customer_cash_balance_transaction import (
        CustomerCashBalanceTransaction,
    )
    from stripe._request_options import RequestOptions
    from stripe.params.test_helpers._customer_fund_cash_balance_params import (
        CustomerFundCashBalanceParams,
    )


class CustomerService(StripeService):
    def fund_cash_balance(
        self,
        customer: str,
        params: "CustomerFundCashBalanceParams",
        options: Optional["RequestOptions"] = None,
    ) -> "CustomerCashBalanceTransaction":
        """
        Create an incoming testmode bank transfer
        """
        return cast(
            "CustomerCashBalanceTransaction",
            self._request(
                "post",
                "/v1/test_helpers/customers/{customer}/fund_cash_balance".format(
                    customer=sanitize_id(customer),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def fund_cash_balance_async(
        self,
        customer: str,
        params: "CustomerFundCashBalanceParams",
        options: Optional["RequestOptions"] = None,
    ) -> "CustomerCashBalanceTransaction":
        """
        Create an incoming testmode bank transfer
        """
        return cast(
            "CustomerCashBalanceTransaction",
            await self._request_async(
                "post",
                "/v1/test_helpers/customers/{customer}/fund_cash_balance".format(
                    customer=sanitize_id(customer),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
