# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._request_options import RequestOptions
    from stripe.billing._credit_balance_summary import CreditBalanceSummary
    from stripe.params.billing._credit_balance_summary_retrieve_params import (
        CreditBalanceSummaryRetrieveParams,
    )


class CreditBalanceSummaryService(StripeService):
    def retrieve(
        self,
        params: "CreditBalanceSummaryRetrieveParams",
        options: Optional["RequestOptions"] = None,
    ) -> "CreditBalanceSummary":
        """
        Retrieves the credit balance summary for a customer.
        """
        return cast(
            "CreditBalanceSummary",
            self._request(
                "get",
                "/v1/billing/credit_balance_summary",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        params: "CreditBalanceSummaryRetrieveParams",
        options: Optional["RequestOptions"] = None,
    ) -> "CreditBalanceSummary":
        """
        Retrieves the credit balance summary for a customer.
        """
        return cast(
            "CreditBalanceSummary",
            await self._request_async(
                "get",
                "/v1/billing/credit_balance_summary",
                base_address="api",
                params=params,
                options=options,
            ),
        )
