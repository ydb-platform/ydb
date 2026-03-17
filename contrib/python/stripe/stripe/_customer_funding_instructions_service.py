# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._funding_instructions import FundingInstructions
    from stripe._request_options import RequestOptions
    from stripe.params._customer_funding_instructions_create_params import (
        CustomerFundingInstructionsCreateParams,
    )


class CustomerFundingInstructionsService(StripeService):
    def create(
        self,
        customer: str,
        params: "CustomerFundingInstructionsCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "FundingInstructions":
        """
        Retrieve funding instructions for a customer cash balance. If funding instructions do not yet exist for the customer, new
        funding instructions will be created. If funding instructions have already been created for a given customer, the same
        funding instructions will be retrieved. In other words, we will return the same funding instructions each time.
        """
        return cast(
            "FundingInstructions",
            self._request(
                "post",
                "/v1/customers/{customer}/funding_instructions".format(
                    customer=sanitize_id(customer),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        customer: str,
        params: "CustomerFundingInstructionsCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "FundingInstructions":
        """
        Retrieve funding instructions for a customer cash balance. If funding instructions do not yet exist for the customer, new
        funding instructions will be created. If funding instructions have already been created for a given customer, the same
        funding instructions will be retrieved. In other words, we will return the same funding instructions each time.
        """
        return cast(
            "FundingInstructions",
            await self._request_async(
                "post",
                "/v1/customers/{customer}/funding_instructions".format(
                    customer=sanitize_id(customer),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
