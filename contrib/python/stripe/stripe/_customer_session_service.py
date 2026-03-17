# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._customer_session import CustomerSession
    from stripe._request_options import RequestOptions
    from stripe.params._customer_session_create_params import (
        CustomerSessionCreateParams,
    )


class CustomerSessionService(StripeService):
    def create(
        self,
        params: "CustomerSessionCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "CustomerSession":
        """
        Creates a Customer Session object that includes a single-use client secret that you can use on your front-end to grant client-side API access for certain customer resources.
        """
        return cast(
            "CustomerSession",
            self._request(
                "post",
                "/v1/customer_sessions",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "CustomerSessionCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "CustomerSession":
        """
        Creates a Customer Session object that includes a single-use client secret that you can use on your front-end to grant client-side API access for certain customer resources.
        """
        return cast(
            "CustomerSession",
            await self._request_async(
                "post",
                "/v1/customer_sessions",
                base_address="api",
                params=params,
                options=options,
            ),
        )
