# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._request_options import RequestOptions
    from stripe.billing_portal._session import Session
    from stripe.params.billing_portal._session_create_params import (
        SessionCreateParams,
    )


class SessionService(StripeService):
    def create(
        self,
        params: Optional["SessionCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Session":
        """
        Creates a session of the customer portal.
        """
        return cast(
            "Session",
            self._request(
                "post",
                "/v1/billing_portal/sessions",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: Optional["SessionCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Session":
        """
        Creates a session of the customer portal.
        """
        return cast(
            "Session",
            await self._request_async(
                "post",
                "/v1/billing_portal/sessions",
                base_address="api",
                params=params,
                options=options,
            ),
        )
