# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._request_options import RequestOptions
    from stripe.financial_connections._session import Session
    from stripe.params.financial_connections._session_create_params import (
        SessionCreateParams,
    )
    from stripe.params.financial_connections._session_retrieve_params import (
        SessionRetrieveParams,
    )


class SessionService(StripeService):
    def retrieve(
        self,
        session: str,
        params: Optional["SessionRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Session":
        """
        Retrieves the details of a Financial Connections Session
        """
        return cast(
            "Session",
            self._request(
                "get",
                "/v1/financial_connections/sessions/{session}".format(
                    session=sanitize_id(session),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        session: str,
        params: Optional["SessionRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Session":
        """
        Retrieves the details of a Financial Connections Session
        """
        return cast(
            "Session",
            await self._request_async(
                "get",
                "/v1/financial_connections/sessions/{session}".format(
                    session=sanitize_id(session),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: "SessionCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Session":
        """
        To launch the Financial Connections authorization flow, create a Session. The session's client_secret can be used to launch the flow using Stripe.js.
        """
        return cast(
            "Session",
            self._request(
                "post",
                "/v1/financial_connections/sessions",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "SessionCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Session":
        """
        To launch the Financial Connections authorization flow, create a Session. The session's client_secret can be used to launch the flow using Stripe.js.
        """
        return cast(
            "Session",
            await self._request_async(
                "post",
                "/v1/financial_connections/sessions",
                base_address="api",
                params=params,
                options=options,
            ),
        )
