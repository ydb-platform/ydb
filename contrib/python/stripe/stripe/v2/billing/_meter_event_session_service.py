# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._request_options import RequestOptions
    from stripe.params.v2.billing._meter_event_session_create_params import (
        MeterEventSessionCreateParams,
    )
    from stripe.v2.billing._meter_event_session import MeterEventSession


class MeterEventSessionService(StripeService):
    def create(
        self,
        params: Optional["MeterEventSessionCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "MeterEventSession":
        """
        Creates a meter event session to send usage on the high-throughput meter event stream. Authentication tokens are only valid for 15 minutes, so you will need to create a new meter event session when your token expires.
        """
        return cast(
            "MeterEventSession",
            self._request(
                "post",
                "/v2/billing/meter_event_session",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: Optional["MeterEventSessionCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "MeterEventSession":
        """
        Creates a meter event session to send usage on the high-throughput meter event stream. Authentication tokens are only valid for 15 minutes, so you will need to create a new meter event session when your token expires.
        """
        return cast(
            "MeterEventSession",
            await self._request_async(
                "post",
                "/v2/billing/meter_event_session",
                base_address="api",
                params=params,
                options=options,
            ),
        )
