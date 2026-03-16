# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from typing import Optional
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._request_options import RequestOptions
    from stripe.params.v2.billing._meter_event_stream_create_params import (
        MeterEventStreamCreateParams,
    )


class MeterEventStreamService(StripeService):
    def create(
        self,
        params: "MeterEventStreamCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "None":
        """
        Creates meter events. Events are processed asynchronously, including validation. Requires a meter event session for authentication. Supports up to 10,000 requests per second in livemode. For even higher rate-limits, contact sales.
        """
        self._request(
            "post",
            "/v2/billing/meter_event_stream",
            base_address="meter_events",
            params=params,
            options=options,
        )

    async def create_async(
        self,
        params: "MeterEventStreamCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "None":
        """
        Creates meter events. Events are processed asynchronously, including validation. Requires a meter event session for authentication. Supports up to 10,000 requests per second in livemode. For even higher rate-limits, contact sales.
        """
        await self._request_async(
            "post",
            "/v2/billing/meter_event_stream",
            base_address="meter_events",
            params=params,
            options=options,
        )
