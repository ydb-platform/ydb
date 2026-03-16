# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._request_options import RequestOptions
    from stripe.params.v2.billing._meter_event_create_params import (
        MeterEventCreateParams,
    )
    from stripe.v2.billing._meter_event import MeterEvent


class MeterEventService(StripeService):
    def create(
        self,
        params: "MeterEventCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "MeterEvent":
        """
        Creates a meter event. Events are validated synchronously, but are processed asynchronously. Supports up to 1,000 events per second in livemode. For higher rate-limits, please use meter event streams instead.
        """
        return cast(
            "MeterEvent",
            self._request(
                "post",
                "/v2/billing/meter_events",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "MeterEventCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "MeterEvent":
        """
        Creates a meter event. Events are validated synchronously, but are processed asynchronously. Supports up to 1,000 events per second in livemode. For higher rate-limits, please use meter event streams instead.
        """
        return cast(
            "MeterEvent",
            await self._request_async(
                "post",
                "/v2/billing/meter_events",
                base_address="api",
                params=params,
                options=options,
            ),
        )
