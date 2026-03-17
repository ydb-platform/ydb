# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._request_options import RequestOptions
    from stripe.billing._meter_event import MeterEvent
    from stripe.params.billing._meter_event_create_params import (
        MeterEventCreateParams,
    )


class MeterEventService(StripeService):
    def create(
        self,
        params: "MeterEventCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "MeterEvent":
        """
        Creates a billing meter event.
        """
        return cast(
            "MeterEvent",
            self._request(
                "post",
                "/v1/billing/meter_events",
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
        Creates a billing meter event.
        """
        return cast(
            "MeterEvent",
            await self._request_async(
                "post",
                "/v1/billing/meter_events",
                base_address="api",
                params=params,
                options=options,
            ),
        )
