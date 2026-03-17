# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._request_options import RequestOptions
    from stripe.params.v2.billing._meter_event_adjustment_create_params import (
        MeterEventAdjustmentCreateParams,
    )
    from stripe.v2.billing._meter_event_adjustment import MeterEventAdjustment


class MeterEventAdjustmentService(StripeService):
    def create(
        self,
        params: "MeterEventAdjustmentCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "MeterEventAdjustment":
        """
        Creates a meter event adjustment to cancel a previously sent meter event.
        """
        return cast(
            "MeterEventAdjustment",
            self._request(
                "post",
                "/v2/billing/meter_event_adjustments",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "MeterEventAdjustmentCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "MeterEventAdjustment":
        """
        Creates a meter event adjustment to cancel a previously sent meter event.
        """
        return cast(
            "MeterEventAdjustment",
            await self._request_async(
                "post",
                "/v2/billing/meter_event_adjustments",
                base_address="api",
                params=params,
                options=options,
            ),
        )
