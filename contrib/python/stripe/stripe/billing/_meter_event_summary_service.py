# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.billing._meter_event_summary import MeterEventSummary
    from stripe.params.billing._meter_event_summary_list_params import (
        MeterEventSummaryListParams,
    )


class MeterEventSummaryService(StripeService):
    def list(
        self,
        id: str,
        params: "MeterEventSummaryListParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[MeterEventSummary]":
        """
        Retrieve a list of billing meter event summaries.
        """
        return cast(
            "ListObject[MeterEventSummary]",
            self._request(
                "get",
                "/v1/billing/meters/{id}/event_summaries".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        id: str,
        params: "MeterEventSummaryListParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[MeterEventSummary]":
        """
        Retrieve a list of billing meter event summaries.
        """
        return cast(
            "ListObject[MeterEventSummary]",
            await self._request_async(
                "get",
                "/v1/billing/meters/{id}/event_summaries".format(
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
