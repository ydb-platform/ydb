# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.params.sigma._scheduled_query_run_list_params import (
        ScheduledQueryRunListParams,
    )
    from stripe.params.sigma._scheduled_query_run_retrieve_params import (
        ScheduledQueryRunRetrieveParams,
    )
    from stripe.sigma._scheduled_query_run import ScheduledQueryRun


class ScheduledQueryRunService(StripeService):
    def list(
        self,
        params: Optional["ScheduledQueryRunListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[ScheduledQueryRun]":
        """
        Returns a list of scheduled query runs.
        """
        return cast(
            "ListObject[ScheduledQueryRun]",
            self._request(
                "get",
                "/v1/sigma/scheduled_query_runs",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["ScheduledQueryRunListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[ScheduledQueryRun]":
        """
        Returns a list of scheduled query runs.
        """
        return cast(
            "ListObject[ScheduledQueryRun]",
            await self._request_async(
                "get",
                "/v1/sigma/scheduled_query_runs",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        scheduled_query_run: str,
        params: Optional["ScheduledQueryRunRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ScheduledQueryRun":
        """
        Retrieves the details of an scheduled query run.
        """
        return cast(
            "ScheduledQueryRun",
            self._request(
                "get",
                "/v1/sigma/scheduled_query_runs/{scheduled_query_run}".format(
                    scheduled_query_run=sanitize_id(scheduled_query_run),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        scheduled_query_run: str,
        params: Optional["ScheduledQueryRunRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ScheduledQueryRun":
        """
        Retrieves the details of an scheduled query run.
        """
        return cast(
            "ScheduledQueryRun",
            await self._request_async(
                "get",
                "/v1/sigma/scheduled_query_runs/{scheduled_query_run}".format(
                    scheduled_query_run=sanitize_id(scheduled_query_run),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
