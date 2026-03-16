# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.params.reporting._report_run_create_params import (
        ReportRunCreateParams,
    )
    from stripe.params.reporting._report_run_list_params import (
        ReportRunListParams,
    )
    from stripe.params.reporting._report_run_retrieve_params import (
        ReportRunRetrieveParams,
    )
    from stripe.reporting._report_run import ReportRun


class ReportRunService(StripeService):
    def list(
        self,
        params: Optional["ReportRunListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[ReportRun]":
        """
        Returns a list of Report Runs, with the most recent appearing first.
        """
        return cast(
            "ListObject[ReportRun]",
            self._request(
                "get",
                "/v1/reporting/report_runs",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["ReportRunListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[ReportRun]":
        """
        Returns a list of Report Runs, with the most recent appearing first.
        """
        return cast(
            "ListObject[ReportRun]",
            await self._request_async(
                "get",
                "/v1/reporting/report_runs",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: "ReportRunCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ReportRun":
        """
        Creates a new object and begin running the report. (Certain report types require a [live-mode API key](https://stripe.com/docs/keys#test-live-modes).)
        """
        return cast(
            "ReportRun",
            self._request(
                "post",
                "/v1/reporting/report_runs",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "ReportRunCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ReportRun":
        """
        Creates a new object and begin running the report. (Certain report types require a [live-mode API key](https://stripe.com/docs/keys#test-live-modes).)
        """
        return cast(
            "ReportRun",
            await self._request_async(
                "post",
                "/v1/reporting/report_runs",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        report_run: str,
        params: Optional["ReportRunRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ReportRun":
        """
        Retrieves the details of an existing Report Run.
        """
        return cast(
            "ReportRun",
            self._request(
                "get",
                "/v1/reporting/report_runs/{report_run}".format(
                    report_run=sanitize_id(report_run),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        report_run: str,
        params: Optional["ReportRunRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ReportRun":
        """
        Retrieves the details of an existing Report Run.
        """
        return cast(
            "ReportRun",
            await self._request_async(
                "get",
                "/v1/reporting/report_runs/{report_run}".format(
                    report_run=sanitize_id(report_run),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
