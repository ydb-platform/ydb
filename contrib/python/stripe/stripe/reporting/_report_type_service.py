# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.params.reporting._report_type_list_params import (
        ReportTypeListParams,
    )
    from stripe.params.reporting._report_type_retrieve_params import (
        ReportTypeRetrieveParams,
    )
    from stripe.reporting._report_type import ReportType


class ReportTypeService(StripeService):
    def list(
        self,
        params: Optional["ReportTypeListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[ReportType]":
        """
        Returns a full list of Report Types.
        """
        return cast(
            "ListObject[ReportType]",
            self._request(
                "get",
                "/v1/reporting/report_types",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["ReportTypeListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[ReportType]":
        """
        Returns a full list of Report Types.
        """
        return cast(
            "ListObject[ReportType]",
            await self._request_async(
                "get",
                "/v1/reporting/report_types",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        report_type: str,
        params: Optional["ReportTypeRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ReportType":
        """
        Retrieves the details of a Report Type. (Certain report types require a [live-mode API key](https://stripe.com/docs/keys#test-live-modes).)
        """
        return cast(
            "ReportType",
            self._request(
                "get",
                "/v1/reporting/report_types/{report_type}".format(
                    report_type=sanitize_id(report_type),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        report_type: str,
        params: Optional["ReportTypeRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ReportType":
        """
        Retrieves the details of a Report Type. (Certain report types require a [live-mode API key](https://stripe.com/docs/keys#test-live-modes).)
        """
        return cast(
            "ReportType",
            await self._request_async(
                "get",
                "/v1/reporting/report_types/{report_type}".format(
                    report_type=sanitize_id(report_type),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
