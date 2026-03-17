# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.identity._verification_report import VerificationReport
    from stripe.params.identity._verification_report_list_params import (
        VerificationReportListParams,
    )
    from stripe.params.identity._verification_report_retrieve_params import (
        VerificationReportRetrieveParams,
    )


class VerificationReportService(StripeService):
    def list(
        self,
        params: Optional["VerificationReportListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[VerificationReport]":
        """
        List all verification reports.
        """
        return cast(
            "ListObject[VerificationReport]",
            self._request(
                "get",
                "/v1/identity/verification_reports",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["VerificationReportListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[VerificationReport]":
        """
        List all verification reports.
        """
        return cast(
            "ListObject[VerificationReport]",
            await self._request_async(
                "get",
                "/v1/identity/verification_reports",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        report: str,
        params: Optional["VerificationReportRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "VerificationReport":
        """
        Retrieves an existing VerificationReport
        """
        return cast(
            "VerificationReport",
            self._request(
                "get",
                "/v1/identity/verification_reports/{report}".format(
                    report=sanitize_id(report),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        report: str,
        params: Optional["VerificationReportRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "VerificationReport":
        """
        Retrieves an existing VerificationReport
        """
        return cast(
            "VerificationReport",
            await self._request_async(
                "get",
                "/v1/identity/verification_reports/{report}".format(
                    report=sanitize_id(report),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
