# -*- coding: utf-8 -*-
# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from __future__ import annotations

from typing import MutableSequence

import proto  # type: ignore

from google.ads.googleads.v20.enums.types import (
    offline_conversion_diagnostic_status_enum,
)
from google.ads.googleads.v20.enums.types import (
    offline_event_upload_client_enum,
)
from google.ads.googleads.v20.resources.types import (
    offline_conversion_upload_client_summary,
)


__protobuf__ = proto.module(
    package="google.ads.googleads.v20.resources",
    marshal="google.ads.googleads.v20",
    manifest={
        "OfflineConversionUploadConversionActionSummary",
    },
)


class OfflineConversionUploadConversionActionSummary(proto.Message):
    r"""Offline conversion upload summary at conversion action level.

    Attributes:
        resource_name (str):
            Output only. The resource name of the offline conversion
            upload summary at conversion action level. Offline
            conversion upload conversion action summary resource names
            have the form:

            ``customers/{customer_id}/offlineConversionUploadConversionActionSummaries/{conversion_action_id}~{client}``
        client (google.ads.googleads.v20.enums.types.OfflineEventUploadClientEnum.OfflineEventUploadClient):
            Output only. Client type of the upload event.
        conversion_action_id (int):
            Output only. Conversion action id.
        conversion_action_name (str):
            Output only. The name of the conversion
            action.
        status (google.ads.googleads.v20.enums.types.OfflineConversionDiagnosticStatusEnum.OfflineConversionDiagnosticStatus):
            Output only. Overall status for offline
            conversion upload conversion action summary.
            Status is generated from most recent calendar
            day with upload stats.
        total_event_count (int):
            Output only. Total count of uploaded events.
        successful_event_count (int):
            Output only. Total count of successful
            uploaded events.
        pending_event_count (int):
            Output only. Total count of pending uploaded
            events.
        last_upload_date_time (str):
            Output only. Date for the latest upload
            batch. The format is "yyyy-mm-dd hh:mm:ss", and
            it's in the time zone of the Google Ads account.
        daily_summaries (MutableSequence[google.ads.googleads.v20.resources.types.OfflineConversionSummary]):
            Output only. Summary of history stats by last
            N days.
        job_summaries (MutableSequence[google.ads.googleads.v20.resources.types.OfflineConversionSummary]):
            Output only. Summary of history stats by last
            N jobs.
        alerts (MutableSequence[google.ads.googleads.v20.resources.types.OfflineConversionAlert]):
            Output only. Details for each error code.
            Alerts are generated from most recent calendar
            day with upload stats.
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    client: (
        offline_event_upload_client_enum.OfflineEventUploadClientEnum.OfflineEventUploadClient
    ) = proto.Field(
        proto.ENUM,
        number=2,
        enum=offline_event_upload_client_enum.OfflineEventUploadClientEnum.OfflineEventUploadClient,
    )
    conversion_action_id: int = proto.Field(
        proto.INT64,
        number=3,
    )
    conversion_action_name: str = proto.Field(
        proto.STRING,
        number=4,
    )
    status: (
        offline_conversion_diagnostic_status_enum.OfflineConversionDiagnosticStatusEnum.OfflineConversionDiagnosticStatus
    ) = proto.Field(
        proto.ENUM,
        number=5,
        enum=offline_conversion_diagnostic_status_enum.OfflineConversionDiagnosticStatusEnum.OfflineConversionDiagnosticStatus,
    )
    total_event_count: int = proto.Field(
        proto.INT64,
        number=6,
    )
    successful_event_count: int = proto.Field(
        proto.INT64,
        number=7,
    )
    pending_event_count: int = proto.Field(
        proto.INT64,
        number=8,
    )
    last_upload_date_time: str = proto.Field(
        proto.STRING,
        number=9,
    )
    daily_summaries: MutableSequence[
        offline_conversion_upload_client_summary.OfflineConversionSummary
    ] = proto.RepeatedField(
        proto.MESSAGE,
        number=10,
        message=offline_conversion_upload_client_summary.OfflineConversionSummary,
    )
    job_summaries: MutableSequence[
        offline_conversion_upload_client_summary.OfflineConversionSummary
    ] = proto.RepeatedField(
        proto.MESSAGE,
        number=11,
        message=offline_conversion_upload_client_summary.OfflineConversionSummary,
    )
    alerts: MutableSequence[
        offline_conversion_upload_client_summary.OfflineConversionAlert
    ] = proto.RepeatedField(
        proto.MESSAGE,
        number=12,
        message=offline_conversion_upload_client_summary.OfflineConversionAlert,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
