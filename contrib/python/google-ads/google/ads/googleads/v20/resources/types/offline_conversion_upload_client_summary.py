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
from google.ads.googleads.v20.errors.types import (
    collection_size_error as gage_collection_size_error,
)
from google.ads.googleads.v20.errors.types import (
    conversion_adjustment_upload_error as gage_conversion_adjustment_upload_error,
)
from google.ads.googleads.v20.errors.types import (
    conversion_upload_error as gage_conversion_upload_error,
)
from google.ads.googleads.v20.errors.types import date_error as gage_date_error
from google.ads.googleads.v20.errors.types import (
    distinct_error as gage_distinct_error,
)
from google.ads.googleads.v20.errors.types import (
    field_error as gage_field_error,
)
from google.ads.googleads.v20.errors.types import (
    mutate_error as gage_mutate_error,
)
from google.ads.googleads.v20.errors.types import (
    not_allowlisted_error as gage_not_allowlisted_error,
)
from google.ads.googleads.v20.errors.types import (
    string_format_error as gage_string_format_error,
)
from google.ads.googleads.v20.errors.types import (
    string_length_error as gage_string_length_error,
)


__protobuf__ = proto.module(
    package="google.ads.googleads.v20.resources",
    marshal="google.ads.googleads.v20",
    manifest={
        "OfflineConversionUploadClientSummary",
        "OfflineConversionSummary",
        "OfflineConversionAlert",
        "OfflineConversionError",
    },
)


class OfflineConversionUploadClientSummary(proto.Message):
    r"""Offline conversion upload summary at customer level.

    Attributes:
        resource_name (str):
            Output only. The resource name of the offline conversion
            upload summary at customer level. Offline conversion upload
            client summary resource names have the form:

            ``customers/{customer_id}/offlineConversionUploadClientSummaries/{client}``
        client (google.ads.googleads.v20.enums.types.OfflineEventUploadClientEnum.OfflineEventUploadClient):
            Output only. Client type of the upload event.
        status (google.ads.googleads.v20.enums.types.OfflineConversionDiagnosticStatusEnum.OfflineConversionDiagnosticStatus):
            Output only. Overall status for offline
            conversion client summary. Status is generated
            from most recent calendar day with upload stats.
        total_event_count (int):
            Output only. Total count of uploaded events.
        successful_event_count (int):
            Output only. Total count of successful
            uploaded events.
        success_rate (float):
            Output only. Successful rate.
        pending_event_count (int):
            Output only. Total count of pending uploaded
            events.
        pending_rate (float):
            Output only. The ratio of total pending
            events to total events.
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
    status: (
        offline_conversion_diagnostic_status_enum.OfflineConversionDiagnosticStatusEnum.OfflineConversionDiagnosticStatus
    ) = proto.Field(
        proto.ENUM,
        number=3,
        enum=offline_conversion_diagnostic_status_enum.OfflineConversionDiagnosticStatusEnum.OfflineConversionDiagnosticStatus,
    )
    total_event_count: int = proto.Field(
        proto.INT64,
        number=4,
    )
    successful_event_count: int = proto.Field(
        proto.INT64,
        number=5,
    )
    success_rate: float = proto.Field(
        proto.DOUBLE,
        number=6,
    )
    pending_event_count: int = proto.Field(
        proto.INT64,
        number=11,
    )
    pending_rate: float = proto.Field(
        proto.DOUBLE,
        number=12,
    )
    last_upload_date_time: str = proto.Field(
        proto.STRING,
        number=7,
    )
    daily_summaries: MutableSequence["OfflineConversionSummary"] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=8,
            message="OfflineConversionSummary",
        )
    )
    job_summaries: MutableSequence["OfflineConversionSummary"] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=9,
            message="OfflineConversionSummary",
        )
    )
    alerts: MutableSequence["OfflineConversionAlert"] = proto.RepeatedField(
        proto.MESSAGE,
        number=10,
        message="OfflineConversionAlert",
    )


class OfflineConversionSummary(proto.Message):
    r"""Historical upload summary, grouped by upload date or job.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        successful_count (int):
            Output only. Total count of successful event.
        failed_count (int):
            Output only. Total count of failed event.
        pending_count (int):
            Output only. Total count of pending uploaded
            event.
        job_id (int):
            Output only. Dimension key for last N jobs.

            This field is a member of `oneof`_ ``dimension_key``.
        upload_date (str):
            Output only. Dimension key for last N days.

            This field is a member of `oneof`_ ``dimension_key``.
    """

    successful_count: int = proto.Field(
        proto.INT64,
        number=3,
    )
    failed_count: int = proto.Field(
        proto.INT64,
        number=4,
    )
    pending_count: int = proto.Field(
        proto.INT64,
        number=5,
    )
    job_id: int = proto.Field(
        proto.INT64,
        number=1,
        oneof="dimension_key",
    )
    upload_date: str = proto.Field(
        proto.STRING,
        number=2,
        oneof="dimension_key",
    )


class OfflineConversionAlert(proto.Message):
    r"""Alert for offline conversion client summary.

    Attributes:
        error (google.ads.googleads.v20.resources.types.OfflineConversionError):
            Output only. Error for offline conversion
            client alert.
        error_percentage (float):
            Output only. Percentage of the error, the range of this
            field should be [0, 1.0].
    """

    error: "OfflineConversionError" = proto.Field(
        proto.MESSAGE,
        number=1,
        message="OfflineConversionError",
    )
    error_percentage: float = proto.Field(
        proto.DOUBLE,
        number=2,
    )


class OfflineConversionError(proto.Message):
    r"""Possible errors for offline conversion client summary.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        collection_size_error (google.ads.googleads.v20.errors.types.CollectionSizeErrorEnum.CollectionSizeError):
            Output only. Collection size error.

            This field is a member of `oneof`_ ``error_code``.
        conversion_adjustment_upload_error (google.ads.googleads.v20.errors.types.ConversionAdjustmentUploadErrorEnum.ConversionAdjustmentUploadError):
            Output only. Conversion adjustment upload
            error.

            This field is a member of `oneof`_ ``error_code``.
        conversion_upload_error (google.ads.googleads.v20.errors.types.ConversionUploadErrorEnum.ConversionUploadError):
            Output only. Conversion upload error.

            This field is a member of `oneof`_ ``error_code``.
        date_error (google.ads.googleads.v20.errors.types.DateErrorEnum.DateError):
            Output only. Date error.

            This field is a member of `oneof`_ ``error_code``.
        distinct_error (google.ads.googleads.v20.errors.types.DistinctErrorEnum.DistinctError):
            Output only. Distinct error.

            This field is a member of `oneof`_ ``error_code``.
        field_error (google.ads.googleads.v20.errors.types.FieldErrorEnum.FieldError):
            Output only. Field error.

            This field is a member of `oneof`_ ``error_code``.
        mutate_error (google.ads.googleads.v20.errors.types.MutateErrorEnum.MutateError):
            Output only. Mutate error.

            This field is a member of `oneof`_ ``error_code``.
        not_allowlisted_error (google.ads.googleads.v20.errors.types.NotAllowlistedErrorEnum.NotAllowlistedError):
            Output only. Not allowlisted error.

            This field is a member of `oneof`_ ``error_code``.
        string_format_error (google.ads.googleads.v20.errors.types.StringFormatErrorEnum.StringFormatError):
            Output only. String format error.

            This field is a member of `oneof`_ ``error_code``.
        string_length_error (google.ads.googleads.v20.errors.types.StringLengthErrorEnum.StringLengthError):
            Output only. String length error.

            This field is a member of `oneof`_ ``error_code``.
    """

    collection_size_error: (
        gage_collection_size_error.CollectionSizeErrorEnum.CollectionSizeError
    ) = proto.Field(
        proto.ENUM,
        number=1,
        oneof="error_code",
        enum=gage_collection_size_error.CollectionSizeErrorEnum.CollectionSizeError,
    )
    conversion_adjustment_upload_error: (
        gage_conversion_adjustment_upload_error.ConversionAdjustmentUploadErrorEnum.ConversionAdjustmentUploadError
    ) = proto.Field(
        proto.ENUM,
        number=2,
        oneof="error_code",
        enum=gage_conversion_adjustment_upload_error.ConversionAdjustmentUploadErrorEnum.ConversionAdjustmentUploadError,
    )
    conversion_upload_error: (
        gage_conversion_upload_error.ConversionUploadErrorEnum.ConversionUploadError
    ) = proto.Field(
        proto.ENUM,
        number=3,
        oneof="error_code",
        enum=gage_conversion_upload_error.ConversionUploadErrorEnum.ConversionUploadError,
    )
    date_error: gage_date_error.DateErrorEnum.DateError = proto.Field(
        proto.ENUM,
        number=4,
        oneof="error_code",
        enum=gage_date_error.DateErrorEnum.DateError,
    )
    distinct_error: gage_distinct_error.DistinctErrorEnum.DistinctError = (
        proto.Field(
            proto.ENUM,
            number=5,
            oneof="error_code",
            enum=gage_distinct_error.DistinctErrorEnum.DistinctError,
        )
    )
    field_error: gage_field_error.FieldErrorEnum.FieldError = proto.Field(
        proto.ENUM,
        number=6,
        oneof="error_code",
        enum=gage_field_error.FieldErrorEnum.FieldError,
    )
    mutate_error: gage_mutate_error.MutateErrorEnum.MutateError = proto.Field(
        proto.ENUM,
        number=7,
        oneof="error_code",
        enum=gage_mutate_error.MutateErrorEnum.MutateError,
    )
    not_allowlisted_error: (
        gage_not_allowlisted_error.NotAllowlistedErrorEnum.NotAllowlistedError
    ) = proto.Field(
        proto.ENUM,
        number=8,
        oneof="error_code",
        enum=gage_not_allowlisted_error.NotAllowlistedErrorEnum.NotAllowlistedError,
    )
    string_format_error: (
        gage_string_format_error.StringFormatErrorEnum.StringFormatError
    ) = proto.Field(
        proto.ENUM,
        number=9,
        oneof="error_code",
        enum=gage_string_format_error.StringFormatErrorEnum.StringFormatError,
    )
    string_length_error: (
        gage_string_length_error.StringLengthErrorEnum.StringLengthError
    ) = proto.Field(
        proto.ENUM,
        number=10,
        oneof="error_code",
        enum=gage_string_length_error.StringLengthErrorEnum.StringLengthError,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
