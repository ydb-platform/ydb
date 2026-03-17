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


import proto  # type: ignore


__protobuf__ = proto.module(
    package="google.ads.googleads.v22.errors",
    marshal="google.ads.googleads.v22",
    manifest={
        "DateErrorEnum",
    },
)


class DateErrorEnum(proto.Message):
    r"""Container for enum describing possible date errors."""

    class DateError(proto.Enum):
        r"""Enum describing possible date errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            INVALID_FIELD_VALUES_IN_DATE (2):
                Given field values do not correspond to a
                valid date.
            INVALID_FIELD_VALUES_IN_DATE_TIME (3):
                Given field values do not correspond to a
                valid date time.
            INVALID_STRING_DATE (4):
                The string date's format should be
                yyyy-mm-dd.
            INVALID_STRING_DATE_TIME_MICROS (6):
                The string date time's format should be
                yyyy-mm-dd hh:mm:ss.ssssss.
            INVALID_STRING_DATE_TIME_SECONDS (11):
                The string date time's format should be
                yyyy-mm-dd hh:mm:ss.
            INVALID_STRING_DATE_TIME_SECONDS_WITH_OFFSET (12):
                The string date time's format should be yyyy-mm-dd
                hh:mm:ss+\|-hh:mm.
            EARLIER_THAN_MINIMUM_DATE (7):
                Date is before allowed minimum.
            LATER_THAN_MAXIMUM_DATE (8):
                Date is after allowed maximum.
            DATE_RANGE_MINIMUM_DATE_LATER_THAN_MAXIMUM_DATE (9):
                Date range bounds are not in order.
            DATE_RANGE_MINIMUM_AND_MAXIMUM_DATES_BOTH_NULL (10):
                Both dates in range are null.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        INVALID_FIELD_VALUES_IN_DATE = 2
        INVALID_FIELD_VALUES_IN_DATE_TIME = 3
        INVALID_STRING_DATE = 4
        INVALID_STRING_DATE_TIME_MICROS = 6
        INVALID_STRING_DATE_TIME_SECONDS = 11
        INVALID_STRING_DATE_TIME_SECONDS_WITH_OFFSET = 12
        EARLIER_THAN_MINIMUM_DATE = 7
        LATER_THAN_MAXIMUM_DATE = 8
        DATE_RANGE_MINIMUM_DATE_LATER_THAN_MAXIMUM_DATE = 9
        DATE_RANGE_MINIMUM_AND_MAXIMUM_DATES_BOTH_NULL = 10


__all__ = tuple(sorted(__protobuf__.manifest))
