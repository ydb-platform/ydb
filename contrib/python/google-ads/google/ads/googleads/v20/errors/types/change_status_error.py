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
    package="google.ads.googleads.v20.errors",
    marshal="google.ads.googleads.v20",
    manifest={
        "ChangeStatusErrorEnum",
    },
)


class ChangeStatusErrorEnum(proto.Message):
    r"""Container for enum describing possible change status errors."""

    class ChangeStatusError(proto.Enum):
        r"""Enum describing possible change status errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            START_DATE_TOO_OLD (3):
                The requested start date is too old.
            CHANGE_DATE_RANGE_INFINITE (4):
                The change_status search request must specify a finite range
                filter on last_change_date_time.
            CHANGE_DATE_RANGE_NEGATIVE (5):
                The change status search request has
                specified invalid date time filters that can
                never logically produce any valid results (for
                example, start time after end time).
            LIMIT_NOT_SPECIFIED (6):
                The change_status search request must specify a LIMIT.
            INVALID_LIMIT_CLAUSE (7):
                The LIMIT specified by change_status request should be less
                than or equal to 10K.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        START_DATE_TOO_OLD = 3
        CHANGE_DATE_RANGE_INFINITE = 4
        CHANGE_DATE_RANGE_NEGATIVE = 5
        LIMIT_NOT_SPECIFIED = 6
        INVALID_LIMIT_CLAUSE = 7


__all__ = tuple(sorted(__protobuf__.manifest))
