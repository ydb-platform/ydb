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
    package="google.ads.googleads.v21.errors",
    marshal="google.ads.googleads.v21",
    manifest={
        "ChangeEventErrorEnum",
    },
)


class ChangeEventErrorEnum(proto.Message):
    r"""Container for enum describing possible change event errors."""

    class ChangeEventError(proto.Enum):
        r"""Enum describing possible change event errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            START_DATE_TOO_OLD (2):
                The requested start date is too old. It
                cannot be older than 30 days.
            CHANGE_DATE_RANGE_INFINITE (3):
                The change_event search request must specify a finite range
                filter on change_date_time.
            CHANGE_DATE_RANGE_NEGATIVE (4):
                The change event search request has specified
                invalid date time filters that can never
                logically produce any valid results (for
                example, start time after end time).
            LIMIT_NOT_SPECIFIED (5):
                The change_event search request must specify a LIMIT.
            INVALID_LIMIT_CLAUSE (6):
                The LIMIT specified by change_event request should be less
                than or equal to 10K.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        START_DATE_TOO_OLD = 2
        CHANGE_DATE_RANGE_INFINITE = 3
        CHANGE_DATE_RANGE_NEGATIVE = 4
        LIMIT_NOT_SPECIFIED = 5
        INVALID_LIMIT_CLAUSE = 6


__all__ = tuple(sorted(__protobuf__.manifest))
