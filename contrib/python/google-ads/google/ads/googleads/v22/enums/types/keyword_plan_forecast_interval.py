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
    package="google.ads.googleads.v22.enums",
    marshal="google.ads.googleads.v22",
    manifest={
        "KeywordPlanForecastIntervalEnum",
    },
)


class KeywordPlanForecastIntervalEnum(proto.Message):
    r"""Container for enumeration of forecast intervals."""

    class KeywordPlanForecastInterval(proto.Enum):
        r"""Forecast intervals.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                The value is unknown in this version.
            NEXT_WEEK (3):
                The next week date range for keyword plan.
                The next week is based on the default locale of
                the user's account and is mostly SUN-SAT or
                MON-SUN.
                This can be different from next-7 days.
            NEXT_MONTH (4):
                The next month date range for keyword plan.
            NEXT_QUARTER (5):
                The next quarter date range for keyword plan.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        NEXT_WEEK = 3
        NEXT_MONTH = 4
        NEXT_QUARTER = 5


__all__ = tuple(sorted(__protobuf__.manifest))
