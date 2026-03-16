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
        "MinuteOfHourEnum",
    },
)


class MinuteOfHourEnum(proto.Message):
    r"""Container for enumeration of quarter-hours."""

    class MinuteOfHour(proto.Enum):
        r"""Enumerates of quarter-hours. For example, "FIFTEEN".

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                The value is unknown in this version.
            ZERO (2):
                Zero minutes past the hour.
            FIFTEEN (3):
                Fifteen minutes past the hour.
            THIRTY (4):
                Thirty minutes past the hour.
            FORTY_FIVE (5):
                Forty-five minutes past the hour.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        ZERO = 2
        FIFTEEN = 3
        THIRTY = 4
        FORTY_FIVE = 5


__all__ = tuple(sorted(__protobuf__.manifest))
