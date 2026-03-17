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
        "FrequencyCapTimeUnitEnum",
    },
)


class FrequencyCapTimeUnitEnum(proto.Message):
    r"""Container for enum describing the unit of time the cap is
    defined at.

    """

    class FrequencyCapTimeUnit(proto.Enum):
        r"""Unit of time the cap is defined at (for example, day, week).

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            DAY (2):
                The cap would define limit per one day.
            WEEK (3):
                The cap would define limit per one week.
            MONTH (4):
                The cap would define limit per one month.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        DAY = 2
        WEEK = 3
        MONTH = 4


__all__ = tuple(sorted(__protobuf__.manifest))
