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
    package="google.ads.googleads.v21.enums",
    marshal="google.ads.googleads.v21",
    manifest={
        "PriceExtensionPriceUnitEnum",
    },
)


class PriceExtensionPriceUnitEnum(proto.Message):
    r"""Container for enum describing price extension price unit."""

    class PriceExtensionPriceUnit(proto.Enum):
        r"""Price extension price unit.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            PER_HOUR (2):
                Per hour.
            PER_DAY (3):
                Per day.
            PER_WEEK (4):
                Per week.
            PER_MONTH (5):
                Per month.
            PER_YEAR (6):
                Per year.
            PER_NIGHT (7):
                Per night.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        PER_HOUR = 2
        PER_DAY = 3
        PER_WEEK = 4
        PER_MONTH = 5
        PER_YEAR = 6
        PER_NIGHT = 7


__all__ = tuple(sorted(__protobuf__.manifest))
