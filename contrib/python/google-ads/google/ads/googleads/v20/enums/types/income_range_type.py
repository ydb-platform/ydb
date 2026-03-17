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
    package="google.ads.googleads.v20.enums",
    marshal="google.ads.googleads.v20",
    manifest={
        "IncomeRangeTypeEnum",
    },
)


class IncomeRangeTypeEnum(proto.Message):
    r"""Container for enum describing the type of demographic income
    ranges.

    """

    class IncomeRangeType(proto.Enum):
        r"""The type of demographic income ranges (for example, between
        0% to 50%).

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            INCOME_RANGE_0_50 (510001):
                0%-50%.
            INCOME_RANGE_50_60 (510002):
                50% to 60%.
            INCOME_RANGE_60_70 (510003):
                60% to 70%.
            INCOME_RANGE_70_80 (510004):
                70% to 80%.
            INCOME_RANGE_80_90 (510005):
                80% to 90%.
            INCOME_RANGE_90_UP (510006):
                Greater than 90%.
            INCOME_RANGE_UNDETERMINED (510000):
                Undetermined income range.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        INCOME_RANGE_0_50 = 510001
        INCOME_RANGE_50_60 = 510002
        INCOME_RANGE_60_70 = 510003
        INCOME_RANGE_70_80 = 510004
        INCOME_RANGE_80_90 = 510005
        INCOME_RANGE_90_UP = 510006
        INCOME_RANGE_UNDETERMINED = 510000


__all__ = tuple(sorted(__protobuf__.manifest))
