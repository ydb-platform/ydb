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
        "AgeRangeTypeEnum",
    },
)


class AgeRangeTypeEnum(proto.Message):
    r"""Container for enum describing the type of demographic age
    ranges.

    """

    class AgeRangeType(proto.Enum):
        r"""The type of demographic age ranges (for example, between 18
        and 24 years old).

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            AGE_RANGE_18_24 (503001):
                Between 18 and 24 years old.
            AGE_RANGE_25_34 (503002):
                Between 25 and 34 years old.
            AGE_RANGE_35_44 (503003):
                Between 35 and 44 years old.
            AGE_RANGE_45_54 (503004):
                Between 45 and 54 years old.
            AGE_RANGE_55_64 (503005):
                Between 55 and 64 years old.
            AGE_RANGE_65_UP (503006):
                65 years old and beyond.
            AGE_RANGE_UNDETERMINED (503999):
                Undetermined age range.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        AGE_RANGE_18_24 = 503001
        AGE_RANGE_25_34 = 503002
        AGE_RANGE_35_44 = 503003
        AGE_RANGE_45_54 = 503004
        AGE_RANGE_55_64 = 503005
        AGE_RANGE_65_UP = 503006
        AGE_RANGE_UNDETERMINED = 503999


__all__ = tuple(sorted(__protobuf__.manifest))
