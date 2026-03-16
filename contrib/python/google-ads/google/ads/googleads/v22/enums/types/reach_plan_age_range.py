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
        "ReachPlanAgeRangeEnum",
    },
)


class ReachPlanAgeRangeEnum(proto.Message):
    r"""Message describing plannable age ranges."""

    class ReachPlanAgeRange(proto.Enum):
        r"""Possible plannable age range values.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                The value is unknown in this version.
            AGE_RANGE_18_24 (503001):
                Between 18 and 24 years old.
            AGE_RANGE_18_34 (2):
                Between 18 and 34 years old.
            AGE_RANGE_18_44 (3):
                Between 18 and 44 years old.
            AGE_RANGE_18_49 (4):
                Between 18 and 49 years old.
            AGE_RANGE_18_54 (5):
                Between 18 and 54 years old.
            AGE_RANGE_18_64 (6):
                Between 18 and 64 years old.
            AGE_RANGE_18_65_UP (7):
                Between 18 and 65+ years old.
            AGE_RANGE_21_34 (8):
                Between 21 and 34 years old.
            AGE_RANGE_25_34 (503002):
                Between 25 and 34 years old.
            AGE_RANGE_25_44 (9):
                Between 25 and 44 years old.
            AGE_RANGE_25_49 (10):
                Between 25 and 49 years old.
            AGE_RANGE_25_54 (11):
                Between 25 and 54 years old.
            AGE_RANGE_25_64 (12):
                Between 25 and 64 years old.
            AGE_RANGE_25_65_UP (13):
                Between 25 and 65+ years old.
            AGE_RANGE_35_44 (503003):
                Between 35 and 44 years old.
            AGE_RANGE_35_49 (14):
                Between 35 and 49 years old.
            AGE_RANGE_35_54 (15):
                Between 35 and 54 years old.
            AGE_RANGE_35_64 (16):
                Between 35 and 64 years old.
            AGE_RANGE_35_65_UP (17):
                Between 35 and 65+ years old.
            AGE_RANGE_45_54 (503004):
                Between 45 and 54 years old.
            AGE_RANGE_45_64 (18):
                Between 45 and 64 years old.
            AGE_RANGE_45_65_UP (19):
                Between 45 and 65+ years old.
            AGE_RANGE_50_65_UP (20):
                Between 50 and 65+ years old.
            AGE_RANGE_55_64 (503005):
                Between 55 and 64 years old.
            AGE_RANGE_55_65_UP (21):
                Between 55 and 65+ years old.
            AGE_RANGE_65_UP (503006):
                65 years old and beyond.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        AGE_RANGE_18_24 = 503001
        AGE_RANGE_18_34 = 2
        AGE_RANGE_18_44 = 3
        AGE_RANGE_18_49 = 4
        AGE_RANGE_18_54 = 5
        AGE_RANGE_18_64 = 6
        AGE_RANGE_18_65_UP = 7
        AGE_RANGE_21_34 = 8
        AGE_RANGE_25_34 = 503002
        AGE_RANGE_25_44 = 9
        AGE_RANGE_25_49 = 10
        AGE_RANGE_25_54 = 11
        AGE_RANGE_25_64 = 12
        AGE_RANGE_25_65_UP = 13
        AGE_RANGE_35_44 = 503003
        AGE_RANGE_35_49 = 14
        AGE_RANGE_35_54 = 15
        AGE_RANGE_35_64 = 16
        AGE_RANGE_35_65_UP = 17
        AGE_RANGE_45_54 = 503004
        AGE_RANGE_45_64 = 18
        AGE_RANGE_45_65_UP = 19
        AGE_RANGE_50_65_UP = 20
        AGE_RANGE_55_64 = 503005
        AGE_RANGE_55_65_UP = 21
        AGE_RANGE_65_UP = 503006


__all__ = tuple(sorted(__protobuf__.manifest))
