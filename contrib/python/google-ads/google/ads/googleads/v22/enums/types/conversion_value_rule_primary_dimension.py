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
        "ConversionValueRulePrimaryDimensionEnum",
    },
)


class ConversionValueRulePrimaryDimensionEnum(proto.Message):
    r"""Container for enum describing value rule primary dimension
    for stats.

    """

    class ConversionValueRulePrimaryDimension(proto.Enum):
        r"""Identifies the primary dimension for conversion value rule
        stats.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            NO_RULE_APPLIED (2):
                For no-value-rule-applied conversions after
                value rule is enabled.
            ORIGINAL (3):
                Below are for value-rule-applied conversions:
                The original stats.
            NEW_VS_RETURNING_USER (4):
                When a new or returning customer condition is
                satisfied.
            GEO_LOCATION (5):
                When a query-time Geo location condition is
                satisfied.
            DEVICE (6):
                When a query-time browsing device condition
                is satisfied.
            AUDIENCE (7):
                When a query-time audience condition is
                satisfied.
            MULTIPLE (8):
                When multiple rules are applied.
            ITINERARY (9):
                When a query-time itinerary condition is
                satisfied.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        NO_RULE_APPLIED = 2
        ORIGINAL = 3
        NEW_VS_RETURNING_USER = 4
        GEO_LOCATION = 5
        DEVICE = 6
        AUDIENCE = 7
        MULTIPLE = 8
        ITINERARY = 9


__all__ = tuple(sorted(__protobuf__.manifest))
