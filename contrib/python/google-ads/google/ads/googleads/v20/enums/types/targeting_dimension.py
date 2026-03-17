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
        "TargetingDimensionEnum",
    },
)


class TargetingDimensionEnum(proto.Message):
    r"""The dimensions that can be targeted."""

    class TargetingDimension(proto.Enum):
        r"""Enum describing possible targeting dimensions.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            KEYWORD (2):
                Keyword criteria, for example, 'mars cruise'.
                KEYWORD may be used as a custom bid dimension.
                Keywords are always a targeting dimension, so
                may not be set as a target "ALL" dimension with
                TargetRestriction.
            AUDIENCE (3):
                Audience criteria, which include user list,
                user interest, custom affinity,  and custom in
                market.
            TOPIC (4):
                Topic criteria for targeting categories of
                content, for example, 'category::Animals>Pets'
                Used for Display and Video targeting.
            GENDER (5):
                Criteria for targeting gender.
            AGE_RANGE (6):
                Criteria for targeting age ranges.
            PLACEMENT (7):
                Placement criteria, which include websites
                like 'www.flowers4sale.com', as well as mobile
                applications, mobile app categories, YouTube
                videos, and YouTube channels.
            PARENTAL_STATUS (8):
                Criteria for parental status targeting.
            INCOME_RANGE (9):
                Criteria for income range targeting.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        KEYWORD = 2
        AUDIENCE = 3
        TOPIC = 4
        GENDER = 5
        AGE_RANGE = 6
        PLACEMENT = 7
        PARENTAL_STATUS = 8
        INCOME_RANGE = 9


__all__ = tuple(sorted(__protobuf__.manifest))
