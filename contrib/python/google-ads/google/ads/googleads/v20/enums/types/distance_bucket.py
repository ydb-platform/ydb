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
        "DistanceBucketEnum",
    },
)


class DistanceBucketEnum(proto.Message):
    r"""Container for distance buckets of a user's distance from an
    advertiser's location extension.

    """

    class DistanceBucket(proto.Enum):
        r"""The distance bucket for a user's distance from an
        advertiser's location extension.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            WITHIN_700M (2):
                User was within 700m of the location.
            WITHIN_1KM (3):
                User was within 1KM of the location.
            WITHIN_5KM (4):
                User was within 5KM of the location.
            WITHIN_10KM (5):
                User was within 10KM of the location.
            WITHIN_15KM (6):
                User was within 15KM of the location.
            WITHIN_20KM (7):
                User was within 20KM of the location.
            WITHIN_25KM (8):
                User was within 25KM of the location.
            WITHIN_30KM (9):
                User was within 30KM of the location.
            WITHIN_35KM (10):
                User was within 35KM of the location.
            WITHIN_40KM (11):
                User was within 40KM of the location.
            WITHIN_45KM (12):
                User was within 45KM of the location.
            WITHIN_50KM (13):
                User was within 50KM of the location.
            WITHIN_55KM (14):
                User was within 55KM of the location.
            WITHIN_60KM (15):
                User was within 60KM of the location.
            WITHIN_65KM (16):
                User was within 65KM of the location.
            BEYOND_65KM (17):
                User was beyond 65KM of the location.
            WITHIN_0_7MILES (18):
                User was within 0.7 miles of the location.
            WITHIN_1MILE (19):
                User was within 1 mile of the location.
            WITHIN_5MILES (20):
                User was within 5 miles of the location.
            WITHIN_10MILES (21):
                User was within 10 miles of the location.
            WITHIN_15MILES (22):
                User was within 15 miles of the location.
            WITHIN_20MILES (23):
                User was within 20 miles of the location.
            WITHIN_25MILES (24):
                User was within 25 miles of the location.
            WITHIN_30MILES (25):
                User was within 30 miles of the location.
            WITHIN_35MILES (26):
                User was within 35 miles of the location.
            WITHIN_40MILES (27):
                User was within 40 miles of the location.
            BEYOND_40MILES (28):
                User was beyond 40 miles of the location.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        WITHIN_700M = 2
        WITHIN_1KM = 3
        WITHIN_5KM = 4
        WITHIN_10KM = 5
        WITHIN_15KM = 6
        WITHIN_20KM = 7
        WITHIN_25KM = 8
        WITHIN_30KM = 9
        WITHIN_35KM = 10
        WITHIN_40KM = 11
        WITHIN_45KM = 12
        WITHIN_50KM = 13
        WITHIN_55KM = 14
        WITHIN_60KM = 15
        WITHIN_65KM = 16
        BEYOND_65KM = 17
        WITHIN_0_7MILES = 18
        WITHIN_1MILE = 19
        WITHIN_5MILES = 20
        WITHIN_10MILES = 21
        WITHIN_15MILES = 22
        WITHIN_20MILES = 23
        WITHIN_25MILES = 24
        WITHIN_30MILES = 25
        WITHIN_35MILES = 26
        WITHIN_40MILES = 27
        BEYOND_40MILES = 28


__all__ = tuple(sorted(__protobuf__.manifest))
