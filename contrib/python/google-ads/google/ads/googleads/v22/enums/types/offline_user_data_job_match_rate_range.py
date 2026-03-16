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
        "OfflineUserDataJobMatchRateRangeEnum",
    },
)


class OfflineUserDataJobMatchRateRangeEnum(proto.Message):
    r"""Container for enum describing reasons match rate ranges for a
    customer match list upload.

    """

    class OfflineUserDataJobMatchRateRange(proto.Enum):
        r"""The match rate range of an offline user data job.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Default value for match rate range.
            MATCH_RANGE_LESS_THAN_20 (2):
                Match rate range for offline data upload
                entity is between 0% and 19%.
            MATCH_RANGE_20_TO_30 (3):
                Match rate range for offline data upload
                entity is between 20% and 30%.
            MATCH_RANGE_31_TO_40 (4):
                Match rate range for offline data upload
                entity is between 31% and 40%.
            MATCH_RANGE_41_TO_50 (5):
                Match rate range for offline data upload
                entity is between 41% and 50%.
            MATCH_RANGE_51_TO_60 (6):
                Match rate range for offline data upload
                entity is between 51% and 60%.
            MATCH_RANGE_61_TO_70 (7):
                Match rate range for offline data upload
                entity is between 61% and 70%.
            MATCH_RANGE_71_TO_80 (8):
                Match rate range for offline data upload
                entity is between 71% and 80%.
            MATCH_RANGE_81_TO_90 (9):
                Match rate range for offline data upload
                entity is between 81% and 90%.
            MATCH_RANGE_91_TO_100 (10):
                Match rate range for offline data upload
                entity is more than or equal to 91%.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        MATCH_RANGE_LESS_THAN_20 = 2
        MATCH_RANGE_20_TO_30 = 3
        MATCH_RANGE_31_TO_40 = 4
        MATCH_RANGE_41_TO_50 = 5
        MATCH_RANGE_51_TO_60 = 6
        MATCH_RANGE_61_TO_70 = 7
        MATCH_RANGE_71_TO_80 = 8
        MATCH_RANGE_81_TO_90 = 9
        MATCH_RANGE_91_TO_100 = 10


__all__ = tuple(sorted(__protobuf__.manifest))
