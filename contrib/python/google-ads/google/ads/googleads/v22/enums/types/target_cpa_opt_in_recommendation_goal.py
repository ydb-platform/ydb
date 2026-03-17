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
        "TargetCpaOptInRecommendationGoalEnum",
    },
)


class TargetCpaOptInRecommendationGoalEnum(proto.Message):
    r"""Container for enum describing goals for TargetCpaOptIn
    recommendation.

    """

    class TargetCpaOptInRecommendationGoal(proto.Enum):
        r"""Goal of TargetCpaOptIn recommendation.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            SAME_COST (2):
                Recommendation to set Target CPA to maintain
                the same cost.
            SAME_CONVERSIONS (3):
                Recommendation to set Target CPA to maintain
                the same conversions.
            SAME_CPA (4):
                Recommendation to set Target CPA to maintain
                the same CPA.
            CLOSEST_CPA (5):
                Recommendation to set Target CPA to a value
                that is as close as possible to, yet lower than,
                the actual CPA (computed for past 28 days).
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        SAME_COST = 2
        SAME_CONVERSIONS = 3
        SAME_CPA = 4
        CLOSEST_CPA = 5


__all__ = tuple(sorted(__protobuf__.manifest))
