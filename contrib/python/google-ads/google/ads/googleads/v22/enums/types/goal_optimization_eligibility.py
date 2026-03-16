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
        "GoalOptimizationEligibilityEnum",
    },
)


class GoalOptimizationEligibilityEnum(proto.Message):
    r"""Container for enum describing possible goal optimization
    eligibility.

    """

    class GoalOptimizationEligibility(proto.Enum):
        r"""The possible goal optimization eligibility.

        Values:
            UNSPECIFIED (0):
                The goal optimization status has not been
                specified.
            UNKNOWN (1):
                The goal optimization status is not known in
                this version.
            ELIGIBLE (2):
                The goal is eligible for campaign
                optimization.
            INELIGIBLE (3):
                The goal is not eligible for campaign
                optimization.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        ELIGIBLE = 2
        INELIGIBLE = 3


__all__ = tuple(sorted(__protobuf__.manifest))
