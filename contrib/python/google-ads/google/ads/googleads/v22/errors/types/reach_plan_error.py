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
    package="google.ads.googleads.v22.errors",
    marshal="google.ads.googleads.v22",
    manifest={
        "ReachPlanErrorEnum",
    },
)


class ReachPlanErrorEnum(proto.Message):
    r"""Container for enum describing possible errors returned from
    the ReachPlanService.

    """

    class ReachPlanError(proto.Enum):
        r"""Enum describing possible errors from ReachPlanService.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            NOT_FORECASTABLE_MISSING_RATE (2):
                Not forecastable due to missing rate card
                data.
            NOT_FORECASTABLE_NOT_ENOUGH_INVENTORY (3):
                Not forecastable due to not enough inventory.
            NOT_FORECASTABLE_ACCOUNT_NOT_ENABLED (4):
                Not forecastable due to account not being
                enabled.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        NOT_FORECASTABLE_MISSING_RATE = 2
        NOT_FORECASTABLE_NOT_ENOUGH_INVENTORY = 3
        NOT_FORECASTABLE_ACCOUNT_NOT_ENABLED = 4


__all__ = tuple(sorted(__protobuf__.manifest))
