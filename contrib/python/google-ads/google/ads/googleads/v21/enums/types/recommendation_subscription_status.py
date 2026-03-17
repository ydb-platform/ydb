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
        "RecommendationSubscriptionStatusEnum",
    },
)


class RecommendationSubscriptionStatusEnum(proto.Message):
    r"""Container for enum describing recommendation subscription
    statuses.

    """

    class RecommendationSubscriptionStatus(proto.Enum):
        r"""Enum describing recommendation subscription statuses.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Output-only. Represents a format not yet
                defined in this enum.
            ENABLED (2):
                A subscription in the enabled state will
                automatically apply any recommendations of that
                type.
            PAUSED (3):
                Recommendations of the relevant type will not
                be automatically applied. Subscriptions cannot
                be deleted. Once created, they can only move
                between enabled and paused states.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        ENABLED = 2
        PAUSED = 3


__all__ = tuple(sorted(__protobuf__.manifest))
