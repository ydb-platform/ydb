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
    package="google.ads.googleads.v20.errors",
    marshal="google.ads.googleads.v20",
    manifest={
        "FeedItemTargetErrorEnum",
    },
)


class FeedItemTargetErrorEnum(proto.Message):
    r"""Container for enum describing possible feed item target
    errors.

    """

    class FeedItemTargetError(proto.Enum):
        r"""Enum describing possible feed item target errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            MUST_SET_TARGET_ONEOF_ON_CREATE (2):
                On CREATE, the FeedItemTarget must have a
                populated field in the oneof target.
            FEED_ITEM_TARGET_ALREADY_EXISTS (3):
                The specified feed item target already
                exists, so it cannot be added.
            FEED_ITEM_SCHEDULES_CANNOT_OVERLAP (4):
                The schedules for a given feed item cannot
                overlap.
            TARGET_LIMIT_EXCEEDED_FOR_GIVEN_TYPE (5):
                Too many targets of a given type were added
                for a single feed item.
            TOO_MANY_SCHEDULES_PER_DAY (6):
                Too many AdSchedules are enabled for the feed
                item for the given day.
            CANNOT_HAVE_ENABLED_CAMPAIGN_AND_ENABLED_AD_GROUP_TARGETS (7):
                A feed item may either have an enabled
                campaign target or an enabled ad group target.
            DUPLICATE_AD_SCHEDULE (8):
                Duplicate ad schedules aren't allowed.
            DUPLICATE_KEYWORD (9):
                Duplicate keywords aren't allowed.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        MUST_SET_TARGET_ONEOF_ON_CREATE = 2
        FEED_ITEM_TARGET_ALREADY_EXISTS = 3
        FEED_ITEM_SCHEDULES_CANNOT_OVERLAP = 4
        TARGET_LIMIT_EXCEEDED_FOR_GIVEN_TYPE = 5
        TOO_MANY_SCHEDULES_PER_DAY = 6
        CANNOT_HAVE_ENABLED_CAMPAIGN_AND_ENABLED_AD_GROUP_TARGETS = 7
        DUPLICATE_AD_SCHEDULE = 8
        DUPLICATE_KEYWORD = 9


__all__ = tuple(sorted(__protobuf__.manifest))
