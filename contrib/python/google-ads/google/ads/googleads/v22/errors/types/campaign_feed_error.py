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
        "CampaignFeedErrorEnum",
    },
)


class CampaignFeedErrorEnum(proto.Message):
    r"""Container for enum describing possible campaign feed errors."""

    class CampaignFeedError(proto.Enum):
        r"""Enum describing possible campaign feed errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            FEED_ALREADY_EXISTS_FOR_PLACEHOLDER_TYPE (2):
                An active feed already exists for this
                campaign and placeholder type.
            CANNOT_CREATE_FOR_REMOVED_FEED (4):
                The specified feed is removed.
            CANNOT_CREATE_ALREADY_EXISTING_CAMPAIGN_FEED (5):
                The CampaignFeed already exists. UPDATE
                should be used to modify the existing
                CampaignFeed.
            CANNOT_MODIFY_REMOVED_CAMPAIGN_FEED (6):
                Cannot update removed campaign feed.
            INVALID_PLACEHOLDER_TYPE (7):
                Invalid placeholder type.
            MISSING_FEEDMAPPING_FOR_PLACEHOLDER_TYPE (8):
                Feed mapping for this placeholder type does
                not exist.
            NO_EXISTING_LOCATION_CUSTOMER_FEED (9):
                Location CampaignFeeds cannot be created
                unless there is a location CustomerFeed for the
                specified feed.
            LEGACY_FEED_TYPE_READ_ONLY (10):
                Feed is read only.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        FEED_ALREADY_EXISTS_FOR_PLACEHOLDER_TYPE = 2
        CANNOT_CREATE_FOR_REMOVED_FEED = 4
        CANNOT_CREATE_ALREADY_EXISTING_CAMPAIGN_FEED = 5
        CANNOT_MODIFY_REMOVED_CAMPAIGN_FEED = 6
        INVALID_PLACEHOLDER_TYPE = 7
        MISSING_FEEDMAPPING_FOR_PLACEHOLDER_TYPE = 8
        NO_EXISTING_LOCATION_CUSTOMER_FEED = 9
        LEGACY_FEED_TYPE_READ_ONLY = 10


__all__ = tuple(sorted(__protobuf__.manifest))
