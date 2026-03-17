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
        "ChangeStatusResourceTypeEnum",
    },
)


class ChangeStatusResourceTypeEnum(proto.Message):
    r"""Container for enum describing supported resource types for
    the ChangeStatus resource.

    """

    class ChangeStatusResourceType(proto.Enum):
        r"""Enum listing the resource types support by the ChangeStatus
        resource.

        Values:
            UNSPECIFIED (0):
                No value has been specified.
            UNKNOWN (1):
                Used for return value only. Represents an
                unclassified resource unknown in this version.
            AD_GROUP (3):
                An AdGroup resource change.
            AD_GROUP_AD (4):
                An AdGroupAd resource change.
            AD_GROUP_CRITERION (5):
                An AdGroupCriterion resource change.
            CAMPAIGN (6):
                A Campaign resource change.
            CAMPAIGN_CRITERION (7):
                A CampaignCriterion resource change.
            CAMPAIGN_BUDGET (8):
                A CampaignBudget resource change.
            FEED (9):
                A Feed resource change.
            FEED_ITEM (10):
                A FeedItem resource change.
            AD_GROUP_FEED (11):
                An AdGroupFeed resource change.
            CAMPAIGN_FEED (12):
                A CampaignFeed resource change.
            AD_GROUP_BID_MODIFIER (13):
                An AdGroupBidModifier resource change.
            SHARED_SET (14):
                A SharedSet resource change.
            CAMPAIGN_SHARED_SET (15):
                A CampaignSharedSet resource change.
            ASSET (16):
                An Asset resource change.
            CUSTOMER_ASSET (17):
                A CustomerAsset resource change.
            CAMPAIGN_ASSET (18):
                A CampaignAsset resource change.
            AD_GROUP_ASSET (19):
                An AdGroupAsset resource change.
            COMBINED_AUDIENCE (20):
                A CombinedAudience resource change.
            ASSET_GROUP (21):
                An AssetGroup resource change.
            ASSET_SET (22):
                An AssetSet resource change.
            CAMPAIGN_ASSET_SET (23):
                A CampaignAssetSet resource change.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        AD_GROUP = 3
        AD_GROUP_AD = 4
        AD_GROUP_CRITERION = 5
        CAMPAIGN = 6
        CAMPAIGN_CRITERION = 7
        CAMPAIGN_BUDGET = 8
        FEED = 9
        FEED_ITEM = 10
        AD_GROUP_FEED = 11
        CAMPAIGN_FEED = 12
        AD_GROUP_BID_MODIFIER = 13
        SHARED_SET = 14
        CAMPAIGN_SHARED_SET = 15
        ASSET = 16
        CUSTOMER_ASSET = 17
        CAMPAIGN_ASSET = 18
        AD_GROUP_ASSET = 19
        COMBINED_AUDIENCE = 20
        ASSET_GROUP = 21
        ASSET_SET = 22
        CAMPAIGN_ASSET_SET = 23


__all__ = tuple(sorted(__protobuf__.manifest))
