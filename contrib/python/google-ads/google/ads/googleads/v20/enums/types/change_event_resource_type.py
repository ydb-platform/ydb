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
        "ChangeEventResourceTypeEnum",
    },
)


class ChangeEventResourceTypeEnum(proto.Message):
    r"""Container for enum describing supported resource types for
    the ChangeEvent resource.

    """

    class ChangeEventResourceType(proto.Enum):
        r"""Enum listing the resource types support by the ChangeEvent
        resource.

        Values:
            UNSPECIFIED (0):
                No value has been specified.
            UNKNOWN (1):
                Used for return value only. Represents an
                unclassified resource unknown in this version.
            AD (2):
                An Ad resource change.
            AD_GROUP (3):
                An AdGroup resource change.
            AD_GROUP_CRITERION (4):
                An AdGroupCriterion resource change.
            CAMPAIGN (5):
                A Campaign resource change.
            CAMPAIGN_BUDGET (6):
                A CampaignBudget resource change.
            AD_GROUP_BID_MODIFIER (7):
                An AdGroupBidModifier resource change.
            CAMPAIGN_CRITERION (8):
                A CampaignCriterion resource change.
            FEED (9):
                A Feed resource change.
            FEED_ITEM (10):
                A FeedItem resource change.
            CAMPAIGN_FEED (11):
                A CampaignFeed resource change.
            AD_GROUP_FEED (12):
                An AdGroupFeed resource change.
            AD_GROUP_AD (13):
                An AdGroupAd resource change.
            ASSET (14):
                An Asset resource change.
            CUSTOMER_ASSET (15):
                A CustomerAsset resource change.
            CAMPAIGN_ASSET (16):
                A CampaignAsset resource change.
            AD_GROUP_ASSET (17):
                An AdGroupAsset resource change.
            ASSET_SET (18):
                An AssetSet resource change.
            ASSET_SET_ASSET (19):
                An AssetSetAsset resource change.
            CAMPAIGN_ASSET_SET (20):
                A CampaignAssetSet resource change.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        AD = 2
        AD_GROUP = 3
        AD_GROUP_CRITERION = 4
        CAMPAIGN = 5
        CAMPAIGN_BUDGET = 6
        AD_GROUP_BID_MODIFIER = 7
        CAMPAIGN_CRITERION = 8
        FEED = 9
        FEED_ITEM = 10
        CAMPAIGN_FEED = 11
        AD_GROUP_FEED = 12
        AD_GROUP_AD = 13
        ASSET = 14
        CUSTOMER_ASSET = 15
        CAMPAIGN_ASSET = 16
        AD_GROUP_ASSET = 17
        ASSET_SET = 18
        ASSET_SET_ASSET = 19
        CAMPAIGN_ASSET_SET = 20


__all__ = tuple(sorted(__protobuf__.manifest))
