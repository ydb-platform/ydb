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
        "AdGroupAdPrimaryStatusReasonEnum",
    },
)


class AdGroupAdPrimaryStatusReasonEnum(proto.Message):
    r"""AdGroupAd Primary Status Reason. Provides insight into why an
    ad group ad is not serving or not serving optimally. These
    reasons are aggregated to determine an overall
    AdGroupAdPrimaryStatus.

    """

    class AdGroupAdPrimaryStatusReason(proto.Enum):
        r"""Possible ad group ad status reasons.

        Values:
            UNSPECIFIED (0):
                No value has been specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            CAMPAIGN_REMOVED (2):
                The user-specified campaign status is
                removed. Contributes to
                AdGroupAdPrimaryStatus.REMOVED.
            CAMPAIGN_PAUSED (3):
                The user-specified campaign status is paused.
                Contributes to AdGroupAdPrimaryStatus.PAUSED.
            CAMPAIGN_PENDING (4):
                The user-specified time for this campaign to
                start is in the future. Contributes to
                AdGroupAdPrimaryStatus.PENDING.
            CAMPAIGN_ENDED (5):
                The user-specified time for this campaign to
                end has passed. Contributes to
                AdGroupAdPrimaryStatus.ENDED.
            AD_GROUP_PAUSED (6):
                The user-specified ad group status is paused.
                Contributes to AdGroupAdPrimaryStatus.PAUSED.
            AD_GROUP_REMOVED (7):
                The user-specified ad group status is
                removed. Contributes to
                AdGroupAdPrimaryStatus.REMOVED.
            AD_GROUP_AD_PAUSED (8):
                The user-specified ad status is paused.
                Contributes to AdGroupAdPrimaryStatus.PAUSED.
            AD_GROUP_AD_REMOVED (9):
                The user-specified ad status is removed.
                Contributes to AdGroupAdPrimaryStatus.REMOVED.
            AD_GROUP_AD_DISAPPROVED (10):
                The ad is disapproved. Contributes to
                AdGroupAdPrimaryStatus.NOT_ELIGIBLE.
            AD_GROUP_AD_UNDER_REVIEW (11):
                The ad is under review. Contributes to
                AdGroupAdPrimaryStatus.PENDING.
            AD_GROUP_AD_POOR_QUALITY (12):
                The ad is poor quality. This is determined by
                the serving stack that served the Ad.
                Contributes to AdGroupAdPrimaryStatus.LIMITED.
            AD_GROUP_AD_NO_ADS (13):
                No eligible ads instances could be generated.
                Contributes to AdGroupAdPrimaryStatus.PENDING.
            AD_GROUP_AD_APPROVED_LABELED (14):
                The ad is internally labeled with a limiting
                label. Contributes to
                AdGroupAdPrimaryStatus.LIMITED.
            AD_GROUP_AD_AREA_OF_INTEREST_ONLY (15):
                The ad is only serving in the user-specified
                area of interest. Contributes to
                AdGroupAdPrimaryStatus.LIMITED.
            AD_GROUP_AD_UNDER_APPEAL (16):
                The ad is part of an ongoing appeal. This
                reason does not impact AdGroupAdPrimaryStatus.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        CAMPAIGN_REMOVED = 2
        CAMPAIGN_PAUSED = 3
        CAMPAIGN_PENDING = 4
        CAMPAIGN_ENDED = 5
        AD_GROUP_PAUSED = 6
        AD_GROUP_REMOVED = 7
        AD_GROUP_AD_PAUSED = 8
        AD_GROUP_AD_REMOVED = 9
        AD_GROUP_AD_DISAPPROVED = 10
        AD_GROUP_AD_UNDER_REVIEW = 11
        AD_GROUP_AD_POOR_QUALITY = 12
        AD_GROUP_AD_NO_ADS = 13
        AD_GROUP_AD_APPROVED_LABELED = 14
        AD_GROUP_AD_AREA_OF_INTEREST_ONLY = 15
        AD_GROUP_AD_UNDER_APPEAL = 16


__all__ = tuple(sorted(__protobuf__.manifest))
