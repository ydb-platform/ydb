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
        "AdGroupCriterionPrimaryStatusReasonEnum",
    },
)


class AdGroupCriterionPrimaryStatusReasonEnum(proto.Message):
    r"""Container for enum describing possible ad group criterion
    primary status reasons.

    """

    class AdGroupCriterionPrimaryStatusReason(proto.Enum):
        r"""Enum describing the possible Ad Group Criterion primary
        status reasons. Provides insight into why an Ad Group Criterion
        is not serving or not serving optimally. These reasons are
        aggregated to determine an overall Ad Group Criterion primary
        status.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents
                unknown value in this version.
            CAMPAIGN_PENDING (2):
                The user-specified time for this campaign to
                start is in the future. Contributes to
                AdGroupCriterionPrimaryStatus.PENDING.
            CAMPAIGN_CRITERION_NEGATIVE (3):
                The ad group criterion is overridden by negative campaign
                criterion. Contributes to
                AdGroupCriterionPrimaryStatus.NOT_ELIGIBLE.
            CAMPAIGN_PAUSED (4):
                The user-specified campaign status is paused.
                Contributes to
                AdGroupCriterionPrimaryStatus.PAUSED.
            CAMPAIGN_REMOVED (5):
                The user-specified campaign status is
                removed. Contributes to
                AdGroupCriterionPrimaryStatus.REMOVED.
            CAMPAIGN_ENDED (6):
                The user-specified time for this campaign to
                end has passed. Contributes to
                AdGroupCriterionPrimaryStatus.ENDED.
            AD_GROUP_PAUSED (7):
                The user-specified ad group status is paused.
                Contributes to
                AdGroupCriterionPrimaryStatus.PAUSED.
            AD_GROUP_REMOVED (8):
                The user-specified ad group status is
                removed. Contributes to
                AdGroupCriterionPrimaryStatus.REMOVED.
            AD_GROUP_CRITERION_DISAPPROVED (9):
                The ad group criterion is disapproved by the ads approval
                system. Contributes to
                AdGroupCriterionPrimaryStatus.NOT_ELIGIBLE.
            AD_GROUP_CRITERION_RARELY_SERVED (10):
                The ad group criterion is rarely served. Contributes to
                AdGroupCriterionPrimaryStatus.NOT_ELIGIBLE.
            AD_GROUP_CRITERION_LOW_QUALITY (11):
                The ad group criterion has a low quality
                score. Contributes to
                AdGroupCriterionPrimaryStatus.LIMITED.
            AD_GROUP_CRITERION_UNDER_REVIEW (12):
                The ad group criterion is under review.
                Contributes to
                AdGroupCriterionPrimaryStatus.PENDING.
            AD_GROUP_CRITERION_PENDING_REVIEW (13):
                The ad group criterion is pending review. Contributes to
                AdGroupCriterionPrimaryStatus.NOT_ELIGIBLE.
            AD_GROUP_CRITERION_BELOW_FIRST_PAGE_BID (14):
                The ad group criterion's bid is below the
                value necessary to serve on the first page.
                Contributes to
                AdGroupCriterionPrimaryStatus.LIMITED.
            AD_GROUP_CRITERION_NEGATIVE (15):
                The ad group criterion is negative. Contributes to
                AdGroupCriterionPrimaryStatus.NOT_ELIGIBLE.
            AD_GROUP_CRITERION_RESTRICTED (16):
                The ad group criterion is restricted. Contributes to
                AdGroupCriterionPrimaryStatus.NOT_ELIGIBLE.
            AD_GROUP_CRITERION_PAUSED (17):
                The user-specified ad group criterion status
                is paused. Contributes to
                AdGroupCriterionPrimaryStatus.PAUSED.
            AD_GROUP_CRITERION_PAUSED_DUE_TO_LOW_ACTIVITY (18):
                The ad group criterion has been paused due to
                prolonged low activity in serving. Contributes
                to AdGroupCriterionPrimaryStatus.PAUSED.
            AD_GROUP_CRITERION_REMOVED (19):
                The user-specified ad group criterion status
                is removed. Contributes to
                AdGroupCriterionPrimaryStatus.REMOVED.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        CAMPAIGN_PENDING = 2
        CAMPAIGN_CRITERION_NEGATIVE = 3
        CAMPAIGN_PAUSED = 4
        CAMPAIGN_REMOVED = 5
        CAMPAIGN_ENDED = 6
        AD_GROUP_PAUSED = 7
        AD_GROUP_REMOVED = 8
        AD_GROUP_CRITERION_DISAPPROVED = 9
        AD_GROUP_CRITERION_RARELY_SERVED = 10
        AD_GROUP_CRITERION_LOW_QUALITY = 11
        AD_GROUP_CRITERION_UNDER_REVIEW = 12
        AD_GROUP_CRITERION_PENDING_REVIEW = 13
        AD_GROUP_CRITERION_BELOW_FIRST_PAGE_BID = 14
        AD_GROUP_CRITERION_NEGATIVE = 15
        AD_GROUP_CRITERION_RESTRICTED = 16
        AD_GROUP_CRITERION_PAUSED = 17
        AD_GROUP_CRITERION_PAUSED_DUE_TO_LOW_ACTIVITY = 18
        AD_GROUP_CRITERION_REMOVED = 19


__all__ = tuple(sorted(__protobuf__.manifest))
