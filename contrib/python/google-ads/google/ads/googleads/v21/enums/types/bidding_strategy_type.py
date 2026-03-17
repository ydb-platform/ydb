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
        "BiddingStrategyTypeEnum",
    },
)


class BiddingStrategyTypeEnum(proto.Message):
    r"""Container for enum describing possible bidding strategy
    types.

    """

    class BiddingStrategyType(proto.Enum):
        r"""Enum describing possible bidding strategy types.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            COMMISSION (16):
                Commission is an automatic bidding strategy
                in which the advertiser pays a certain portion
                of the conversion value.
            ENHANCED_CPC (2):
                Enhanced CPC is a bidding strategy that
                raises bids for clicks that seem more likely to
                lead to a conversion and lowers them for clicks
                where they seem less likely.
            FIXED_CPM (19):
                Fixed CPM is a manual bidding strategy with a
                fixed CPM.
            INVALID (17):
                Used for return value only. Indicates that a
                campaign does not have a bidding strategy. This
                prevents the campaign from serving. For example,
                a campaign may be attached to a manager bidding
                strategy and the serving account is subsequently
                unlinked from the manager account. In this case
                the campaign will automatically be detached from
                the now inaccessible manager bidding strategy
                and transition to the INVALID bidding strategy
                type.
            MANUAL_CPA (18):
                Manual bidding strategy that allows
                advertiser to set the bid per
                advertiser-specified action.
            MANUAL_CPC (3):
                Manual click based bidding where user pays
                per click.
            MANUAL_CPM (4):
                Manual impression based bidding
                where user pays per thousand impressions.
            MANUAL_CPV (13):
                A bidding strategy that pays a configurable
                amount per video view.
            MAXIMIZE_CONVERSIONS (10):
                A bidding strategy that automatically
                maximizes number of conversions given a daily
                budget.
            MAXIMIZE_CONVERSION_VALUE (11):
                An automated bidding strategy that
                automatically sets bids to maximize revenue
                while spending your budget.
            PAGE_ONE_PROMOTED (5):
                Page-One Promoted bidding scheme, which sets
                max cpc bids to target impressions on page one
                or page one promoted slots on google.com. This
                enum value is deprecated.
            PERCENT_CPC (12):
                Percent Cpc is bidding strategy where bids
                are a fraction of the advertised price for some
                good or service.
            TARGET_CPA (6):
                Target CPA is an automated bid strategy that
                sets bids to help get as many conversions as
                possible at the target cost-per-acquisition
                (CPA) you set.
            TARGET_CPM (14):
                Target CPM is an automated bid strategy that
                sets bids to help get as many impressions as
                possible at the target cost per one thousand
                impressions (CPM) you set.
            TARGET_CPV (20):
                Target CPV is an automated bidding strategy
                that sets bids to optimize performance given the
                average target cost per view.
            TARGET_IMPRESSION_SHARE (15):
                An automated bidding strategy that sets bids
                so that a certain percentage of search ads are
                shown at the top of the first page (or other
                targeted location).
            TARGET_OUTRANK_SHARE (7):
                Target Outrank Share is an automated bidding
                strategy that sets bids based on the target
                fraction of auctions where the advertiser should
                outrank a specific competitor.
                This enum value is deprecated.
            TARGET_ROAS (8):
                Target ROAS is an automated bidding strategy
                that helps you maximize revenue while averaging
                a specific target Return On Average Spend
                (ROAS).
            TARGET_SPEND (9):
                Target Spend is an automated bid strategy
                that sets your bids to help get as many clicks
                as possible within your budget.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        COMMISSION = 16
        ENHANCED_CPC = 2
        FIXED_CPM = 19
        INVALID = 17
        MANUAL_CPA = 18
        MANUAL_CPC = 3
        MANUAL_CPM = 4
        MANUAL_CPV = 13
        MAXIMIZE_CONVERSIONS = 10
        MAXIMIZE_CONVERSION_VALUE = 11
        PAGE_ONE_PROMOTED = 5
        PERCENT_CPC = 12
        TARGET_CPA = 6
        TARGET_CPM = 14
        TARGET_CPV = 20
        TARGET_IMPRESSION_SHARE = 15
        TARGET_OUTRANK_SHARE = 7
        TARGET_ROAS = 8
        TARGET_SPEND = 9


__all__ = tuple(sorted(__protobuf__.manifest))
