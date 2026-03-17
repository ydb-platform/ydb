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
        "BiddingStrategySystemStatusEnum",
    },
)


class BiddingStrategySystemStatusEnum(proto.Message):
    r"""Message describing BiddingStrategy system statuses."""

    class BiddingStrategySystemStatus(proto.Enum):
        r"""The possible system statuses of a BiddingStrategy.

        Values:
            UNSPECIFIED (0):
                Signals that an unexpected error occurred,
                for example, no bidding strategy type was found,
                or no status information was found.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            ENABLED (2):
                The bid strategy is active, and AdWords
                cannot find any specific issues with the
                strategy.
            LEARNING_NEW (3):
                The bid strategy is learning because it has
                been recently created or recently reactivated.
            LEARNING_SETTING_CHANGE (4):
                The bid strategy is learning because of a
                recent setting change.
            LEARNING_BUDGET_CHANGE (5):
                The bid strategy is learning because of a
                recent budget change.
            LEARNING_COMPOSITION_CHANGE (6):
                The bid strategy is learning because of
                recent change in number of campaigns, ad groups
                or keywords attached to it.
            LEARNING_CONVERSION_TYPE_CHANGE (7):
                The bid strategy depends on conversion
                reporting and the customer recently modified
                conversion types that were relevant to the bid
                strategy.
            LEARNING_CONVERSION_SETTING_CHANGE (8):
                The bid strategy depends on conversion
                reporting and the customer recently changed
                their conversion settings.
            LIMITED_BY_CPC_BID_CEILING (9):
                The bid strategy is limited by its bid
                ceiling.
            LIMITED_BY_CPC_BID_FLOOR (10):
                The bid strategy is limited by its bid floor.
            LIMITED_BY_DATA (11):
                The bid strategy is limited because there was
                not enough conversion traffic over the past
                weeks.
            LIMITED_BY_BUDGET (12):
                A significant fraction of keywords in this
                bid strategy are limited by budget.
            LIMITED_BY_LOW_PRIORITY_SPEND (13):
                The bid strategy cannot reach its target
                spend because its spend has been de-prioritized.
            LIMITED_BY_LOW_QUALITY (14):
                A significant fraction of keywords in this
                bid strategy have a low Quality Score.
            LIMITED_BY_INVENTORY (15):
                The bid strategy cannot fully spend its
                budget because of narrow targeting.
            MISCONFIGURED_ZERO_ELIGIBILITY (16):
                Missing conversion tracking (no pings
                present) and/or remarketing lists for SSC.
            MISCONFIGURED_CONVERSION_TYPES (17):
                The bid strategy depends on conversion
                reporting and the customer is lacking conversion
                types that might be reported against this
                strategy.
            MISCONFIGURED_CONVERSION_SETTINGS (18):
                The bid strategy depends on conversion
                reporting and the customer's conversion settings
                are misconfigured.
            MISCONFIGURED_SHARED_BUDGET (19):
                There are campaigns outside the bid strategy
                that share budgets with campaigns included in
                the strategy.
            MISCONFIGURED_STRATEGY_TYPE (20):
                The campaign has an invalid strategy type and
                is not serving.
            PAUSED (21):
                The bid strategy is not active. Either there
                are no active campaigns, ad groups or keywords
                attached to the bid strategy. Or there are no
                active budgets connected to the bid strategy.
            UNAVAILABLE (22):
                This bid strategy currently does not support
                status reporting.
            MULTIPLE_LEARNING (23):
                There were multiple LEARNING\_\* system statuses for this
                bid strategy during the time in question.
            MULTIPLE_LIMITED (24):
                There were multiple LIMITED\_\* system statuses for this bid
                strategy during the time in question.
            MULTIPLE_MISCONFIGURED (25):
                There were multiple MISCONFIGURED\_\* system statuses for
                this bid strategy during the time in question.
            MULTIPLE (26):
                There were multiple system statuses for this
                bid strategy during the time in question.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        ENABLED = 2
        LEARNING_NEW = 3
        LEARNING_SETTING_CHANGE = 4
        LEARNING_BUDGET_CHANGE = 5
        LEARNING_COMPOSITION_CHANGE = 6
        LEARNING_CONVERSION_TYPE_CHANGE = 7
        LEARNING_CONVERSION_SETTING_CHANGE = 8
        LIMITED_BY_CPC_BID_CEILING = 9
        LIMITED_BY_CPC_BID_FLOOR = 10
        LIMITED_BY_DATA = 11
        LIMITED_BY_BUDGET = 12
        LIMITED_BY_LOW_PRIORITY_SPEND = 13
        LIMITED_BY_LOW_QUALITY = 14
        LIMITED_BY_INVENTORY = 15
        MISCONFIGURED_ZERO_ELIGIBILITY = 16
        MISCONFIGURED_CONVERSION_TYPES = 17
        MISCONFIGURED_CONVERSION_SETTINGS = 18
        MISCONFIGURED_SHARED_BUDGET = 19
        MISCONFIGURED_STRATEGY_TYPE = 20
        PAUSED = 21
        UNAVAILABLE = 22
        MULTIPLE_LEARNING = 23
        MULTIPLE_LIMITED = 24
        MULTIPLE_MISCONFIGURED = 25
        MULTIPLE = 26


__all__ = tuple(sorted(__protobuf__.manifest))
