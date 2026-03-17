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
        "BiddingErrorEnum",
    },
)


class BiddingErrorEnum(proto.Message):
    r"""Container for enum describing possible bidding errors."""

    class BiddingError(proto.Enum):
        r"""Enum describing possible bidding errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            BIDDING_STRATEGY_TRANSITION_NOT_ALLOWED (2):
                Cannot transition to new bidding strategy.
            CANNOT_ATTACH_BIDDING_STRATEGY_TO_CAMPAIGN (7):
                Cannot attach bidding strategy to campaign.
            INVALID_ANONYMOUS_BIDDING_STRATEGY_TYPE (10):
                Bidding strategy is not supported or cannot
                be used as anonymous.
            INVALID_BIDDING_STRATEGY_TYPE (14):
                The type does not match the named strategy's
                type.
            INVALID_BID (17):
                The bid is invalid.
            BIDDING_STRATEGY_NOT_AVAILABLE_FOR_ACCOUNT_TYPE (18):
                Bidding strategy is not available for the
                account type.
            CANNOT_CREATE_CAMPAIGN_WITH_BIDDING_STRATEGY (21):
                Campaign can not be created with given
                bidding strategy. It can be transitioned to the
                strategy, once eligible.
            CANNOT_TARGET_CONTENT_NETWORK_ONLY_WITH_CAMPAIGN_LEVEL_POP_BIDDING_STRATEGY (23):
                Cannot target content network only as
                campaign uses Page One Promoted bidding
                strategy.
            BIDDING_STRATEGY_NOT_SUPPORTED_WITH_AD_SCHEDULE (24):
                Budget Optimizer and Target Spend bidding
                strategies are not supported for campaigns with
                AdSchedule targeting.
            PAY_PER_CONVERSION_NOT_AVAILABLE_FOR_CUSTOMER (25):
                Pay per conversion is not available to all
                the customer, only few customers on the
                allow-list can use this.
            PAY_PER_CONVERSION_NOT_ALLOWED_WITH_TARGET_CPA (26):
                Pay per conversion is not allowed with Target
                CPA.
            BIDDING_STRATEGY_NOT_ALLOWED_FOR_SEARCH_ONLY_CAMPAIGNS (27):
                Cannot set bidding strategy to Manual CPM for
                search network only campaigns.
            BIDDING_STRATEGY_NOT_SUPPORTED_IN_DRAFTS_OR_EXPERIMENTS (28):
                The bidding strategy is not supported for use
                in drafts or experiments.
            BIDDING_STRATEGY_TYPE_DOES_NOT_SUPPORT_PRODUCT_TYPE_ADGROUP_CRITERION (29):
                Bidding strategy type does not support
                product type ad group criterion.
            BID_TOO_SMALL (30):
                Bid amount is too small.
            BID_TOO_BIG (31):
                Bid amount is too big.
            BID_TOO_MANY_FRACTIONAL_DIGITS (32):
                Bid has too many fractional digit precision.
            INVALID_DOMAIN_NAME (33):
                Invalid domain name specified.
            NOT_COMPATIBLE_WITH_PAYMENT_MODE (34):
                The field is not compatible with the payment
                mode.
            BIDDING_STRATEGY_TYPE_INCOMPATIBLE_WITH_SHARED_BUDGET (37):
                Bidding strategy type is incompatible with
                shared budget.
            BIDDING_STRATEGY_AND_BUDGET_MUST_BE_ALIGNED (38):
                The attached bidding strategy and budget must
                be aligned with each other if alignment is
                specified on either entity.
            BIDDING_STRATEGY_AND_BUDGET_MUST_BE_ATTACHED_TO_THE_SAME_CAMPAIGNS_TO_ALIGN (39):
                The attached bidding strategy and budget must
                be attached to the same campaigns to become
                aligned.
            BIDDING_STRATEGY_AND_BUDGET_MUST_BE_REMOVED_TOGETHER (40):
                The aligned bidding strategy and budget must
                be removed at the same time.
            CPC_BID_FLOOR_MICROS_GREATER_THAN_CPC_BID_CEILING_MICROS (41):
                cpc_bid_floor_micros is greater than cpc_bid_ceiling_micros.
            TARGET_ROAS_TOLERANCE_PERCENT_MILLIS_MUST_BE_INTEGER (42):
                target_roas_tolerance_percent_millis must be integer.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        BIDDING_STRATEGY_TRANSITION_NOT_ALLOWED = 2
        CANNOT_ATTACH_BIDDING_STRATEGY_TO_CAMPAIGN = 7
        INVALID_ANONYMOUS_BIDDING_STRATEGY_TYPE = 10
        INVALID_BIDDING_STRATEGY_TYPE = 14
        INVALID_BID = 17
        BIDDING_STRATEGY_NOT_AVAILABLE_FOR_ACCOUNT_TYPE = 18
        CANNOT_CREATE_CAMPAIGN_WITH_BIDDING_STRATEGY = 21
        CANNOT_TARGET_CONTENT_NETWORK_ONLY_WITH_CAMPAIGN_LEVEL_POP_BIDDING_STRATEGY = (
            23
        )
        BIDDING_STRATEGY_NOT_SUPPORTED_WITH_AD_SCHEDULE = 24
        PAY_PER_CONVERSION_NOT_AVAILABLE_FOR_CUSTOMER = 25
        PAY_PER_CONVERSION_NOT_ALLOWED_WITH_TARGET_CPA = 26
        BIDDING_STRATEGY_NOT_ALLOWED_FOR_SEARCH_ONLY_CAMPAIGNS = 27
        BIDDING_STRATEGY_NOT_SUPPORTED_IN_DRAFTS_OR_EXPERIMENTS = 28
        BIDDING_STRATEGY_TYPE_DOES_NOT_SUPPORT_PRODUCT_TYPE_ADGROUP_CRITERION = (
            29
        )
        BID_TOO_SMALL = 30
        BID_TOO_BIG = 31
        BID_TOO_MANY_FRACTIONAL_DIGITS = 32
        INVALID_DOMAIN_NAME = 33
        NOT_COMPATIBLE_WITH_PAYMENT_MODE = 34
        BIDDING_STRATEGY_TYPE_INCOMPATIBLE_WITH_SHARED_BUDGET = 37
        BIDDING_STRATEGY_AND_BUDGET_MUST_BE_ALIGNED = 38
        BIDDING_STRATEGY_AND_BUDGET_MUST_BE_ATTACHED_TO_THE_SAME_CAMPAIGNS_TO_ALIGN = (
            39
        )
        BIDDING_STRATEGY_AND_BUDGET_MUST_BE_REMOVED_TOGETHER = 40
        CPC_BID_FLOOR_MICROS_GREATER_THAN_CPC_BID_CEILING_MICROS = 41
        TARGET_ROAS_TOLERANCE_PERCENT_MILLIS_MUST_BE_INTEGER = 42


__all__ = tuple(sorted(__protobuf__.manifest))
