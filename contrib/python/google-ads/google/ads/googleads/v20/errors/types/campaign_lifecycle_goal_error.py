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
        "CampaignLifecycleGoalErrorEnum",
    },
)


class CampaignLifecycleGoalErrorEnum(proto.Message):
    r"""Container for enum describing possible campaign lifecycle
    goal errors.

    """

    class CampaignLifecycleGoalError(proto.Enum):
        r"""Enum describing possible campaign lifecycle goal errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            CAMPAIGN_MISSING (2):
                Campaign is not specified.
            INVALID_CAMPAIGN (3):
                Cannot find the specified campaign.
            CUSTOMER_ACQUISITION_INVALID_OPTIMIZATION_MODE (4):
                Optimization mode is unspecified or invalid.
            INCOMPATIBLE_BIDDING_STRATEGY (5):
                The configured lifecycle goal setting is not compatible with
                the bidding strategy the campaign is using. Specifically,
                BID_HIGHER_FOR_NEW_CUSTOMER requires conversion-value based
                bidding strategy type such as MAXIMIZE_CONVERSION_VALUE.
            MISSING_PURCHASE_GOAL (6):
                Lifecycle goals require the campaign to
                optimize towards purchase conversion goal.
            CUSTOMER_ACQUISITION_INVALID_HIGH_LIFETIME_VALUE (7):
                CampaignLifecycleGoal.customer_acquisition_goal_settings.value_settings.high_lifetime_value
                is invalid or not allowed, such as when the specified value
                is smaller than 0.01, when the optimization mode is not
                BID_HIGHER_FOR_NEW_CUSTOMER, or when
                CampaignLifecycleGoal.customer_acquisition_goal_settings.value_settings.high_lifetime_value
                is specified smaller than/without
                CampaignLifecycleGoal.customer_acquisition_goal_settings.value_settings.value.
            CUSTOMER_ACQUISITION_UNSUPPORTED_CAMPAIGN_TYPE (8):
                Customer acquisition goal is not supported on
                this campaign type.
            CUSTOMER_ACQUISITION_INVALID_VALUE (9):
                CampaignLifecycleGoal.customer_acquisition_goal_settings.value_settings.value
                is invalid or not allowed, such as when the specified value
                is smaller than 0.01, or when the optimization mode is not
                BID_HIGHER_FOR_NEW_CUSTOMER.
            CUSTOMER_ACQUISITION_VALUE_MISSING (10):
                To use BID_HIGHER_FOR_NEW_CUSTOMER mode, either
                CampaignLifecycleGoal.customer_acquisition_goal_settings.value_settings.value
                or
                CustomerLifecycleGoal.customer_acquisition_goal_value_settings.value
                must have been specified. If a manager account is managing
                your account's conversion tracking, then only the
                CustomerLifecycleGoal of that manager account is used.
            CUSTOMER_ACQUISITION_MISSING_EXISTING_CUSTOMER_DEFINITION (11):
                In order for a campaign to adopt the customer acquisition
                goal,
                CustomerLifecycleGoal.lifecycle_goal_customer_definition_settings.existing_user_lists
                must include active and accessible userlist with more than
                1000 members in the Search/Youtube network. If a manager
                account is managing your account's conversion tracking, then
                only the CustomerLifecycleGoal of that manager account is
                used. Also make sure that the manager account shares
                audience segments with sub-accounts with continuous audience
                sharing.
            CUSTOMER_ACQUISITION_MISSING_HIGH_VALUE_CUSTOMER_DEFINITION (12):
                In order for a campaign to adopt the customer acquisition
                goal with high lifetime value optimization,
                CustomerLifecycleGoal.lifecycle_goal_customer_definition_settings.high_lifetime_value_user_lists
                must include active and accessible userlist with more than
                1000 members in the Search/Youtube network. If a manager
                account is managing your account's conversion tracking, then
                only the CustomerLifecycleGoal of that manager account is
                used. Also make sure that the manager account shares
                audience segments with sub-accounts using continuous
                audience sharing.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        CAMPAIGN_MISSING = 2
        INVALID_CAMPAIGN = 3
        CUSTOMER_ACQUISITION_INVALID_OPTIMIZATION_MODE = 4
        INCOMPATIBLE_BIDDING_STRATEGY = 5
        MISSING_PURCHASE_GOAL = 6
        CUSTOMER_ACQUISITION_INVALID_HIGH_LIFETIME_VALUE = 7
        CUSTOMER_ACQUISITION_UNSUPPORTED_CAMPAIGN_TYPE = 8
        CUSTOMER_ACQUISITION_INVALID_VALUE = 9
        CUSTOMER_ACQUISITION_VALUE_MISSING = 10
        CUSTOMER_ACQUISITION_MISSING_EXISTING_CUSTOMER_DEFINITION = 11
        CUSTOMER_ACQUISITION_MISSING_HIGH_VALUE_CUSTOMER_DEFINITION = 12


__all__ = tuple(sorted(__protobuf__.manifest))
