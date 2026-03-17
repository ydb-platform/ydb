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
        "CampaignBudgetErrorEnum",
    },
)


class CampaignBudgetErrorEnum(proto.Message):
    r"""Container for enum describing possible campaign budget
    errors.

    """

    class CampaignBudgetError(proto.Enum):
        r"""Enum describing possible campaign budget errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            CAMPAIGN_BUDGET_CANNOT_BE_SHARED (17):
                The campaign budget cannot be shared.
            CAMPAIGN_BUDGET_REMOVED (2):
                The requested campaign budget no longer
                exists.
            CAMPAIGN_BUDGET_IN_USE (3):
                The campaign budget is associated with at
                least one campaign, and so the campaign budget
                cannot be removed.
            CAMPAIGN_BUDGET_PERIOD_NOT_AVAILABLE (4):
                Customer is not on the allow-list for this
                campaign budget period.
            CANNOT_MODIFY_FIELD_OF_IMPLICITLY_SHARED_CAMPAIGN_BUDGET (6):
                This field is not mutable on implicitly
                shared campaign budgets
            CANNOT_UPDATE_CAMPAIGN_BUDGET_TO_IMPLICITLY_SHARED (7):
                Cannot change explicitly shared campaign
                budgets back to implicitly shared ones.
            CANNOT_UPDATE_CAMPAIGN_BUDGET_TO_EXPLICITLY_SHARED_WITHOUT_NAME (8):
                An implicit campaign budget without a name
                cannot be changed to explicitly shared campaign
                budget.
            CANNOT_UPDATE_CAMPAIGN_BUDGET_TO_EXPLICITLY_SHARED (9):
                Cannot change an implicitly shared campaign
                budget to an explicitly shared one.
            CANNOT_USE_IMPLICITLY_SHARED_CAMPAIGN_BUDGET_WITH_MULTIPLE_CAMPAIGNS (10):
                Only explicitly shared campaign budgets can
                be used with multiple campaigns.
            DUPLICATE_NAME (11):
                A campaign budget with this name already
                exists.
            MONEY_AMOUNT_IN_WRONG_CURRENCY (12):
                A money amount was not in the expected
                currency.
            MONEY_AMOUNT_LESS_THAN_CURRENCY_MINIMUM_CPC (13):
                A money amount was less than the minimum CPC
                for currency.
            MONEY_AMOUNT_TOO_LARGE (14):
                A money amount was greater than the maximum
                allowed.
            NEGATIVE_MONEY_AMOUNT (15):
                A money amount was negative.
            NON_MULTIPLE_OF_MINIMUM_CURRENCY_UNIT (16):
                A money amount was not a multiple of a
                minimum unit.
            TOTAL_BUDGET_AMOUNT_MUST_BE_UNSET_FOR_BUDGET_PERIOD_DAILY (18):
                Total budget amount must be unset when
                BudgetPeriod is DAILY.
            INVALID_PERIOD (19):
                The period of the budget is not allowed.
            CANNOT_USE_ACCELERATED_DELIVERY_MODE (20):
                Cannot use accelerated delivery method on
                this budget.
            BUDGET_AMOUNT_MUST_BE_UNSET_FOR_CUSTOM_BUDGET_PERIOD (21):
                Budget amount must be unset when BudgetPeriod
                is CUSTOM.
            BUDGET_BELOW_PER_DAY_MINIMUM (22):
                Budget amount or total amount must be above this campaign's
                per-day minimum. See the error's
                details.budget_per_day_minimum_error_details field for more
                information.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        CAMPAIGN_BUDGET_CANNOT_BE_SHARED = 17
        CAMPAIGN_BUDGET_REMOVED = 2
        CAMPAIGN_BUDGET_IN_USE = 3
        CAMPAIGN_BUDGET_PERIOD_NOT_AVAILABLE = 4
        CANNOT_MODIFY_FIELD_OF_IMPLICITLY_SHARED_CAMPAIGN_BUDGET = 6
        CANNOT_UPDATE_CAMPAIGN_BUDGET_TO_IMPLICITLY_SHARED = 7
        CANNOT_UPDATE_CAMPAIGN_BUDGET_TO_EXPLICITLY_SHARED_WITHOUT_NAME = 8
        CANNOT_UPDATE_CAMPAIGN_BUDGET_TO_EXPLICITLY_SHARED = 9
        CANNOT_USE_IMPLICITLY_SHARED_CAMPAIGN_BUDGET_WITH_MULTIPLE_CAMPAIGNS = (
            10
        )
        DUPLICATE_NAME = 11
        MONEY_AMOUNT_IN_WRONG_CURRENCY = 12
        MONEY_AMOUNT_LESS_THAN_CURRENCY_MINIMUM_CPC = 13
        MONEY_AMOUNT_TOO_LARGE = 14
        NEGATIVE_MONEY_AMOUNT = 15
        NON_MULTIPLE_OF_MINIMUM_CURRENCY_UNIT = 16
        TOTAL_BUDGET_AMOUNT_MUST_BE_UNSET_FOR_BUDGET_PERIOD_DAILY = 18
        INVALID_PERIOD = 19
        CANNOT_USE_ACCELERATED_DELIVERY_MODE = 20
        BUDGET_AMOUNT_MUST_BE_UNSET_FOR_CUSTOM_BUDGET_PERIOD = 21
        BUDGET_BELOW_PER_DAY_MINIMUM = 22


__all__ = tuple(sorted(__protobuf__.manifest))
