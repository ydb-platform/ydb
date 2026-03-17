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
        "CampaignGoalConfigErrorEnum",
    },
)


class CampaignGoalConfigErrorEnum(proto.Message):
    r"""Container for enum describing possible campaign goal config
    errors.

    """

    class CampaignGoalConfigError(proto.Enum):
        r"""Enum describing possible campaign goal config errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            GOAL_NOT_FOUND (3):
                Goal is either removed or does not exist for
                this account.
            CAMPAIGN_NOT_FOUND (4):
                Campaign is either removed or does not exist.
            HIGH_LIFETIME_VALUE_PRESENT_BUT_VALUE_ABSENT (9):
                If high lifetime value is present then value
                should be present.
            HIGH_LIFETIME_VALUE_LESS_THAN_OR_EQUAL_TO_VALUE (10):
                High lifetime value should be greater than
                value.
            CUSTOMER_LIFECYCLE_OPTIMIZATION_CAMPAIGN_TYPE_NOT_SUPPORTED (11):
                When using customer lifecycle optimization
                goal, campaign type should be supported.
            CUSTOMER_NOT_ALLOWLISTED_FOR_RETENTION_ONLY (12):
                Customer must be allowlisted to use retention
                only goal.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        GOAL_NOT_FOUND = 3
        CAMPAIGN_NOT_FOUND = 4
        HIGH_LIFETIME_VALUE_PRESENT_BUT_VALUE_ABSENT = 9
        HIGH_LIFETIME_VALUE_LESS_THAN_OR_EQUAL_TO_VALUE = 10
        CUSTOMER_LIFECYCLE_OPTIMIZATION_CAMPAIGN_TYPE_NOT_SUPPORTED = 11
        CUSTOMER_NOT_ALLOWLISTED_FOR_RETENTION_ONLY = 12


__all__ = tuple(sorted(__protobuf__.manifest))
