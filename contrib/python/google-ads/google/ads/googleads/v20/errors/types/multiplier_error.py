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
        "MultiplierErrorEnum",
    },
)


class MultiplierErrorEnum(proto.Message):
    r"""Container for enum describing possible multiplier errors."""

    class MultiplierError(proto.Enum):
        r"""Enum describing possible multiplier errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            MULTIPLIER_TOO_HIGH (2):
                Multiplier value is too high
            MULTIPLIER_TOO_LOW (3):
                Multiplier value is too low
            TOO_MANY_FRACTIONAL_DIGITS (4):
                Too many fractional digits
            MULTIPLIER_NOT_ALLOWED_FOR_BIDDING_STRATEGY (5):
                A multiplier cannot be set for this bidding
                strategy
            MULTIPLIER_NOT_ALLOWED_WHEN_BASE_BID_IS_MISSING (6):
                A multiplier cannot be set when there is no
                base bid (for example, content max cpc)
            NO_MULTIPLIER_SPECIFIED (7):
                A bid multiplier must be specified
            MULTIPLIER_CAUSES_BID_TO_EXCEED_DAILY_BUDGET (8):
                Multiplier causes bid to exceed daily budget
            MULTIPLIER_CAUSES_BID_TO_EXCEED_MONTHLY_BUDGET (9):
                Multiplier causes bid to exceed monthly
                budget
            MULTIPLIER_CAUSES_BID_TO_EXCEED_CUSTOM_BUDGET (10):
                Multiplier causes bid to exceed custom budget
            MULTIPLIER_CAUSES_BID_TO_EXCEED_MAX_ALLOWED_BID (11):
                Multiplier causes bid to exceed maximum
                allowed bid
            BID_LESS_THAN_MIN_ALLOWED_BID_WITH_MULTIPLIER (12):
                Multiplier causes bid to become less than the
                minimum bid allowed
            MULTIPLIER_AND_BIDDING_STRATEGY_TYPE_MISMATCH (13):
                Multiplier type (cpc versus cpm) needs to
                match campaign's bidding strategy
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        MULTIPLIER_TOO_HIGH = 2
        MULTIPLIER_TOO_LOW = 3
        TOO_MANY_FRACTIONAL_DIGITS = 4
        MULTIPLIER_NOT_ALLOWED_FOR_BIDDING_STRATEGY = 5
        MULTIPLIER_NOT_ALLOWED_WHEN_BASE_BID_IS_MISSING = 6
        NO_MULTIPLIER_SPECIFIED = 7
        MULTIPLIER_CAUSES_BID_TO_EXCEED_DAILY_BUDGET = 8
        MULTIPLIER_CAUSES_BID_TO_EXCEED_MONTHLY_BUDGET = 9
        MULTIPLIER_CAUSES_BID_TO_EXCEED_CUSTOM_BUDGET = 10
        MULTIPLIER_CAUSES_BID_TO_EXCEED_MAX_ALLOWED_BID = 11
        BID_LESS_THAN_MIN_ALLOWED_BID_WITH_MULTIPLIER = 12
        MULTIPLIER_AND_BIDDING_STRATEGY_TYPE_MISMATCH = 13


__all__ = tuple(sorted(__protobuf__.manifest))
