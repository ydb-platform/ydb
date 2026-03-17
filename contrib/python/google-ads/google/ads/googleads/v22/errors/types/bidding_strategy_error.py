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
        "BiddingStrategyErrorEnum",
    },
)


class BiddingStrategyErrorEnum(proto.Message):
    r"""Container for enum describing possible bidding strategy
    errors.

    """

    class BiddingStrategyError(proto.Enum):
        r"""Enum describing possible bidding strategy errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            DUPLICATE_NAME (2):
                Each bidding strategy must have a unique
                name.
            CANNOT_CHANGE_BIDDING_STRATEGY_TYPE (3):
                Bidding strategy type is immutable.
            CANNOT_REMOVE_ASSOCIATED_STRATEGY (4):
                Only bidding strategies not linked to
                campaigns, adgroups or adgroup criteria can be
                removed.
            BIDDING_STRATEGY_NOT_SUPPORTED (5):
                The specified bidding strategy is not
                supported.
            INCOMPATIBLE_BIDDING_STRATEGY_AND_BIDDING_STRATEGY_GOAL_TYPE (6):
                The bidding strategy is incompatible with the
                campaign's bidding strategy goal type.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        DUPLICATE_NAME = 2
        CANNOT_CHANGE_BIDDING_STRATEGY_TYPE = 3
        CANNOT_REMOVE_ASSOCIATED_STRATEGY = 4
        BIDDING_STRATEGY_NOT_SUPPORTED = 5
        INCOMPATIBLE_BIDDING_STRATEGY_AND_BIDDING_STRATEGY_GOAL_TYPE = 6


__all__ = tuple(sorted(__protobuf__.manifest))
