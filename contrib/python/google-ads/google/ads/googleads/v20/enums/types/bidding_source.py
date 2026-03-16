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
        "BiddingSourceEnum",
    },
)


class BiddingSourceEnum(proto.Message):
    r"""Container for enum describing possible bidding sources."""

    class BiddingSource(proto.Enum):
        r"""Indicates where a bid or target is defined. For example, an
        ad group criterion may define a cpc bid directly, or it can
        inherit its cpc bid from the ad group.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            CAMPAIGN_BIDDING_STRATEGY (5):
                Effective bid or target is inherited from
                campaign bidding strategy.
            AD_GROUP (6):
                The bid or target is defined on the ad group.
            AD_GROUP_CRITERION (7):
                The bid or target is defined on the ad group
                criterion.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        CAMPAIGN_BIDDING_STRATEGY = 5
        AD_GROUP = 6
        AD_GROUP_CRITERION = 7


__all__ = tuple(sorted(__protobuf__.manifest))
