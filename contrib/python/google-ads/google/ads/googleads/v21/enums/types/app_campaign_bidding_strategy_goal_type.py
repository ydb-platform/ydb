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
        "AppCampaignBiddingStrategyGoalTypeEnum",
    },
)


class AppCampaignBiddingStrategyGoalTypeEnum(proto.Message):
    r"""Container for enum describing goal towards which the bidding
    strategy of an app campaign should optimize for.

    """

    class AppCampaignBiddingStrategyGoalType(proto.Enum):
        r"""Goal type of App campaign BiddingStrategy.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            OPTIMIZE_INSTALLS_TARGET_INSTALL_COST (2):
                Aim to maximize the number of app installs.
                The cpa bid is the target cost per install.
            OPTIMIZE_IN_APP_CONVERSIONS_TARGET_INSTALL_COST (3):
                Aim to maximize the long term number of
                selected in-app conversions from app installs.
                The cpa bid is the target cost per install.
            OPTIMIZE_IN_APP_CONVERSIONS_TARGET_CONVERSION_COST (4):
                Aim to maximize the long term number of
                selected in-app conversions from app installs.
                The cpa bid is the target cost per in-app
                conversion. Note that the actual cpa may seem
                higher than the target cpa at first, since the
                long term conversions haven't happened yet.
            OPTIMIZE_RETURN_ON_ADVERTISING_SPEND (5):
                Aim to maximize all conversions' value, for
                example, install + selected in-app conversions
                while achieving or exceeding target return on
                advertising spend.
            OPTIMIZE_PRE_REGISTRATION_CONVERSION_VOLUME (6):
                Aim to maximize the pre-registration of the
                app.
            OPTIMIZE_INSTALLS_WITHOUT_TARGET_INSTALL_COST (7):
                Aim to maximize installation of the app
                without target cost-per-install.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        OPTIMIZE_INSTALLS_TARGET_INSTALL_COST = 2
        OPTIMIZE_IN_APP_CONVERSIONS_TARGET_INSTALL_COST = 3
        OPTIMIZE_IN_APP_CONVERSIONS_TARGET_CONVERSION_COST = 4
        OPTIMIZE_RETURN_ON_ADVERTISING_SPEND = 5
        OPTIMIZE_PRE_REGISTRATION_CONVERSION_VOLUME = 6
        OPTIMIZE_INSTALLS_WITHOUT_TARGET_INSTALL_COST = 7


__all__ = tuple(sorted(__protobuf__.manifest))
