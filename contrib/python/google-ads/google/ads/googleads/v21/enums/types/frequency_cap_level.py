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
        "FrequencyCapLevelEnum",
    },
)


class FrequencyCapLevelEnum(proto.Message):
    r"""Container for enum describing the level on which the cap is
    to be applied.

    """

    class FrequencyCapLevel(proto.Enum):
        r"""The level on which the cap is to be applied (e.g ad group ad,
        ad group). Cap is applied to all the resources of this level.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            AD_GROUP_AD (2):
                The cap is applied at the ad group ad level.
            AD_GROUP (3):
                The cap is applied at the ad group level.
            CAMPAIGN (4):
                The cap is applied at the campaign level.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        AD_GROUP_AD = 2
        AD_GROUP = 3
        CAMPAIGN = 4


__all__ = tuple(sorted(__protobuf__.manifest))
