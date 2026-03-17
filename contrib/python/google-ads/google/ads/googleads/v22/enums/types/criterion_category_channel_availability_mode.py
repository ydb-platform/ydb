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
        "CriterionCategoryChannelAvailabilityModeEnum",
    },
)


class CriterionCategoryChannelAvailabilityModeEnum(proto.Message):
    r"""Describes channel availability mode for a criterion
    availability - whether the availability is meant to include all
    advertising channels, or a particular channel with all its
    channel subtypes, or a channel with a certain subset of channel
    subtypes.

    """

    class CriterionCategoryChannelAvailabilityMode(proto.Enum):
        r"""Enum containing the possible
        CriterionCategoryChannelAvailabilityMode.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            ALL_CHANNELS (2):
                The category is available to campaigns of all
                channel types and subtypes.
            CHANNEL_TYPE_AND_ALL_SUBTYPES (3):
                The category is available to campaigns of a
                specific channel type, including all subtypes
                under it.
            CHANNEL_TYPE_AND_SUBSET_SUBTYPES (4):
                The category is available to campaigns of a
                specific channel type and subtype(s).
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        ALL_CHANNELS = 2
        CHANNEL_TYPE_AND_ALL_SUBTYPES = 3
        CHANNEL_TYPE_AND_SUBSET_SUBTYPES = 4


__all__ = tuple(sorted(__protobuf__.manifest))
