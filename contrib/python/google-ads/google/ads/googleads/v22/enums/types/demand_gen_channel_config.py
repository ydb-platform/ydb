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
        "DemandGenChannelConfigEnum",
    },
)


class DemandGenChannelConfigEnum(proto.Message):
    r"""Container for the channel config enum."""

    class DemandGenChannelConfig(proto.Enum):
        r"""This value indicates which field within the 'oneof' group
        (where only one option can be active) is used in the channel
        controls for a Demand Gen ad group.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                The value is unknown in this version.
            CHANNEL_STRATEGY (2):
                The channel controls configuration uses a
                general channel strategy; individual channels
                are not configured separately.
            SELECTED_CHANNELS (3):
                The channel controls configuration explicitly
                defines the selected channels.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        CHANNEL_STRATEGY = 2
        SELECTED_CHANNELS = 3


__all__ = tuple(sorted(__protobuf__.manifest))
