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
        "DemandGenChannelStrategyEnum",
    },
)


class DemandGenChannelStrategyEnum(proto.Message):
    r"""Container for the channel strategy enum."""

    class DemandGenChannelStrategy(proto.Enum):
        r"""The channel strategy defines a general grouping of channels
        to enable in the Demand Gen channel controls.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                The value is unknown in this version.
            ALL_CHANNELS (2):
                All channels are enabled.
            ALL_OWNED_AND_OPERATED_CHANNELS (3):
                All Google-owned and operated channels are
                enabled; third-party channels (e.g., Display)
                are disabled.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        ALL_CHANNELS = 2
        ALL_OWNED_AND_OPERATED_CHANNELS = 3


__all__ = tuple(sorted(__protobuf__.manifest))
