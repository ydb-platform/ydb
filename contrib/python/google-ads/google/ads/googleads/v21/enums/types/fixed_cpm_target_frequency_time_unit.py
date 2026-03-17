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
        "FixedCpmTargetFrequencyTimeUnitEnum",
    },
)


class FixedCpmTargetFrequencyTimeUnitEnum(proto.Message):
    r"""Container for enum describing bidding goal target frequency
    time units.

    """

    class FixedCpmTargetFrequencyTimeUnit(proto.Enum):
        r"""An enum describing the time window over which the bidding is
        optimized to meet the target frequency when using Fixed CPM
        bidding strategy.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            MONTHLY (2):
                Optimize bidding to reach target frequency in
                30 days.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        MONTHLY = 2


__all__ = tuple(sorted(__protobuf__.manifest))
