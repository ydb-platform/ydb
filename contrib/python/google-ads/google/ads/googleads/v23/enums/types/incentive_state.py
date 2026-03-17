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
    package="google.ads.googleads.v23.enums",
    marshal="google.ads.googleads.v23",
    manifest={
        "IncentiveStateEnum",
    },
)


class IncentiveStateEnum(proto.Message):
    r"""Container for the incentive state enum."""

    class IncentiveState(proto.Enum):
        r"""The possible states of a redeemed incentive.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            REDEEMED (2):
                The incentive has been redeemed but the
                requirements are not yet met.
            FULFILLED (3):
                The incentive's requirements have been met
                but the reward has not yet been granted.
            REWARD_GRANTED (4):
                The reward for the incentive has been
                granted.
            EXPIRED (5):
                The incentive expired before the requirements
                were met.
            REWARD_EXPIRED (6):
                The granted reward has expired.
            INVALIDATED (7):
                The incentive was marked as invalid after
                redemption.
            REWARD_EXHAUSTED (8):
                The granted reward has been fully used.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        REDEEMED = 2
        FULFILLED = 3
        REWARD_GRANTED = 4
        EXPIRED = 5
        REWARD_EXPIRED = 6
        INVALIDATED = 7
        REWARD_EXHAUSTED = 8


__all__ = tuple(sorted(__protobuf__.manifest))
