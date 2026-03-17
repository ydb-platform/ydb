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
        "AdGroupPrimaryStatusEnum",
    },
)


class AdGroupPrimaryStatusEnum(proto.Message):
    r"""Ad Group Primary Status. Provides insight into why an ad
    group is not serving or not serving optimally.

    """

    class AdGroupPrimaryStatus(proto.Enum):
        r"""The possible statuses of an AdGroup.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            ELIGIBLE (2):
                The ad group is eligible to serve.
            PAUSED (3):
                The ad group is paused.
            REMOVED (4):
                The ad group is removed.
            PENDING (5):
                The ad group may serve in the future.
            NOT_ELIGIBLE (6):
                The ad group is not eligible to serve.
            LIMITED (7):
                The ad group has limited servability.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        ELIGIBLE = 2
        PAUSED = 3
        REMOVED = 4
        PENDING = 5
        NOT_ELIGIBLE = 6
        LIMITED = 7


__all__ = tuple(sorted(__protobuf__.manifest))
