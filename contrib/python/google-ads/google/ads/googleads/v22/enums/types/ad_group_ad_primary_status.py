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
        "AdGroupAdPrimaryStatusEnum",
    },
)


class AdGroupAdPrimaryStatusEnum(proto.Message):
    r"""Ad Group Ad Primary Status. Provides insight into why an ad
    group ad is not serving or not serving optimally.

    """

    class AdGroupAdPrimaryStatus(proto.Enum):
        r"""The possible statuses of an AdGroupAd.

        Values:
            UNSPECIFIED (0):
                No value has been specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            ELIGIBLE (2):
                The ad group ad is eligible to serve.
            PAUSED (3):
                The ad group ad is paused.
            REMOVED (4):
                The ad group ad is removed.
            PENDING (5):
                The ad cannot serve now, but may serve later
                without advertiser action.
            LIMITED (6):
                The ad group ad is serving in a limited
                capacity.
            NOT_ELIGIBLE (7):
                The ad group ad is not eligible to serve.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        ELIGIBLE = 2
        PAUSED = 3
        REMOVED = 4
        PENDING = 5
        LIMITED = 6
        NOT_ELIGIBLE = 7


__all__ = tuple(sorted(__protobuf__.manifest))
