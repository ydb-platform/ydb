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
        "SmartCampaignStatusEnum",
    },
)


class SmartCampaignStatusEnum(proto.Message):
    r"""A container for an enum that describes Smart campaign
    statuses.

    """

    class SmartCampaignStatus(proto.Enum):
        r"""Smart campaign statuses.

        Values:
            UNSPECIFIED (0):
                The status has not been specified.
            UNKNOWN (1):
                The received value is not known in this
                version.
                This is a response-only value.
            PAUSED (2):
                The campaign was paused.
            NOT_ELIGIBLE (3):
                The campaign is not eligible to serve and has
                issues that may require intervention.
            PENDING (4):
                The campaign is pending the approval of at
                least one ad.
            ELIGIBLE (5):
                The campaign is eligible to serve.
            REMOVED (6):
                The campaign has been removed.
            ENDED (7):
                The campaign has ended.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        PAUSED = 2
        NOT_ELIGIBLE = 3
        PENDING = 4
        ELIGIBLE = 5
        REMOVED = 6
        ENDED = 7


__all__ = tuple(sorted(__protobuf__.manifest))
