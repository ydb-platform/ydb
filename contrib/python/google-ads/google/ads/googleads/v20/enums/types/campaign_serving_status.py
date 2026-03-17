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
    package="google.ads.googleads.v20.enums",
    marshal="google.ads.googleads.v20",
    manifest={
        "CampaignServingStatusEnum",
    },
)


class CampaignServingStatusEnum(proto.Message):
    r"""Message describing Campaign serving statuses."""

    class CampaignServingStatus(proto.Enum):
        r"""Possible serving statuses of a campaign.

        Values:
            UNSPECIFIED (0):
                No value has been specified.
            UNKNOWN (1):
                The received value is not known in this
                version.
                This is a response-only value.
            SERVING (2):
                Serving.
            NONE (3):
                None.
            ENDED (4):
                Ended.
            PENDING (5):
                Pending.
            SUSPENDED (6):
                Suspended.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        SERVING = 2
        NONE = 3
        ENDED = 4
        PENDING = 5
        SUSPENDED = 6


__all__ = tuple(sorted(__protobuf__.manifest))
