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
        "AssetLinkPrimaryStatusEnum",
    },
)


class AssetLinkPrimaryStatusEnum(proto.Message):
    r"""Provides the primary status of an asset link.
    For example: a sitelink may be paused for a particular campaign.

    """

    class AssetLinkPrimaryStatus(proto.Enum):
        r"""Enum Provides insight into why an asset is not serving or not
        serving at full capacity for a particular link level. There
        could be one status with multiple reasons. For example, a
        sitelink might be paused by the user, but also limited in
        serving due to violation of an alcohol policy. In this case, the
        PrimaryStatus will be returned as PAUSED, since the asset's
        effective status is determined by its paused state.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            ELIGIBLE (2):
                The asset is eligible to serve.
            PAUSED (3):
                The user-specified asset link status is
                paused.
            REMOVED (4):
                The user-specified asset link status is
                removed.
            PENDING (5):
                The asset may serve in the future.
            LIMITED (6):
                The asset is serving in a partial capacity.
            NOT_ELIGIBLE (7):
                The asset is not eligible to serve.
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
