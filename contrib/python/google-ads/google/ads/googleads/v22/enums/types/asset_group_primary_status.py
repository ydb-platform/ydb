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
        "AssetGroupPrimaryStatusEnum",
    },
)


class AssetGroupPrimaryStatusEnum(proto.Message):
    r"""Container for enum describing possible asset group primary
    status.

    """

    class AssetGroupPrimaryStatus(proto.Enum):
        r"""Enum describing the possible asset group primary status.
        Provides insights into why an asset group is not serving or not
        serving optimally.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            ELIGIBLE (2):
                The asset group is eligible to serve.
            PAUSED (3):
                The asset group is paused.
            REMOVED (4):
                The asset group is removed.
            NOT_ELIGIBLE (5):
                The asset group is not eligible to serve.
            LIMITED (6):
                The asset group has limited servability.
            PENDING (7):
                The asset group is pending approval and may
                serve in the future.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        ELIGIBLE = 2
        PAUSED = 3
        REMOVED = 4
        NOT_ELIGIBLE = 5
        LIMITED = 6
        PENDING = 7


__all__ = tuple(sorted(__protobuf__.manifest))
