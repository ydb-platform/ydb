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
        "AssetCoverageVideoAspectRatioRequirementEnum",
    },
)


class AssetCoverageVideoAspectRatioRequirementEnum(proto.Message):
    r"""Container for enum describing possible ad strength video
    aspect ratio requirements.

    """

    class AssetCoverageVideoAspectRatioRequirement(proto.Enum):
        r"""Enum describing possible ad strength video aspect ratio
        requirements.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                The received value is not known in this
                version.
            HORIZONTAL (2):
                The video requires a horizontal aspect ratio.
            SQUARE (3):
                The video requires a square aspect ratio.
            VERTICAL (4):
                The video requires a vertical aspect ratio.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        HORIZONTAL = 2
        SQUARE = 3
        VERTICAL = 4


__all__ = tuple(sorted(__protobuf__.manifest))
