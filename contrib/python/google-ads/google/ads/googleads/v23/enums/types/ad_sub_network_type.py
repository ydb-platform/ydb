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
        "AdSubNetworkTypeEnum",
    },
)


class AdSubNetworkTypeEnum(proto.Message):
    r"""Container for enumeration of Google Ads sub network types."""

    class AdSubNetworkType(proto.Enum):
        r"""Enumerates Google Ads sub network types.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Unknown.
            UNSEGMENTED (2):
                The whole network without any sub network
                type segmentation.
            YOUTUBE_INSTREAM (3):
                Ads served in-stream of YouTube organic
                videos.
            YOUTUBE_INFEED (4):
                Ads served on YouTube feed surfaces, such as
                YouTube Home or Watch Next.
            YOUTUBE_SHORTS (5):
                Ads served on the YouTube Shorts feed.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        UNSEGMENTED = 2
        YOUTUBE_INSTREAM = 3
        YOUTUBE_INFEED = 4
        YOUTUBE_SHORTS = 5


__all__ = tuple(sorted(__protobuf__.manifest))
