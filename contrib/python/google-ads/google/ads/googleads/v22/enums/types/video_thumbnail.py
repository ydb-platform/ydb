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
        "VideoThumbnailEnum",
    },
)


class VideoThumbnailEnum(proto.Message):
    r"""Defines the thumbnail to use for In-Display video ads. Note that
    DEFAULT_THUMBNAIL may have been uploaded by the user while
    thumbnails 1-3 are auto-generated from the video.

    """

    class VideoThumbnail(proto.Enum):
        r"""Enum listing the possible types of a video thumbnail.

        Values:
            UNSPECIFIED (0):
                The type has not been specified.
            UNKNOWN (1):
                The received value is not known in this
                version. This is a response-only value.
            DEFAULT_THUMBNAIL (2):
                The default thumbnail. Can be auto-generated
                or user-uploaded.
            THUMBNAIL_1 (3):
                Thumbnail 1, generated from the video.
            THUMBNAIL_2 (4):
                Thumbnail 2, generated from the video.
            THUMBNAIL_3 (5):
                Thumbnail 3, generated from the video.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        DEFAULT_THUMBNAIL = 2
        THUMBNAIL_1 = 3
        THUMBNAIL_2 = 4
        THUMBNAIL_3 = 5


__all__ = tuple(sorted(__protobuf__.manifest))
