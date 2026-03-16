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
        "MediaTypeEnum",
    },
)


class MediaTypeEnum(proto.Message):
    r"""Container for enum describing the types of media."""

    class MediaType(proto.Enum):
        r"""The type of media.

        Values:
            UNSPECIFIED (0):
                The media type has not been specified.
            UNKNOWN (1):
                The received value is not known in this
                version.
                This is a response-only value.
            IMAGE (2):
                Static image, used for image ad.
            ICON (3):
                Small image, used for map ad.
            MEDIA_BUNDLE (4):
                ZIP file, used in fields of template ads.
            AUDIO (5):
                Audio file.
            VIDEO (6):
                Video file.
            DYNAMIC_IMAGE (7):
                Animated image, such as animated GIF.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        IMAGE = 2
        ICON = 3
        MEDIA_BUNDLE = 4
        AUDIO = 5
        VIDEO = 6
        DYNAMIC_IMAGE = 7


__all__ = tuple(sorted(__protobuf__.manifest))
