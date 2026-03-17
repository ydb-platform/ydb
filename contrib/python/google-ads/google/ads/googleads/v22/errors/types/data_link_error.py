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
    package="google.ads.googleads.v22.errors",
    marshal="google.ads.googleads.v22",
    manifest={
        "DataLinkErrorEnum",
    },
)


class DataLinkErrorEnum(proto.Message):
    r"""Container for enum describing possible DataLink errors."""

    class DataLinkError(proto.Enum):
        r"""Enum describing possible DataLink errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            YOUTUBE_CHANNEL_ID_INVALID (2):
                The requested YouTube Channel ID is invalid.
            YOUTUBE_VIDEO_ID_INVALID (3):
                The requested YouTube Video ID is invalid.
            YOUTUBE_VIDEO_FROM_DIFFERENT_CHANNEL (4):
                The requested YouTube Video ID doesn't belong
                to the requested YouTube Channel ID.
            PERMISSION_DENIED (5):
                A link cannot be created because the customer
                doesn't have the permission.
            INVALID_STATUS (6):
                A link can not be removed or updated because
                the status is invalid.
            INVALID_UPDATE_STATUS (7):
                The input status in the update request is
                invalid.
            INVALID_RESOURCE_NAME (8):
                The input resource name is invalid.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        YOUTUBE_CHANNEL_ID_INVALID = 2
        YOUTUBE_VIDEO_ID_INVALID = 3
        YOUTUBE_VIDEO_FROM_DIFFERENT_CHANNEL = 4
        PERMISSION_DENIED = 5
        INVALID_STATUS = 6
        INVALID_UPDATE_STATUS = 7
        INVALID_RESOURCE_NAME = 8


__all__ = tuple(sorted(__protobuf__.manifest))
