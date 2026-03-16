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
        "YouTubeVideoUploadStateEnum",
    },
)


class YouTubeVideoUploadStateEnum(proto.Message):
    r"""Container for enum describing the state of a YouTube video
    upload.

    """

    class YouTubeVideoUploadState(proto.Enum):
        r"""Represents the current state of a video within its upload and
        processing lifecycle. It tracks the full progression from upload
        initiation through processing and completion, including failure,
        rejection, or deletion outcomes. It helps determine whether a
        video is still being uploaded, ready for use, or no longer
        available.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                The value is unknown in this version.
            PENDING (2):
                The video is currently being uploaded.
            UPLOADED (3):
                The video was uploaded and is being
                processed.
            PROCESSED (4):
                The video was successfully uploaded and
                processed.
            FAILED (5):
                The video upload or processing did not
                complete successfully.
            REJECTED (6):
                The video was not accepted due to validation
                or policy reasons.
            UNAVAILABLE (7):
                The video upload state is unavailable. It may
                have been removed from YouTube.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        PENDING = 2
        UPLOADED = 3
        PROCESSED = 4
        FAILED = 5
        REJECTED = 6
        UNAVAILABLE = 7


__all__ = tuple(sorted(__protobuf__.manifest))
