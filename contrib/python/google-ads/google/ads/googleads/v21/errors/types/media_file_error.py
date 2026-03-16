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
    package="google.ads.googleads.v21.errors",
    marshal="google.ads.googleads.v21",
    manifest={
        "MediaFileErrorEnum",
    },
)


class MediaFileErrorEnum(proto.Message):
    r"""Container for enum describing possible media file errors."""

    class MediaFileError(proto.Enum):
        r"""Enum describing possible media file errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            CANNOT_CREATE_STANDARD_ICON (2):
                Cannot create a standard icon type.
            CANNOT_SELECT_STANDARD_ICON_WITH_OTHER_TYPES (3):
                May only select Standard Icons alone.
            CANNOT_SPECIFY_MEDIA_FILE_ID_AND_DATA (4):
                Image contains both a media file ID and data.
            DUPLICATE_MEDIA (5):
                A media file with given type and reference ID
                already exists.
            EMPTY_FIELD (6):
                A required field was not specified or is an
                empty string.
            RESOURCE_REFERENCED_IN_MULTIPLE_OPS (7):
                A media file may only be modified once per
                call.
            FIELD_NOT_SUPPORTED_FOR_MEDIA_SUB_TYPE (8):
                Field is not supported for the media sub
                type.
            INVALID_MEDIA_FILE_ID (9):
                The media file ID is invalid.
            INVALID_MEDIA_SUB_TYPE (10):
                The media subtype is invalid.
            INVALID_MEDIA_FILE_TYPE (11):
                The media file type is invalid.
            INVALID_MIME_TYPE (12):
                The mimetype is invalid.
            INVALID_REFERENCE_ID (13):
                The media reference ID is invalid.
            INVALID_YOU_TUBE_ID (14):
                The YouTube video ID is invalid.
            MEDIA_FILE_FAILED_TRANSCODING (15):
                Media file has failed transcoding
            MEDIA_NOT_TRANSCODED (16):
                Media file has not been transcoded.
            MEDIA_TYPE_DOES_NOT_MATCH_MEDIA_FILE_TYPE (17):
                The media type does not match the actual
                media file's type.
            NO_FIELDS_SPECIFIED (18):
                None of the fields have been specified.
            NULL_REFERENCE_ID_AND_MEDIA_ID (19):
                One of reference ID or media file ID must be
                specified.
            TOO_LONG (20):
                The string has too many characters.
            UNSUPPORTED_TYPE (21):
                The specified type is not supported.
            YOU_TUBE_SERVICE_UNAVAILABLE (22):
                YouTube is unavailable for requesting video
                data.
            YOU_TUBE_VIDEO_HAS_NON_POSITIVE_DURATION (23):
                The YouTube video has a non positive
                duration.
            YOU_TUBE_VIDEO_NOT_FOUND (24):
                The YouTube video ID is syntactically valid
                but the video was not found.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        CANNOT_CREATE_STANDARD_ICON = 2
        CANNOT_SELECT_STANDARD_ICON_WITH_OTHER_TYPES = 3
        CANNOT_SPECIFY_MEDIA_FILE_ID_AND_DATA = 4
        DUPLICATE_MEDIA = 5
        EMPTY_FIELD = 6
        RESOURCE_REFERENCED_IN_MULTIPLE_OPS = 7
        FIELD_NOT_SUPPORTED_FOR_MEDIA_SUB_TYPE = 8
        INVALID_MEDIA_FILE_ID = 9
        INVALID_MEDIA_SUB_TYPE = 10
        INVALID_MEDIA_FILE_TYPE = 11
        INVALID_MIME_TYPE = 12
        INVALID_REFERENCE_ID = 13
        INVALID_YOU_TUBE_ID = 14
        MEDIA_FILE_FAILED_TRANSCODING = 15
        MEDIA_NOT_TRANSCODED = 16
        MEDIA_TYPE_DOES_NOT_MATCH_MEDIA_FILE_TYPE = 17
        NO_FIELDS_SPECIFIED = 18
        NULL_REFERENCE_ID_AND_MEDIA_ID = 19
        TOO_LONG = 20
        UNSUPPORTED_TYPE = 21
        YOU_TUBE_SERVICE_UNAVAILABLE = 22
        YOU_TUBE_VIDEO_HAS_NON_POSITIVE_DURATION = 23
        YOU_TUBE_VIDEO_NOT_FOUND = 24


__all__ = tuple(sorted(__protobuf__.manifest))
