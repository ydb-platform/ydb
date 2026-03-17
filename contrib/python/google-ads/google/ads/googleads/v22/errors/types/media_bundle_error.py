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
        "MediaBundleErrorEnum",
    },
)


class MediaBundleErrorEnum(proto.Message):
    r"""Container for enum describing possible media bundle errors."""

    class MediaBundleError(proto.Enum):
        r"""Enum describing possible media bundle errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            BAD_REQUEST (3):
                There was a problem with the request.
            DOUBLECLICK_BUNDLE_NOT_ALLOWED (4):
                HTML5 ads using DoubleClick Studio created
                ZIP files are not supported.
            EXTERNAL_URL_NOT_ALLOWED (5):
                Cannot reference URL external to the media
                bundle.
            FILE_TOO_LARGE (6):
                Media bundle file is too large.
            GOOGLE_WEB_DESIGNER_ZIP_FILE_NOT_PUBLISHED (7):
                ZIP file from Google Web Designer is not
                published.
            INVALID_INPUT (8):
                Input was invalid.
            INVALID_MEDIA_BUNDLE (9):
                There was a problem with the media bundle.
            INVALID_MEDIA_BUNDLE_ENTRY (10):
                There was a problem with one or more of the
                media bundle entries.
            INVALID_MIME_TYPE (11):
                The media bundle contains a file with an
                unknown mime type
            INVALID_PATH (12):
                The media bundle contain an invalid asset
                path.
            INVALID_URL_REFERENCE (13):
                HTML5 ad is trying to reference an asset not
                in .ZIP file
            MEDIA_DATA_TOO_LARGE (14):
                Media data is too large.
            MISSING_PRIMARY_MEDIA_BUNDLE_ENTRY (15):
                The media bundle contains no primary entry.
            SERVER_ERROR (16):
                There was an error on the server.
            STORAGE_ERROR (17):
                The image could not be stored.
            SWIFFY_BUNDLE_NOT_ALLOWED (18):
                Media bundle created with the Swiffy tool is
                not allowed.
            TOO_MANY_FILES (19):
                The media bundle contains too many files.
            UNEXPECTED_SIZE (20):
                The media bundle is not of legal dimensions.
            UNSUPPORTED_GOOGLE_WEB_DESIGNER_ENVIRONMENT (21):
                Google Web Designer not created for "Google
                Ads" environment.
            UNSUPPORTED_HTML5_FEATURE (22):
                Unsupported HTML5 feature in HTML5 asset.
            URL_IN_MEDIA_BUNDLE_NOT_SSL_COMPLIANT (23):
                URL in HTML5 entry is not ssl compliant.
            CUSTOM_EXIT_NOT_ALLOWED (24):
                Custom exits not allowed in HTML5 entry.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        BAD_REQUEST = 3
        DOUBLECLICK_BUNDLE_NOT_ALLOWED = 4
        EXTERNAL_URL_NOT_ALLOWED = 5
        FILE_TOO_LARGE = 6
        GOOGLE_WEB_DESIGNER_ZIP_FILE_NOT_PUBLISHED = 7
        INVALID_INPUT = 8
        INVALID_MEDIA_BUNDLE = 9
        INVALID_MEDIA_BUNDLE_ENTRY = 10
        INVALID_MIME_TYPE = 11
        INVALID_PATH = 12
        INVALID_URL_REFERENCE = 13
        MEDIA_DATA_TOO_LARGE = 14
        MISSING_PRIMARY_MEDIA_BUNDLE_ENTRY = 15
        SERVER_ERROR = 16
        STORAGE_ERROR = 17
        SWIFFY_BUNDLE_NOT_ALLOWED = 18
        TOO_MANY_FILES = 19
        UNEXPECTED_SIZE = 20
        UNSUPPORTED_GOOGLE_WEB_DESIGNER_ENVIRONMENT = 21
        UNSUPPORTED_HTML5_FEATURE = 22
        URL_IN_MEDIA_BUNDLE_NOT_SSL_COMPLIANT = 23
        CUSTOM_EXIT_NOT_ALLOWED = 24


__all__ = tuple(sorted(__protobuf__.manifest))
