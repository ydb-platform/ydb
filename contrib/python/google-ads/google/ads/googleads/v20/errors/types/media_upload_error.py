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
    package="google.ads.googleads.v20.errors",
    marshal="google.ads.googleads.v20",
    manifest={
        "MediaUploadErrorEnum",
    },
)


class MediaUploadErrorEnum(proto.Message):
    r"""Container for enum describing possible media uploading
    errors.

    """

    class MediaUploadError(proto.Enum):
        r"""Enum describing possible media uploading errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            FILE_TOO_BIG (2):
                The uploaded file is too big.
            UNPARSEABLE_IMAGE (3):
                Image data is unparseable.
            ANIMATED_IMAGE_NOT_ALLOWED (4):
                Animated images are not allowed.
            FORMAT_NOT_ALLOWED (5):
                The image or media bundle format is not
                allowed.
            EXTERNAL_URL_NOT_ALLOWED (6):
                Cannot reference URL external to the media
                bundle.
            INVALID_URL_REFERENCE (7):
                HTML5 ad is trying to reference an asset not
                in .ZIP file.
            MISSING_PRIMARY_MEDIA_BUNDLE_ENTRY (8):
                The media bundle contains no primary entry.
            ANIMATED_VISUAL_EFFECT (9):
                Animation has disallowed visual effects.
            ANIMATION_TOO_LONG (10):
                Animation longer than the allowed 30 second
                limit.
            ASPECT_RATIO_NOT_ALLOWED (11):
                The aspect ratio of the image does not match
                the expected aspect ratios provided in the asset
                spec.
            AUDIO_NOT_ALLOWED_IN_MEDIA_BUNDLE (12):
                Audio files are not allowed in bundle.
            CMYK_JPEG_NOT_ALLOWED (13):
                CMYK jpegs are not supported.
            FLASH_NOT_ALLOWED (14):
                Flash movies are not allowed.
            FRAME_RATE_TOO_HIGH (15):
                The frame rate of the video is higher than
                the allowed 5fps.
            GOOGLE_WEB_DESIGNER_ZIP_FILE_NOT_PUBLISHED (16):
                ZIP file from Google Web Designer is not
                published.
            IMAGE_CONSTRAINTS_VIOLATED (17):
                Image constraints are violated, but more details (like
                DIMENSIONS_NOT_ALLOWED or ASPECT_RATIO_NOT_ALLOWED) can not
                be provided. This happens when asset spec contains more than
                one constraint and criteria of different constraints are
                violated.
            INVALID_MEDIA_BUNDLE (18):
                Media bundle data is unrecognizable.
            INVALID_MEDIA_BUNDLE_ENTRY (19):
                There was a problem with one or more of the
                media bundle entries.
            INVALID_MIME_TYPE (20):
                The asset has an invalid mime type.
            INVALID_PATH (21):
                The media bundle contains an invalid asset
                path.
            LAYOUT_PROBLEM (22):
                Image has layout problem.
            MALFORMED_URL (23):
                An asset had a URL reference that is
                malformed per RFC 1738 convention.
            MEDIA_BUNDLE_NOT_ALLOWED (24):
                The uploaded media bundle format is not
                allowed.
            MEDIA_BUNDLE_NOT_COMPATIBLE_TO_PRODUCT_TYPE (25):
                The media bundle is not compatible with the
                asset spec product type. (For example, Gmail,
                dynamic remarketing, etc.)
            MEDIA_BUNDLE_REJECTED_BY_MULTIPLE_ASSET_SPECS (26):
                A bundle being uploaded that is incompatible
                with multiple assets for different reasons.
            TOO_MANY_FILES_IN_MEDIA_BUNDLE (27):
                The media bundle contains too many files.
            UNSUPPORTED_GOOGLE_WEB_DESIGNER_ENVIRONMENT (28):
                Google Web Designer not created for "Google
                Ads" environment.
            UNSUPPORTED_HTML5_FEATURE (29):
                Unsupported HTML5 feature in HTML5 asset.
            URL_IN_MEDIA_BUNDLE_NOT_SSL_COMPLIANT (30):
                URL in HTML5 entry is not SSL compliant.
            VIDEO_FILE_NAME_TOO_LONG (31):
                Video file name is longer than the 50 allowed
                characters.
            VIDEO_MULTIPLE_FILES_WITH_SAME_NAME (32):
                Multiple videos with same name in a bundle.
            VIDEO_NOT_ALLOWED_IN_MEDIA_BUNDLE (33):
                Videos are not allowed in media bundle.
            CANNOT_UPLOAD_MEDIA_TYPE_THROUGH_API (34):
                This type of media cannot be uploaded through
                the Google Ads API.
            DIMENSIONS_NOT_ALLOWED (35):
                The dimensions of the image are not allowed.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        FILE_TOO_BIG = 2
        UNPARSEABLE_IMAGE = 3
        ANIMATED_IMAGE_NOT_ALLOWED = 4
        FORMAT_NOT_ALLOWED = 5
        EXTERNAL_URL_NOT_ALLOWED = 6
        INVALID_URL_REFERENCE = 7
        MISSING_PRIMARY_MEDIA_BUNDLE_ENTRY = 8
        ANIMATED_VISUAL_EFFECT = 9
        ANIMATION_TOO_LONG = 10
        ASPECT_RATIO_NOT_ALLOWED = 11
        AUDIO_NOT_ALLOWED_IN_MEDIA_BUNDLE = 12
        CMYK_JPEG_NOT_ALLOWED = 13
        FLASH_NOT_ALLOWED = 14
        FRAME_RATE_TOO_HIGH = 15
        GOOGLE_WEB_DESIGNER_ZIP_FILE_NOT_PUBLISHED = 16
        IMAGE_CONSTRAINTS_VIOLATED = 17
        INVALID_MEDIA_BUNDLE = 18
        INVALID_MEDIA_BUNDLE_ENTRY = 19
        INVALID_MIME_TYPE = 20
        INVALID_PATH = 21
        LAYOUT_PROBLEM = 22
        MALFORMED_URL = 23
        MEDIA_BUNDLE_NOT_ALLOWED = 24
        MEDIA_BUNDLE_NOT_COMPATIBLE_TO_PRODUCT_TYPE = 25
        MEDIA_BUNDLE_REJECTED_BY_MULTIPLE_ASSET_SPECS = 26
        TOO_MANY_FILES_IN_MEDIA_BUNDLE = 27
        UNSUPPORTED_GOOGLE_WEB_DESIGNER_ENVIRONMENT = 28
        UNSUPPORTED_HTML5_FEATURE = 29
        URL_IN_MEDIA_BUNDLE_NOT_SSL_COMPLIANT = 30
        VIDEO_FILE_NAME_TOO_LONG = 31
        VIDEO_MULTIPLE_FILES_WITH_SAME_NAME = 32
        VIDEO_NOT_ALLOWED_IN_MEDIA_BUNDLE = 33
        CANNOT_UPLOAD_MEDIA_TYPE_THROUGH_API = 34
        DIMENSIONS_NOT_ALLOWED = 35


__all__ = tuple(sorted(__protobuf__.manifest))
