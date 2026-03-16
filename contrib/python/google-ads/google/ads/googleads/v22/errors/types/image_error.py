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
        "ImageErrorEnum",
    },
)


class ImageErrorEnum(proto.Message):
    r"""Container for enum describing possible image errors."""

    class ImageError(proto.Enum):
        r"""Enum describing possible image errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            INVALID_IMAGE (2):
                The image is not valid.
            STORAGE_ERROR (3):
                The image could not be stored.
            BAD_REQUEST (4):
                There was a problem with the request.
            UNEXPECTED_SIZE (5):
                The image is not of legal dimensions.
            ANIMATED_NOT_ALLOWED (6):
                Animated image are not permitted.
            ANIMATION_TOO_LONG (7):
                Animation is too long.
            SERVER_ERROR (8):
                There was an error on the server.
            CMYK_JPEG_NOT_ALLOWED (9):
                Image cannot be in CMYK color format.
            FLASH_NOT_ALLOWED (10):
                Flash images are not permitted.
            FLASH_WITHOUT_CLICKTAG (11):
                Flash images must support clickTag.
            FLASH_ERROR_AFTER_FIXING_CLICK_TAG (12):
                A flash error has occurred after fixing the
                click tag.
            ANIMATED_VISUAL_EFFECT (13):
                Unacceptable visual effects.
            FLASH_ERROR (14):
                There was a problem with the flash image.
            LAYOUT_PROBLEM (15):
                Incorrect image layout.
            PROBLEM_READING_IMAGE_FILE (16):
                There was a problem reading the image file.
            ERROR_STORING_IMAGE (17):
                There was an error storing the image.
            ASPECT_RATIO_NOT_ALLOWED (18):
                The aspect ratio of the image is not allowed.
            FLASH_HAS_NETWORK_OBJECTS (19):
                Flash cannot have network objects.
            FLASH_HAS_NETWORK_METHODS (20):
                Flash cannot have network methods.
            FLASH_HAS_URL (21):
                Flash cannot have a Url.
            FLASH_HAS_MOUSE_TRACKING (22):
                Flash cannot use mouse tracking.
            FLASH_HAS_RANDOM_NUM (23):
                Flash cannot have a random number.
            FLASH_SELF_TARGETS (24):
                Ad click target cannot be '\_self'.
            FLASH_BAD_GETURL_TARGET (25):
                GetUrl method should only use '\_blank'.
            FLASH_VERSION_NOT_SUPPORTED (26):
                Flash version is not supported.
            FLASH_WITHOUT_HARD_CODED_CLICK_URL (27):
                Flash movies need to have hard coded click
                URL or clickTAG
            INVALID_FLASH_FILE (28):
                Uploaded flash file is corrupted.
            FAILED_TO_FIX_CLICK_TAG_IN_FLASH (29):
                Uploaded flash file can be parsed, but the
                click tag can not be fixed properly.
            FLASH_ACCESSES_NETWORK_RESOURCES (30):
                Flash movie accesses network resources
            FLASH_EXTERNAL_JS_CALL (31):
                Flash movie attempts to call external
                javascript code
            FLASH_EXTERNAL_FS_CALL (32):
                Flash movie attempts to call flash system
                commands
            FILE_TOO_LARGE (33):
                Image file is too large.
            IMAGE_DATA_TOO_LARGE (34):
                Image data is too large.
            IMAGE_PROCESSING_ERROR (35):
                Error while processing the image.
            IMAGE_TOO_SMALL (36):
                Image is too small.
            INVALID_INPUT (37):
                Input was invalid.
            PROBLEM_READING_FILE (38):
                There was a problem reading the image file.
            IMAGE_CONSTRAINTS_VIOLATED (39):
                Image constraints are violated, but details like
                ASPECT_RATIO_NOT_ALLOWED can't be provided. This happens
                when asset spec contains more than one constraint and
                different criteria of different constraints are violated.
            FORMAT_NOT_ALLOWED (40):
                Image format is not allowed.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        INVALID_IMAGE = 2
        STORAGE_ERROR = 3
        BAD_REQUEST = 4
        UNEXPECTED_SIZE = 5
        ANIMATED_NOT_ALLOWED = 6
        ANIMATION_TOO_LONG = 7
        SERVER_ERROR = 8
        CMYK_JPEG_NOT_ALLOWED = 9
        FLASH_NOT_ALLOWED = 10
        FLASH_WITHOUT_CLICKTAG = 11
        FLASH_ERROR_AFTER_FIXING_CLICK_TAG = 12
        ANIMATED_VISUAL_EFFECT = 13
        FLASH_ERROR = 14
        LAYOUT_PROBLEM = 15
        PROBLEM_READING_IMAGE_FILE = 16
        ERROR_STORING_IMAGE = 17
        ASPECT_RATIO_NOT_ALLOWED = 18
        FLASH_HAS_NETWORK_OBJECTS = 19
        FLASH_HAS_NETWORK_METHODS = 20
        FLASH_HAS_URL = 21
        FLASH_HAS_MOUSE_TRACKING = 22
        FLASH_HAS_RANDOM_NUM = 23
        FLASH_SELF_TARGETS = 24
        FLASH_BAD_GETURL_TARGET = 25
        FLASH_VERSION_NOT_SUPPORTED = 26
        FLASH_WITHOUT_HARD_CODED_CLICK_URL = 27
        INVALID_FLASH_FILE = 28
        FAILED_TO_FIX_CLICK_TAG_IN_FLASH = 29
        FLASH_ACCESSES_NETWORK_RESOURCES = 30
        FLASH_EXTERNAL_JS_CALL = 31
        FLASH_EXTERNAL_FS_CALL = 32
        FILE_TOO_LARGE = 33
        IMAGE_DATA_TOO_LARGE = 34
        IMAGE_PROCESSING_ERROR = 35
        IMAGE_TOO_SMALL = 36
        INVALID_INPUT = 37
        PROBLEM_READING_FILE = 38
        IMAGE_CONSTRAINTS_VIOLATED = 39
        FORMAT_NOT_ALLOWED = 40


__all__ = tuple(sorted(__protobuf__.manifest))
