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
        "UrlFieldErrorEnum",
    },
)


class UrlFieldErrorEnum(proto.Message):
    r"""Container for enum describing possible url field errors."""

    class UrlFieldError(proto.Enum):
        r"""Enum describing possible url field errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            INVALID_TRACKING_URL_TEMPLATE (2):
                The tracking url template is invalid.
            INVALID_TAG_IN_TRACKING_URL_TEMPLATE (3):
                The tracking url template contains invalid
                tag.
            MISSING_TRACKING_URL_TEMPLATE_TAG (4):
                The tracking url template must contain at
                least one tag (for example, {lpurl}), This
                applies only to tracking url template associated
                with website ads or product ads.
            MISSING_PROTOCOL_IN_TRACKING_URL_TEMPLATE (5):
                The tracking url template must start with a
                valid protocol (or lpurl tag).
            INVALID_PROTOCOL_IN_TRACKING_URL_TEMPLATE (6):
                The tracking url template starts with an
                invalid protocol.
            MALFORMED_TRACKING_URL_TEMPLATE (7):
                The tracking url template contains illegal
                characters.
            MISSING_HOST_IN_TRACKING_URL_TEMPLATE (8):
                The tracking url template must contain a host
                name (or lpurl tag).
            INVALID_TLD_IN_TRACKING_URL_TEMPLATE (9):
                The tracking url template has an invalid or
                missing top level domain extension.
            REDUNDANT_NESTED_TRACKING_URL_TEMPLATE_TAG (10):
                The tracking url template contains nested
                occurrences of the same conditional tag (for
                example, {ifmobile:{ifmobile:x}}).
            INVALID_FINAL_URL (11):
                The final url is invalid.
            INVALID_TAG_IN_FINAL_URL (12):
                The final url contains invalid tag.
            REDUNDANT_NESTED_FINAL_URL_TAG (13):
                The final url contains nested occurrences of
                the same conditional tag (for example,
                {ifmobile:{ifmobile:x}}).
            MISSING_PROTOCOL_IN_FINAL_URL (14):
                The final url must start with a valid
                protocol.
            INVALID_PROTOCOL_IN_FINAL_URL (15):
                The final url starts with an invalid
                protocol.
            MALFORMED_FINAL_URL (16):
                The final url contains illegal characters.
            MISSING_HOST_IN_FINAL_URL (17):
                The final url must contain a host name.
            INVALID_TLD_IN_FINAL_URL (18):
                The tracking url template has an invalid or
                missing top level domain extension.
            INVALID_FINAL_MOBILE_URL (19):
                The final mobile url is invalid.
            INVALID_TAG_IN_FINAL_MOBILE_URL (20):
                The final mobile url contains invalid tag.
            REDUNDANT_NESTED_FINAL_MOBILE_URL_TAG (21):
                The final mobile url contains nested
                occurrences of the same conditional tag (for
                example, {ifmobile:{ifmobile:x}}).
            MISSING_PROTOCOL_IN_FINAL_MOBILE_URL (22):
                The final mobile url must start with a valid
                protocol.
            INVALID_PROTOCOL_IN_FINAL_MOBILE_URL (23):
                The final mobile url starts with an invalid
                protocol.
            MALFORMED_FINAL_MOBILE_URL (24):
                The final mobile url contains illegal
                characters.
            MISSING_HOST_IN_FINAL_MOBILE_URL (25):
                The final mobile url must contain a host
                name.
            INVALID_TLD_IN_FINAL_MOBILE_URL (26):
                The tracking url template has an invalid or
                missing top level domain extension.
            INVALID_FINAL_APP_URL (27):
                The final app url is invalid.
            INVALID_TAG_IN_FINAL_APP_URL (28):
                The final app url contains invalid tag.
            REDUNDANT_NESTED_FINAL_APP_URL_TAG (29):
                The final app url contains nested occurrences
                of the same conditional tag (for example,
                {ifmobile:{ifmobile:x}}).
            MULTIPLE_APP_URLS_FOR_OSTYPE (30):
                More than one app url found for the same OS
                type.
            INVALID_OSTYPE (31):
                The OS type given for an app url is not
                valid.
            INVALID_PROTOCOL_FOR_APP_URL (32):
                The protocol given for an app url is not
                valid. (For example, "android-app://")
            INVALID_PACKAGE_ID_FOR_APP_URL (33):
                The package id (app id) given for an app url
                is not valid.
            URL_CUSTOM_PARAMETERS_COUNT_EXCEEDS_LIMIT (34):
                The number of url custom parameters for an
                resource exceeds the maximum limit allowed.
            INVALID_CHARACTERS_IN_URL_CUSTOM_PARAMETER_KEY (39):
                An invalid character appears in the parameter
                key.
            INVALID_CHARACTERS_IN_URL_CUSTOM_PARAMETER_VALUE (40):
                An invalid character appears in the parameter
                value.
            INVALID_TAG_IN_URL_CUSTOM_PARAMETER_VALUE (41):
                The url custom parameter value fails url tag
                validation.
            REDUNDANT_NESTED_URL_CUSTOM_PARAMETER_TAG (42):
                The custom parameter contains nested
                occurrences of the same conditional tag (for
                example, {ifmobile:{ifmobile:x}}).
            MISSING_PROTOCOL (43):
                The protocol (http:// or https://) is
                missing.
            INVALID_PROTOCOL (52):
                Unsupported protocol in URL. Only http and
                https are supported.
            INVALID_URL (44):
                The url is invalid.
            DESTINATION_URL_DEPRECATED (45):
                Destination Url is deprecated.
            INVALID_TAG_IN_URL (46):
                The url contains invalid tag.
            MISSING_URL_TAG (47):
                The url must contain at least one tag (for
                example, {lpurl}).
            DUPLICATE_URL_ID (48):
                Duplicate url id.
            INVALID_URL_ID (49):
                Invalid url id.
            FINAL_URL_SUFFIX_MALFORMED (50):
                The final url suffix cannot begin with '?' or
                '&' characters and must be a valid query string.
            INVALID_TAG_IN_FINAL_URL_SUFFIX (51):
                The final url suffix cannot contain {lpurl}
                related or {ignore} tags.
            INVALID_TOP_LEVEL_DOMAIN (53):
                The top level domain is invalid, for example,
                not a public top level domain listed in
                publicsuffix.org.
            MALFORMED_TOP_LEVEL_DOMAIN (54):
                Malformed top level domain in URL.
            MALFORMED_URL (55):
                Malformed URL.
            MISSING_HOST (56):
                No host found in URL.
            NULL_CUSTOM_PARAMETER_VALUE (57):
                Custom parameter value cannot be null.
            VALUE_TRACK_PARAMETER_NOT_SUPPORTED (58):
                Track parameter is not supported.
            UNSUPPORTED_APP_STORE (59):
                The app store connected to the url is not
                supported.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        INVALID_TRACKING_URL_TEMPLATE = 2
        INVALID_TAG_IN_TRACKING_URL_TEMPLATE = 3
        MISSING_TRACKING_URL_TEMPLATE_TAG = 4
        MISSING_PROTOCOL_IN_TRACKING_URL_TEMPLATE = 5
        INVALID_PROTOCOL_IN_TRACKING_URL_TEMPLATE = 6
        MALFORMED_TRACKING_URL_TEMPLATE = 7
        MISSING_HOST_IN_TRACKING_URL_TEMPLATE = 8
        INVALID_TLD_IN_TRACKING_URL_TEMPLATE = 9
        REDUNDANT_NESTED_TRACKING_URL_TEMPLATE_TAG = 10
        INVALID_FINAL_URL = 11
        INVALID_TAG_IN_FINAL_URL = 12
        REDUNDANT_NESTED_FINAL_URL_TAG = 13
        MISSING_PROTOCOL_IN_FINAL_URL = 14
        INVALID_PROTOCOL_IN_FINAL_URL = 15
        MALFORMED_FINAL_URL = 16
        MISSING_HOST_IN_FINAL_URL = 17
        INVALID_TLD_IN_FINAL_URL = 18
        INVALID_FINAL_MOBILE_URL = 19
        INVALID_TAG_IN_FINAL_MOBILE_URL = 20
        REDUNDANT_NESTED_FINAL_MOBILE_URL_TAG = 21
        MISSING_PROTOCOL_IN_FINAL_MOBILE_URL = 22
        INVALID_PROTOCOL_IN_FINAL_MOBILE_URL = 23
        MALFORMED_FINAL_MOBILE_URL = 24
        MISSING_HOST_IN_FINAL_MOBILE_URL = 25
        INVALID_TLD_IN_FINAL_MOBILE_URL = 26
        INVALID_FINAL_APP_URL = 27
        INVALID_TAG_IN_FINAL_APP_URL = 28
        REDUNDANT_NESTED_FINAL_APP_URL_TAG = 29
        MULTIPLE_APP_URLS_FOR_OSTYPE = 30
        INVALID_OSTYPE = 31
        INVALID_PROTOCOL_FOR_APP_URL = 32
        INVALID_PACKAGE_ID_FOR_APP_URL = 33
        URL_CUSTOM_PARAMETERS_COUNT_EXCEEDS_LIMIT = 34
        INVALID_CHARACTERS_IN_URL_CUSTOM_PARAMETER_KEY = 39
        INVALID_CHARACTERS_IN_URL_CUSTOM_PARAMETER_VALUE = 40
        INVALID_TAG_IN_URL_CUSTOM_PARAMETER_VALUE = 41
        REDUNDANT_NESTED_URL_CUSTOM_PARAMETER_TAG = 42
        MISSING_PROTOCOL = 43
        INVALID_PROTOCOL = 52
        INVALID_URL = 44
        DESTINATION_URL_DEPRECATED = 45
        INVALID_TAG_IN_URL = 46
        MISSING_URL_TAG = 47
        DUPLICATE_URL_ID = 48
        INVALID_URL_ID = 49
        FINAL_URL_SUFFIX_MALFORMED = 50
        INVALID_TAG_IN_FINAL_URL_SUFFIX = 51
        INVALID_TOP_LEVEL_DOMAIN = 53
        MALFORMED_TOP_LEVEL_DOMAIN = 54
        MALFORMED_URL = 55
        MISSING_HOST = 56
        NULL_CUSTOM_PARAMETER_VALUE = 57
        VALUE_TRACK_PARAMETER_NOT_SUPPORTED = 58
        UNSUPPORTED_APP_STORE = 59


__all__ = tuple(sorted(__protobuf__.manifest))
