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
        "PolicyTopicEvidenceDestinationMismatchUrlTypeEnum",
    },
)


class PolicyTopicEvidenceDestinationMismatchUrlTypeEnum(proto.Message):
    r"""Container for enum describing possible policy topic evidence
    destination mismatch url types.

    """

    class PolicyTopicEvidenceDestinationMismatchUrlType(proto.Enum):
        r"""The possible policy topic evidence destination mismatch url
        types.

        Values:
            UNSPECIFIED (0):
                No value has been specified.
            UNKNOWN (1):
                The received value is not known in this
                version.
                This is a response-only value.
            DISPLAY_URL (2):
                The display url.
            FINAL_URL (3):
                The final url.
            FINAL_MOBILE_URL (4):
                The final mobile url.
            TRACKING_URL (5):
                The tracking url template, with substituted
                desktop url.
            MOBILE_TRACKING_URL (6):
                The tracking url template, with substituted
                mobile url.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        DISPLAY_URL = 2
        FINAL_URL = 3
        FINAL_MOBILE_URL = 4
        TRACKING_URL = 5
        MOBILE_TRACKING_URL = 6


__all__ = tuple(sorted(__protobuf__.manifest))
