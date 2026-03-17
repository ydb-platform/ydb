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
        "TrackingCodeTypeEnum",
    },
)


class TrackingCodeTypeEnum(proto.Message):
    r"""Container for enum describing the type of the generated tag
    snippets for tracking conversions.

    """

    class TrackingCodeType(proto.Enum):
        r"""The type of the generated tag snippets for tracking
        conversions.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            WEBPAGE (2):
                The snippet that is fired as a result of a
                website page loading.
            WEBPAGE_ONCLICK (3):
                The snippet contains a JavaScript function
                which fires the tag. This function is typically
                called from an onClick handler added to a link
                or button element on the page.
            CLICK_TO_CALL (4):
                For embedding on a mobile webpage. The
                snippet contains a JavaScript function which
                fires the tag.
            WEBSITE_CALL (5):
                The snippet that is used to replace the phone
                number on your website with a Google forwarding
                number for call tracking purposes.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        WEBPAGE = 2
        WEBPAGE_ONCLICK = 3
        CLICK_TO_CALL = 4
        WEBSITE_CALL = 5


__all__ = tuple(sorted(__protobuf__.manifest))
