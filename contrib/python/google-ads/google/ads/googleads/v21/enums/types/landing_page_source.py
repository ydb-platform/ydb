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
    package="google.ads.googleads.v21.enums",
    marshal="google.ads.googleads.v21",
    manifest={
        "LandingPageSourceEnum",
    },
)


class LandingPageSourceEnum(proto.Message):
    r"""Container for enum describing the source of a landing page in
    the landing page report.

    """

    class LandingPageSource(proto.Enum):
        r"""The source of a landing page in the landing page report.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            ADVERTISER (2):
                The landing page was explicitly provided by
                the advertiser.
            AUTOMATIC (3):
                The landing page was selected automatically.
                This could happen when the advertiser enables AI
                Max or other features that automatically select
                landing pages and Google selects the best
                landing page for the query.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        ADVERTISER = 2
        AUTOMATIC = 3


__all__ = tuple(sorted(__protobuf__.manifest))
