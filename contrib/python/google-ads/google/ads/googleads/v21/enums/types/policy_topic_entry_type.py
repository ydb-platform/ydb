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
        "PolicyTopicEntryTypeEnum",
    },
)


class PolicyTopicEntryTypeEnum(proto.Message):
    r"""Container for enum describing possible policy topic entry
    types.

    """

    class PolicyTopicEntryType(proto.Enum):
        r"""The possible policy topic entry types.

        Values:
            UNSPECIFIED (0):
                No value has been specified.
            UNKNOWN (1):
                The received value is not known in this
                version.
                This is a response-only value.
            PROHIBITED (2):
                The resource will not be served.
            LIMITED (4):
                The resource will not be served under some
                circumstances.
            FULLY_LIMITED (8):
                The resource cannot serve at all because of
                the current targeting criteria.
            DESCRIPTIVE (5):
                May be of interest, but does not limit how
                the resource is served.
            BROADENING (6):
                Could increase coverage beyond normal.
            AREA_OF_INTEREST_ONLY (7):
                Constrained for all targeted countries, but
                may serve in other countries through area of
                interest.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        PROHIBITED = 2
        LIMITED = 4
        FULLY_LIMITED = 8
        DESCRIPTIVE = 5
        BROADENING = 6
        AREA_OF_INTEREST_ONLY = 7


__all__ = tuple(sorted(__protobuf__.manifest))
