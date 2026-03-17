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
        "SkAdNetworkUserTypeEnum",
    },
)


class SkAdNetworkUserTypeEnum(proto.Message):
    r"""Container for enumeration of SkAdNetwork user types."""

    class SkAdNetworkUserType(proto.Enum):
        r"""Enumerates SkAdNetwork user types

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                The value is unknown in this version.
            UNAVAILABLE (2):
                The value was not present in the postback or
                we do not have this data for other reasons.
            NEW_INSTALLER (3):
                The user installed the app for the first
                time.
            REINSTALLER (4):
                The user has previously installed the app.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        UNAVAILABLE = 2
        NEW_INSTALLER = 3
        REINSTALLER = 4


__all__ = tuple(sorted(__protobuf__.manifest))
