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
        "AudienceScopeEnum",
    },
)


class AudienceScopeEnum(proto.Message):
    r"""Defines the scope an audience can be used in."""

    class AudienceScope(proto.Enum):
        r"""Enum containing possible audience scope values.

        Values:
            UNSPECIFIED (0):
                The scope has not been specified.
            UNKNOWN (1):
                The received value is not known in this
                version.
            CUSTOMER (2):
                The audience is scoped at the customer level.
            ASSET_GROUP (3):
                The audience is scoped under a specific
                AssetGroup.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        CUSTOMER = 2
        ASSET_GROUP = 3


__all__ = tuple(sorted(__protobuf__.manifest))
