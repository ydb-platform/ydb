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
        "AssetSetStatusEnum",
    },
)


class AssetSetStatusEnum(proto.Message):
    r"""Container for enum describing possible statuses of an asset
    set.

    """

    class AssetSetStatus(proto.Enum):
        r"""The possible statuses of an asset set.

        Values:
            UNSPECIFIED (0):
                The status has not been specified.
            UNKNOWN (1):
                The received value is not known in this
                version. This is a response-only value.
            ENABLED (2):
                The asset set is enabled.
            REMOVED (3):
                The asset set is removed.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        ENABLED = 2
        REMOVED = 3


__all__ = tuple(sorted(__protobuf__.manifest))
