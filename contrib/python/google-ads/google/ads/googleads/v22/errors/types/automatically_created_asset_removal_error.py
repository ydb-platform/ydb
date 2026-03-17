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
        "AutomaticallyCreatedAssetRemovalErrorEnum",
    },
)


class AutomaticallyCreatedAssetRemovalErrorEnum(proto.Message):
    r"""Container for enum describing possible automatically created
    asset removal errors.

    """

    class AutomaticallyCreatedAssetRemovalError(proto.Enum):
        r"""Enum describing possible automatically created asset removal
        errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            AD_DOES_NOT_EXIST (2):
                The ad does not exist.
            INVALID_AD_TYPE (3):
                Ad type is not supported. Only Responsive
                Search Ad type is supported.
            ASSET_DOES_NOT_EXIST (4):
                The asset does not exist.
            ASSET_FIELD_TYPE_DOES_NOT_MATCH (5):
                The asset field type does not match.
            NOT_AN_AUTOMATICALLY_CREATED_ASSET (6):
                Not an automatically created asset.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        AD_DOES_NOT_EXIST = 2
        INVALID_AD_TYPE = 3
        ASSET_DOES_NOT_EXIST = 4
        ASSET_FIELD_TYPE_DOES_NOT_MATCH = 5
        NOT_AN_AUTOMATICALLY_CREATED_ASSET = 6


__all__ = tuple(sorted(__protobuf__.manifest))
