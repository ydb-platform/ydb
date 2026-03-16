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
    package="google.ads.googleads.v20.errors",
    marshal="google.ads.googleads.v20",
    manifest={
        "AssetGroupSignalErrorEnum",
    },
)


class AssetGroupSignalErrorEnum(proto.Message):
    r"""Container for enum describing possible asset group signal
    errors.

    """

    class AssetGroupSignalError(proto.Enum):
        r"""Enum describing possible asset group signal errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            TOO_MANY_WORDS (2):
                The number of words in the Search Theme
                signal exceed the allowed maximum. You can add
                up to 10 words in a keyword. See
                https://support.google.com/google-ads/answer/7476658
                for details.
            SEARCH_THEME_POLICY_VIOLATION (3):
                The search theme requested to be added
                violates certain policy. See
                https://support.google.com/adspolicy/answer/6008942.
            AUDIENCE_WITH_WRONG_ASSET_GROUP_ID (4):
                The asset group referenced by the asset group
                signal does not match the asset group referenced
                by the audience being used in the asset group
                signal.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        TOO_MANY_WORDS = 2
        SEARCH_THEME_POLICY_VIOLATION = 3
        AUDIENCE_WITH_WRONG_ASSET_GROUP_ID = 4


__all__ = tuple(sorted(__protobuf__.manifest))
