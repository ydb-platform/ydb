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
        "KeywordPlanAdGroupErrorEnum",
    },
)


class KeywordPlanAdGroupErrorEnum(proto.Message):
    r"""Container for enum describing possible errors from applying a
    keyword plan ad group.

    """

    class KeywordPlanAdGroupError(proto.Enum):
        r"""Enum describing possible errors from applying a keyword plan
        ad group.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            INVALID_NAME (2):
                The keyword plan ad group name is missing,
                empty, longer than allowed limit or contains
                invalid chars.
            DUPLICATE_NAME (3):
                The keyword plan ad group name is duplicate
                to an existing keyword plan AdGroup name or
                other keyword plan AdGroup name in the request.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        INVALID_NAME = 2
        DUPLICATE_NAME = 3


__all__ = tuple(sorted(__protobuf__.manifest))
