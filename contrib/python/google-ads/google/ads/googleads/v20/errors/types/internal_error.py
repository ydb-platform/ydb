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
        "InternalErrorEnum",
    },
)


class InternalErrorEnum(proto.Message):
    r"""Container for enum describing possible internal errors."""

    class InternalError(proto.Enum):
        r"""Enum describing possible internal errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            INTERNAL_ERROR (2):
                Google Ads API encountered unexpected
                internal error.
            ERROR_CODE_NOT_PUBLISHED (3):
                The intended error code doesn't exist in
                specified API version. It will be released in a
                future API version.
            TRANSIENT_ERROR (4):
                Google Ads API encountered an unexpected
                transient error. The user should retry their
                request in these cases.
            DEADLINE_EXCEEDED (5):
                The request took longer than a deadline.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        INTERNAL_ERROR = 2
        ERROR_CODE_NOT_PUBLISHED = 3
        TRANSIENT_ERROR = 4
        DEADLINE_EXCEEDED = 5


__all__ = tuple(sorted(__protobuf__.manifest))
