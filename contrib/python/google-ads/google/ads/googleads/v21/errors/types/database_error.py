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
        "DatabaseErrorEnum",
    },
)


class DatabaseErrorEnum(proto.Message):
    r"""Container for enum describing possible database errors."""

    class DatabaseError(proto.Enum):
        r"""Enum describing possible database errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            CONCURRENT_MODIFICATION (2):
                Multiple requests were attempting to modify
                the same resource at once. Retry the request.
            DATA_CONSTRAINT_VIOLATION (3):
                The request conflicted with existing data.
                This error will usually be replaced with a more
                specific error if the request is retried.
            REQUEST_TOO_LARGE (4):
                The data written is too large. Split the
                request into smaller requests.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        CONCURRENT_MODIFICATION = 2
        DATA_CONSTRAINT_VIOLATION = 3
        REQUEST_TOO_LARGE = 4


__all__ = tuple(sorted(__protobuf__.manifest))
