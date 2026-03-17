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
        "ContextErrorEnum",
    },
)


class ContextErrorEnum(proto.Message):
    r"""Container for enum describing possible context errors."""

    class ContextError(proto.Enum):
        r"""Enum describing possible context errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            OPERATION_NOT_PERMITTED_FOR_CONTEXT (2):
                The operation is not allowed for the given
                context.
            OPERATION_NOT_PERMITTED_FOR_REMOVED_RESOURCE (3):
                The operation is not allowed for removed
                resources.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        OPERATION_NOT_PERMITTED_FOR_CONTEXT = 2
        OPERATION_NOT_PERMITTED_FOR_REMOVED_RESOURCE = 3


__all__ = tuple(sorted(__protobuf__.manifest))
