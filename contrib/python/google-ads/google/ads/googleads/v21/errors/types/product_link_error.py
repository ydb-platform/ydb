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
        "ProductLinkErrorEnum",
    },
)


class ProductLinkErrorEnum(proto.Message):
    r"""Container for enum describing possible ProductLink errors."""

    class ProductLinkError(proto.Enum):
        r"""Enum describing possible ProductLink errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            INVALID_OPERATION (2):
                The requested operation is invalid. For
                example, you are not allowed to remove a link
                from a partner account.
            CREATION_NOT_PERMITTED (3):
                The creation request is not permitted.
            INVITATION_EXISTS (4):
                A link cannot be created because a pending
                link already exists.
            LINK_EXISTS (5):
                A link cannot be created because an active
                link already exists.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        INVALID_OPERATION = 2
        CREATION_NOT_PERMITTED = 3
        INVITATION_EXISTS = 4
        LINK_EXISTS = 5


__all__ = tuple(sorted(__protobuf__.manifest))
