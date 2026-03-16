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
        "CampaignSharedSetErrorEnum",
    },
)


class CampaignSharedSetErrorEnum(proto.Message):
    r"""Container for enum describing possible campaign shared set
    errors.

    """

    class CampaignSharedSetError(proto.Enum):
        r"""Enum describing possible campaign shared set errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            SHARED_SET_ACCESS_DENIED (2):
                The shared set belongs to another customer
                and permission isn't granted.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        SHARED_SET_ACCESS_DENIED = 2


__all__ = tuple(sorted(__protobuf__.manifest))
