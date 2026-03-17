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
    package="google.ads.googleads.v20.enums",
    marshal="google.ads.googleads.v20",
    manifest={
        "UserListPrepopulationStatusEnum",
    },
)


class UserListPrepopulationStatusEnum(proto.Message):
    r"""Indicates status of prepopulation based on the rule."""

    class UserListPrepopulationStatus(proto.Enum):
        r"""Enum describing possible user list prepopulation status.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            REQUESTED (2):
                Prepopoulation is being requested.
            FINISHED (3):
                Prepopulation is finished.
            FAILED (4):
                Prepopulation failed.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        REQUESTED = 2
        FINISHED = 3
        FAILED = 4


__all__ = tuple(sorted(__protobuf__.manifest))
