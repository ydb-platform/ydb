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
        "ConversionCustomVariableStatusEnum",
    },
)


class ConversionCustomVariableStatusEnum(proto.Message):
    r"""Container for enum describing possible statuses of a
    conversion custom variable.

    """

    class ConversionCustomVariableStatus(proto.Enum):
        r"""Possible statuses of a conversion custom variable.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            ACTIVATION_NEEDED (2):
                The conversion custom variable is pending
                activation and will not accrue stats until set
                to ENABLED.

                This status can't be used in CREATE and UPDATE
                requests.
            ENABLED (3):
                The conversion custom variable is enabled and
                will accrue stats.
            PAUSED (4):
                The conversion custom variable is paused and
                will not accrue stats until set to ENABLED
                again.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        ACTIVATION_NEEDED = 2
        ENABLED = 3
        PAUSED = 4


__all__ = tuple(sorted(__protobuf__.manifest))
