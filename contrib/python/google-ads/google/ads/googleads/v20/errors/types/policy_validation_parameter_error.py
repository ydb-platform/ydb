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
        "PolicyValidationParameterErrorEnum",
    },
)


class PolicyValidationParameterErrorEnum(proto.Message):
    r"""Container for enum describing possible policy validation
    parameter errors.

    """

    class PolicyValidationParameterError(proto.Enum):
        r"""Enum describing possible policy validation parameter errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            UNSUPPORTED_AD_TYPE_FOR_IGNORABLE_POLICY_TOPICS (2):
                Ignorable policy topics are not supported for
                the ad type.
            UNSUPPORTED_AD_TYPE_FOR_EXEMPT_POLICY_VIOLATION_KEYS (3):
                Exempt policy violation keys are not
                supported for the ad type.
            CANNOT_SET_BOTH_IGNORABLE_POLICY_TOPICS_AND_EXEMPT_POLICY_VIOLATION_KEYS (4):
                Cannot set ignorable policy topics and exempt
                policy violation keys in the same policy
                violation parameter.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        UNSUPPORTED_AD_TYPE_FOR_IGNORABLE_POLICY_TOPICS = 2
        UNSUPPORTED_AD_TYPE_FOR_EXEMPT_POLICY_VIOLATION_KEYS = 3
        CANNOT_SET_BOTH_IGNORABLE_POLICY_TOPICS_AND_EXEMPT_POLICY_VIOLATION_KEYS = (
            4
        )


__all__ = tuple(sorted(__protobuf__.manifest))
