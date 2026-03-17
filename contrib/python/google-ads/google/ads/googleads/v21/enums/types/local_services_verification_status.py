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
    package="google.ads.googleads.v21.enums",
    marshal="google.ads.googleads.v21",
    manifest={
        "LocalServicesVerificationStatusEnum",
    },
)


class LocalServicesVerificationStatusEnum(proto.Message):
    r"""Container for enum describing status of a particular Local
    Services Ads verification category.

    """

    class LocalServicesVerificationStatus(proto.Enum):
        r"""Enum describing status of a particular Local Services Ads
        verification category.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Unknown verification status.
            NEEDS_REVIEW (2):
                Verification has started, but has not
                finished.
            FAILED (3):
                Verification has failed.
            PASSED (4):
                Verification has passed.
            NOT_APPLICABLE (5):
                Verification is not applicable.
            NO_SUBMISSION (6):
                Verification is required but pending
                submission.
            PARTIAL_SUBMISSION (7):
                Not all required verification has been
                submitted.
            PENDING_ESCALATION (8):
                Verification needs review by Local Services
                Ads Ops Specialist.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        NEEDS_REVIEW = 2
        FAILED = 3
        PASSED = 4
        NOT_APPLICABLE = 5
        NO_SUBMISSION = 6
        PARTIAL_SUBMISSION = 7
        PENDING_ESCALATION = 8


__all__ = tuple(sorted(__protobuf__.manifest))
