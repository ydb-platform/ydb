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
        "LocalServicesVerificationArtifactStatusEnum",
    },
)


class LocalServicesVerificationArtifactStatusEnum(proto.Message):
    r"""Container for enum describing the status of local services
    verification artifact.

    """

    class LocalServicesVerificationArtifactStatus(proto.Enum):
        r"""Enums describing statuses of a local services verification
        artifact.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            PASSED (2):
                Artifact has passed verification.
            FAILED (3):
                Artifact has failed verification.
            PENDING (4):
                Artifact is in the process of verification.
            NO_SUBMISSION (5):
                Artifact needs user to upload information
                before it is verified.
            CANCELLED (6):
                Artifact has been cancelled by the user.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        PASSED = 2
        FAILED = 3
        PENDING = 4
        NO_SUBMISSION = 5
        CANCELLED = 6


__all__ = tuple(sorted(__protobuf__.manifest))
