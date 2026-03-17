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
        "IdentityVerificationProgramEnum",
    },
)


class IdentityVerificationProgramEnum(proto.Message):
    r"""Container for IdentityVerificationProgram."""

    class IdentityVerificationProgram(proto.Enum):
        r"""Type of identity verification program.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                The value is unknown in this version.
            ADVERTISER_IDENTITY_VERIFICATION (2):
                Advertiser submits documents to verify their
                identity.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        ADVERTISER_IDENTITY_VERIFICATION = 2


__all__ = tuple(sorted(__protobuf__.manifest))
