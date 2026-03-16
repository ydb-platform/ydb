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
        "BrandRequestRejectionReasonEnum",
    },
)


class BrandRequestRejectionReasonEnum(proto.Message):
    r"""Container for enum describing rejection reasons for the
    customer brand requests.

    """

    class BrandRequestRejectionReason(proto.Enum):
        r"""Enumeration of different brand request rejection reasons.

        Values:
            UNSPECIFIED (0):
                No value has been specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            EXISTING_BRAND (2):
                Brand is already present in the commercial
                brand set.
            EXISTING_BRAND_VARIANT (3):
                Brand is already present in the commercial
                brand set, but is a variant.
            INCORRECT_INFORMATION (4):
                Brand information is not correct (eg: URL and
                name don't match).
            NOT_A_BRAND (5):
                Not a valid brand as per Google policy.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        EXISTING_BRAND = 2
        EXISTING_BRAND_VARIANT = 3
        INCORRECT_INFORMATION = 4
        NOT_A_BRAND = 5


__all__ = tuple(sorted(__protobuf__.manifest))
