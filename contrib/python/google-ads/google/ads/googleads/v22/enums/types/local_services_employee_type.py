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
    package="google.ads.googleads.v22.enums",
    marshal="google.ads.googleads.v22",
    manifest={
        "LocalServicesEmployeeTypeEnum",
    },
)


class LocalServicesEmployeeTypeEnum(proto.Message):
    r"""Container for enum describing the types of local services
    employee.

    """

    class LocalServicesEmployeeType(proto.Enum):
        r"""Enums describing types of a local services employee.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            BUSINESS_OWNER (2):
                Represents the owner of the business.
            EMPLOYEE (3):
                Represents an employee of the business.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        BUSINESS_OWNER = 2
        EMPLOYEE = 3


__all__ = tuple(sorted(__protobuf__.manifest))
