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
        "UserListCrmDataSourceTypeEnum",
    },
)


class UserListCrmDataSourceTypeEnum(proto.Message):
    r"""Indicates source of Crm upload data."""

    class UserListCrmDataSourceType(proto.Enum):
        r"""Enum describing possible user list crm data source type.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            FIRST_PARTY (2):
                The uploaded data is first-party data.
            THIRD_PARTY_CREDIT_BUREAU (3):
                The uploaded data is from a third-party
                credit bureau.
            THIRD_PARTY_VOTER_FILE (4):
                The uploaded data is from a third-party voter
                file.
            THIRD_PARTY_PARTNER_DATA (5):
                The uploaded data is third party partner
                data.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        FIRST_PARTY = 2
        THIRD_PARTY_CREDIT_BUREAU = 3
        THIRD_PARTY_VOTER_FILE = 4
        THIRD_PARTY_PARTNER_DATA = 5


__all__ = tuple(sorted(__protobuf__.manifest))
