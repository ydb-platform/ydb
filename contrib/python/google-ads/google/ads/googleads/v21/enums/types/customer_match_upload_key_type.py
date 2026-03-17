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
        "CustomerMatchUploadKeyTypeEnum",
    },
)


class CustomerMatchUploadKeyTypeEnum(proto.Message):
    r"""Indicates what type of data are the user list's members
    matched from.

    """

    class CustomerMatchUploadKeyType(proto.Enum):
        r"""Enum describing possible customer match upload key types.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            CONTACT_INFO (2):
                Members are matched from customer info such
                as email address, phone number or physical
                address.
            CRM_ID (3):
                Members are matched from a user id generated
                and assigned by the advertiser.
            MOBILE_ADVERTISING_ID (4):
                Members are matched from mobile advertising
                ids.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        CONTACT_INFO = 2
        CRM_ID = 3
        MOBILE_ADVERTISING_ID = 4


__all__ = tuple(sorted(__protobuf__.manifest))
