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
        "LocalServicesLeadConversationTypeEnum",
    },
)


class LocalServicesLeadConversationTypeEnum(proto.Message):
    r"""Container for enum describing possible types of lead
    conversation.

    """

    class ConversationType(proto.Enum):
        r"""Possible types of lead conversation.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            EMAIL (2):
                Email lead conversation.
            MESSAGE (3):
                Message lead conversation.
            PHONE_CALL (4):
                Phone call lead conversation.
            SMS (5):
                SMS lead conversation.
            BOOKING (6):
                Booking lead conversation.
            WHATSAPP (7):
                WhatsApp lead conversation.
            ADS_API (8):
                Lead conversation created through Google Ads
                API.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        EMAIL = 2
        MESSAGE = 3
        PHONE_CALL = 4
        SMS = 5
        BOOKING = 6
        WHATSAPP = 7
        ADS_API = 8


__all__ = tuple(sorted(__protobuf__.manifest))
