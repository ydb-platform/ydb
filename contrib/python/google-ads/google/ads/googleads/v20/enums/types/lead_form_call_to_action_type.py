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
        "LeadFormCallToActionTypeEnum",
    },
)


class LeadFormCallToActionTypeEnum(proto.Message):
    r"""Describes the type of call-to-action phrases in a lead form."""

    class LeadFormCallToActionType(proto.Enum):
        r"""Enum describing the type of call-to-action phrases in a lead
        form.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            LEARN_MORE (2):
                Learn more.
            GET_QUOTE (3):
                Get quote.
            APPLY_NOW (4):
                Apply now.
            SIGN_UP (5):
                Sign Up.
            CONTACT_US (6):
                Contact us.
            SUBSCRIBE (7):
                Subscribe.
            DOWNLOAD (8):
                Download.
            BOOK_NOW (9):
                Book now.
            GET_OFFER (10):
                Get offer.
            REGISTER (11):
                Register.
            GET_INFO (12):
                Get info.
            REQUEST_DEMO (13):
                Request a demo.
            JOIN_NOW (14):
                Join now.
            GET_STARTED (15):
                Get started.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        LEARN_MORE = 2
        GET_QUOTE = 3
        APPLY_NOW = 4
        SIGN_UP = 5
        CONTACT_US = 6
        SUBSCRIBE = 7
        DOWNLOAD = 8
        BOOK_NOW = 9
        GET_OFFER = 10
        REGISTER = 11
        GET_INFO = 12
        REQUEST_DEMO = 13
        JOIN_NOW = 14
        GET_STARTED = 15


__all__ = tuple(sorted(__protobuf__.manifest))
