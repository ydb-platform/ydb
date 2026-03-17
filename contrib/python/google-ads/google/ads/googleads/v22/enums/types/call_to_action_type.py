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
        "CallToActionTypeEnum",
    },
)


class CallToActionTypeEnum(proto.Message):
    r"""Container for enum describing the call to action types."""

    class CallToActionType(proto.Enum):
        r"""Enum describing possible types of call to action.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            LEARN_MORE (2):
                The call to action type is learn more.
            GET_QUOTE (3):
                The call to action type is get quote.
            APPLY_NOW (4):
                The call to action type is apply now.
            SIGN_UP (5):
                The call to action type is sign up.
            CONTACT_US (6):
                The call to action type is contact us.
            SUBSCRIBE (7):
                The call to action type is subscribe.
            DOWNLOAD (8):
                The call to action type is download.
            BOOK_NOW (9):
                The call to action type is book now.
            SHOP_NOW (10):
                The call to action type is shop now.
            BUY_NOW (11):
                The call to action type is buy now.
            DONATE_NOW (12):
                The call to action type is donate now.
            ORDER_NOW (13):
                The call to action type is order now.
            PLAY_NOW (14):
                The call to action type is play now.
            SEE_MORE (15):
                The call to action type is see more.
            START_NOW (16):
                The call to action type is start now.
            VISIT_SITE (17):
                The call to action type is visit site.
            WATCH_NOW (18):
                The call to action type is watch now.
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
        SHOP_NOW = 10
        BUY_NOW = 11
        DONATE_NOW = 12
        ORDER_NOW = 13
        PLAY_NOW = 14
        SEE_MORE = 15
        START_NOW = 16
        VISIT_SITE = 17
        WATCH_NOW = 18


__all__ = tuple(sorted(__protobuf__.manifest))
