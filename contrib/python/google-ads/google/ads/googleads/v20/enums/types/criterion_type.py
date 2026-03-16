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
        "CriterionTypeEnum",
    },
)


class CriterionTypeEnum(proto.Message):
    r"""The possible types of a criterion."""

    class CriterionType(proto.Enum):
        r"""Enum describing possible criterion types.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            KEYWORD (2):
                Keyword, for example, 'mars cruise'.
            PLACEMENT (3):
                Placement, also known as Website, for
                example, 'www.flowers4sale.com'
            MOBILE_APP_CATEGORY (4):
                Mobile application categories to target.
            MOBILE_APPLICATION (5):
                Mobile applications to target.
            DEVICE (6):
                Devices to target.
            LOCATION (7):
                Locations to target.
            LISTING_GROUP (8):
                Listing groups to target.
            AD_SCHEDULE (9):
                Ad Schedule.
            AGE_RANGE (10):
                Age range.
            GENDER (11):
                Gender.
            INCOME_RANGE (12):
                Income Range.
            PARENTAL_STATUS (13):
                Parental status.
            YOUTUBE_VIDEO (14):
                YouTube Video.
            YOUTUBE_CHANNEL (15):
                YouTube Channel.
            USER_LIST (16):
                User list.
            PROXIMITY (17):
                Proximity.
            TOPIC (18):
                A topic target on the display network (for
                example, "Pets & Animals").
            LISTING_SCOPE (19):
                Listing scope to target.
            LANGUAGE (20):
                Language.
            IP_BLOCK (21):
                IpBlock.
            CONTENT_LABEL (22):
                Content Label for category exclusion.
            CARRIER (23):
                Carrier.
            USER_INTEREST (24):
                A category the user is interested in.
            WEBPAGE (25):
                Webpage criterion for dynamic search ads.
            OPERATING_SYSTEM_VERSION (26):
                Operating system version.
            APP_PAYMENT_MODEL (27):
                App payment model.
            MOBILE_DEVICE (28):
                Mobile device.
            CUSTOM_AFFINITY (29):
                Custom affinity.
            CUSTOM_INTENT (30):
                Custom intent.
            LOCATION_GROUP (31):
                Location group.
            CUSTOM_AUDIENCE (32):
                Custom audience
            COMBINED_AUDIENCE (33):
                Combined audience
            KEYWORD_THEME (34):
                Smart Campaign keyword theme
            AUDIENCE (35):
                Audience
            NEGATIVE_KEYWORD_LIST (36):
                Negative Keyword List
            LOCAL_SERVICE_ID (37):
                Local Services Ads Service ID.
            SEARCH_THEME (38):
                Search Theme.
            BRAND (39):
                Brand
            BRAND_LIST (40):
                Brand List
            LIFE_EVENT (41):
                Life Event
            WEBPAGE_LIST (42):
                Webpage List
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        KEYWORD = 2
        PLACEMENT = 3
        MOBILE_APP_CATEGORY = 4
        MOBILE_APPLICATION = 5
        DEVICE = 6
        LOCATION = 7
        LISTING_GROUP = 8
        AD_SCHEDULE = 9
        AGE_RANGE = 10
        GENDER = 11
        INCOME_RANGE = 12
        PARENTAL_STATUS = 13
        YOUTUBE_VIDEO = 14
        YOUTUBE_CHANNEL = 15
        USER_LIST = 16
        PROXIMITY = 17
        TOPIC = 18
        LISTING_SCOPE = 19
        LANGUAGE = 20
        IP_BLOCK = 21
        CONTENT_LABEL = 22
        CARRIER = 23
        USER_INTEREST = 24
        WEBPAGE = 25
        OPERATING_SYSTEM_VERSION = 26
        APP_PAYMENT_MODEL = 27
        MOBILE_DEVICE = 28
        CUSTOM_AFFINITY = 29
        CUSTOM_INTENT = 30
        LOCATION_GROUP = 31
        CUSTOM_AUDIENCE = 32
        COMBINED_AUDIENCE = 33
        KEYWORD_THEME = 34
        AUDIENCE = 35
        NEGATIVE_KEYWORD_LIST = 36
        LOCAL_SERVICE_ID = 37
        SEARCH_THEME = 38
        BRAND = 39
        BRAND_LIST = 40
        LIFE_EVENT = 41
        WEBPAGE_LIST = 42


__all__ = tuple(sorted(__protobuf__.manifest))
