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
        "ChangeClientTypeEnum",
    },
)


class ChangeClientTypeEnum(proto.Message):
    r"""Container for enum describing the sources that the change
    event resource was made through.

    """

    class ChangeClientType(proto.Enum):
        r"""The source that the change_event resource was made through.

        Values:
            UNSPECIFIED (0):
                No value has been specified.
            UNKNOWN (1):
                Used for return value only. Represents an
                unclassified client type unknown in this
                version.
            GOOGLE_ADS_WEB_CLIENT (2):
                Changes made through the "ads.google.com".
                For example, changes made through campaign
                management.
            GOOGLE_ADS_AUTOMATED_RULE (3):
                Changes made through Google Ads automated
                rules.
            GOOGLE_ADS_SCRIPTS (4):
                Changes made through Google Ads scripts.
            GOOGLE_ADS_BULK_UPLOAD (5):
                Changes made by Google Ads bulk upload.
            GOOGLE_ADS_API (6):
                Changes made by Google Ads API.
            GOOGLE_ADS_EDITOR (7):
                Changes made by Google Ads Editor. This value
                is a placeholder. The API does not return these
                changes.
            GOOGLE_ADS_MOBILE_APP (8):
                Changes made by Google Ads mobile app.
            GOOGLE_ADS_RECOMMENDATIONS (9):
                Changes made through Google Ads
                recommendations.
            SEARCH_ADS_360_SYNC (10):
                Changes made through Search Ads 360 Sync.
            SEARCH_ADS_360_POST (11):
                Changes made through Search Ads 360 Post.
            INTERNAL_TOOL (12):
                Changes made through internal tools.
                For example, when a user sets a URL template on
                an entity like a Campaign, it's automatically
                wrapped with the SA360 Clickserver URL.
            OTHER (13):
                Types of changes that are not categorized,
                for example, changes made by coupon redemption
                through Google Ads.
            GOOGLE_ADS_RECOMMENDATIONS_SUBSCRIPTION (14):
                Changes made by subscribing to Google Ads
                recommendations.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        GOOGLE_ADS_WEB_CLIENT = 2
        GOOGLE_ADS_AUTOMATED_RULE = 3
        GOOGLE_ADS_SCRIPTS = 4
        GOOGLE_ADS_BULK_UPLOAD = 5
        GOOGLE_ADS_API = 6
        GOOGLE_ADS_EDITOR = 7
        GOOGLE_ADS_MOBILE_APP = 8
        GOOGLE_ADS_RECOMMENDATIONS = 9
        SEARCH_ADS_360_SYNC = 10
        SEARCH_ADS_360_POST = 11
        INTERNAL_TOOL = 12
        OTHER = 13
        GOOGLE_ADS_RECOMMENDATIONS_SUBSCRIPTION = 14


__all__ = tuple(sorted(__protobuf__.manifest))
