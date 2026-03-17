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
        "ConversionActionTypeEnum",
    },
)


class ConversionActionTypeEnum(proto.Message):
    r"""Container for enum describing possible types of a conversion
    action.

    """

    class ConversionActionType(proto.Enum):
        r"""Possible types of a conversion action.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            AD_CALL (2):
                Conversions that occur when a user clicks on
                an ad's call extension.
            CLICK_TO_CALL (3):
                Conversions that occur when a user on a
                mobile device clicks a phone number.
            GOOGLE_PLAY_DOWNLOAD (4):
                Conversions that occur when a user downloads
                a mobile app from the Google Play Store.
            GOOGLE_PLAY_IN_APP_PURCHASE (5):
                Conversions that occur when a user makes a
                purchase in an app through Android billing.
            UPLOAD_CALLS (6):
                Call conversions that are tracked by the
                advertiser and uploaded.
            UPLOAD_CLICKS (7):
                Conversions that are tracked by the
                advertiser and uploaded with attributed clicks.
            WEBPAGE (8):
                Conversions that occur on a webpage.
            WEBSITE_CALL (9):
                Conversions that occur when a user calls a
                dynamically-generated phone number from an
                advertiser's website.
            STORE_SALES_DIRECT_UPLOAD (10):
                Store Sales conversion based on first-party
                or third-party merchant data uploads.
                Only customers on the allowlist can use store
                sales direct upload types.
            STORE_SALES (11):
                Store Sales conversion based on first-party
                or third-party merchant data uploads and/or from
                in-store purchases using cards from payment
                networks.
                Only customers on the allowlist can use store
                sales types. Read only.
            FIREBASE_ANDROID_FIRST_OPEN (12):
                Android app first open conversions tracked
                through Firebase.
            FIREBASE_ANDROID_IN_APP_PURCHASE (13):
                Android app in app purchase conversions
                tracked through Firebase.
            FIREBASE_ANDROID_CUSTOM (14):
                Android app custom conversions tracked
                through Firebase.
            FIREBASE_IOS_FIRST_OPEN (15):
                iOS app first open conversions tracked
                through Firebase.
            FIREBASE_IOS_IN_APP_PURCHASE (16):
                iOS app in app purchase conversions tracked
                through Firebase.
            FIREBASE_IOS_CUSTOM (17):
                iOS app custom conversions tracked through
                Firebase.
            THIRD_PARTY_APP_ANALYTICS_ANDROID_FIRST_OPEN (18):
                Android app first open conversions tracked
                through Third Party App Analytics.
            THIRD_PARTY_APP_ANALYTICS_ANDROID_IN_APP_PURCHASE (19):
                Android app in app purchase conversions
                tracked through Third Party App Analytics.
            THIRD_PARTY_APP_ANALYTICS_ANDROID_CUSTOM (20):
                Android app custom conversions tracked
                through Third Party App Analytics.
            THIRD_PARTY_APP_ANALYTICS_IOS_FIRST_OPEN (21):
                iOS app first open conversions tracked
                through Third Party App Analytics.
            THIRD_PARTY_APP_ANALYTICS_IOS_IN_APP_PURCHASE (22):
                iOS app in app purchase conversions tracked
                through Third Party App Analytics.
            THIRD_PARTY_APP_ANALYTICS_IOS_CUSTOM (23):
                iOS app custom conversions tracked through
                Third Party App Analytics.
            ANDROID_APP_PRE_REGISTRATION (24):
                Conversions that occur when a user
                pre-registers a mobile app from the Google Play
                Store. Read only.
            ANDROID_INSTALLS_ALL_OTHER_APPS (25):
                Conversions that track all Google Play
                downloads which aren't tracked by an
                app-specific type. Read only.
            FLOODLIGHT_ACTION (26):
                Floodlight activity that counts the number of
                times that users have visited a particular
                webpage after seeing or clicking on one of an
                advertiser's ads. Read only.
            FLOODLIGHT_TRANSACTION (27):
                Floodlight activity that tracks the number of
                sales made or the number of items purchased. Can
                also capture the total value of each sale. Read
                only.
            GOOGLE_HOSTED (28):
                Conversions that track local actions from
                Google's products and services after interacting
                with an ad. Read only.
            LEAD_FORM_SUBMIT (29):
                Conversions reported when a user submits a
                lead form. Read only.
            SALESFORCE (30):
                Deprecated: The Salesforce integration will
                be going away and replaced with an improved way
                to import your conversions from Salesforce.
                - see
                  https://support.google.com/google-ads/answer/14728349
            SEARCH_ADS_360 (31):
                Conversions imported from Search Ads 360
                Floodlight data. Read only.
            SMART_CAMPAIGN_AD_CLICKS_TO_CALL (32):
                Call conversions that occur on Smart campaign
                Ads without call tracking setup, using Smart
                campaign custom criteria. Read only.
            SMART_CAMPAIGN_MAP_CLICKS_TO_CALL (33):
                The user clicks on a call element within
                Google Maps. Smart campaign only. Read only.
            SMART_CAMPAIGN_MAP_DIRECTIONS (34):
                The user requests directions to a business
                location within Google Maps. Smart campaign
                only. Read only.
            SMART_CAMPAIGN_TRACKED_CALLS (35):
                Call conversions that occur on Smart campaign
                Ads with call tracking setup, using Smart
                campaign custom criteria. Read only.
            STORE_VISITS (36):
                Conversions that occur when a user visits an
                advertiser's retail store. Read only.
            WEBPAGE_CODELESS (37):
                Conversions created from website events (such
                as form submissions or page loads), that don't
                use individually coded event snippets. Read
                only.
            UNIVERSAL_ANALYTICS_GOAL (38):
                Conversions that come from linked Universal
                Analytics goals.
            UNIVERSAL_ANALYTICS_TRANSACTION (39):
                Conversions that come from linked Universal
                Analytics transactions.
            GOOGLE_ANALYTICS_4_CUSTOM (40):
                Conversions that come from linked Google
                Analytics 4 custom event conversions.
            GOOGLE_ANALYTICS_4_PURCHASE (41):
                Conversions that come from linked Google
                Analytics 4 purchase conversions.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        AD_CALL = 2
        CLICK_TO_CALL = 3
        GOOGLE_PLAY_DOWNLOAD = 4
        GOOGLE_PLAY_IN_APP_PURCHASE = 5
        UPLOAD_CALLS = 6
        UPLOAD_CLICKS = 7
        WEBPAGE = 8
        WEBSITE_CALL = 9
        STORE_SALES_DIRECT_UPLOAD = 10
        STORE_SALES = 11
        FIREBASE_ANDROID_FIRST_OPEN = 12
        FIREBASE_ANDROID_IN_APP_PURCHASE = 13
        FIREBASE_ANDROID_CUSTOM = 14
        FIREBASE_IOS_FIRST_OPEN = 15
        FIREBASE_IOS_IN_APP_PURCHASE = 16
        FIREBASE_IOS_CUSTOM = 17
        THIRD_PARTY_APP_ANALYTICS_ANDROID_FIRST_OPEN = 18
        THIRD_PARTY_APP_ANALYTICS_ANDROID_IN_APP_PURCHASE = 19
        THIRD_PARTY_APP_ANALYTICS_ANDROID_CUSTOM = 20
        THIRD_PARTY_APP_ANALYTICS_IOS_FIRST_OPEN = 21
        THIRD_PARTY_APP_ANALYTICS_IOS_IN_APP_PURCHASE = 22
        THIRD_PARTY_APP_ANALYTICS_IOS_CUSTOM = 23
        ANDROID_APP_PRE_REGISTRATION = 24
        ANDROID_INSTALLS_ALL_OTHER_APPS = 25
        FLOODLIGHT_ACTION = 26
        FLOODLIGHT_TRANSACTION = 27
        GOOGLE_HOSTED = 28
        LEAD_FORM_SUBMIT = 29
        SALESFORCE = 30
        SEARCH_ADS_360 = 31
        SMART_CAMPAIGN_AD_CLICKS_TO_CALL = 32
        SMART_CAMPAIGN_MAP_CLICKS_TO_CALL = 33
        SMART_CAMPAIGN_MAP_DIRECTIONS = 34
        SMART_CAMPAIGN_TRACKED_CALLS = 35
        STORE_VISITS = 36
        WEBPAGE_CODELESS = 37
        UNIVERSAL_ANALYTICS_GOAL = 38
        UNIVERSAL_ANALYTICS_TRANSACTION = 39
        GOOGLE_ANALYTICS_4_CUSTOM = 40
        GOOGLE_ANALYTICS_4_PURCHASE = 41


__all__ = tuple(sorted(__protobuf__.manifest))
