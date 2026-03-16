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
        "ExternalConversionSourceEnum",
    },
)


class ExternalConversionSourceEnum(proto.Message):
    r"""Container for enum describing the external conversion source
    that is associated with a ConversionAction.

    """

    class ExternalConversionSource(proto.Enum):
        r"""The external conversion source that is associated with a
        ConversionAction.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Represents value unknown in this version.
            WEBPAGE (2):
                Conversion that occurs when a user navigates
                to a particular webpage after viewing an ad;
                Displayed in Google Ads UI as 'Website'.
            ANALYTICS (3):
                Conversion that comes from linked Google
                Analytics goal or transaction; Displayed in
                Google Ads UI as 'Analytics'.
            UPLOAD (4):
                Website conversion that is uploaded through
                ConversionUploadService; Displayed in Google Ads
                UI as 'Import from clicks'.
            AD_CALL_METRICS (5):
                Conversion that occurs when a user clicks on
                a call extension directly on an ad; Displayed in
                Google Ads UI as 'Calls from ads'.
            WEBSITE_CALL_METRICS (6):
                Conversion that occurs when a user calls a
                dynamically-generated phone number (by installed
                javascript) from an advertiser's website after
                clicking on an ad; Displayed in Google Ads UI as
                'Calls from website'.
            STORE_VISITS (7):
                Conversion that occurs when a user visits an
                advertiser's retail store after clicking on a
                Google ad; Displayed in Google Ads UI as 'Store
                visits'.
            ANDROID_IN_APP (8):
                Conversion that occurs when a user takes an
                in-app action such as a purchase in an Android
                app; Displayed in Google Ads UI as 'Android
                in-app action'.
            IOS_IN_APP (9):
                Conversion that occurs when a user takes an
                in-app action such as a purchase in an iOS app;
                Displayed in Google Ads UI as 'iOS in-app
                action'.
            IOS_FIRST_OPEN (10):
                Conversion that occurs when a user opens an
                iOS app for the first time; Displayed in Google
                Ads UI as 'iOS app install (first open)'.
            APP_UNSPECIFIED (11):
                Legacy app conversions that do not have an
                AppPlatform provided; Displayed in Google Ads UI
                as 'Mobile app'.
            ANDROID_FIRST_OPEN (12):
                Conversion that occurs when a user opens an
                Android app for the first time; Displayed in
                Google Ads UI as 'Android app install (first
                open)'.
            UPLOAD_CALLS (13):
                Call conversion that is uploaded through
                ConversionUploadService; Displayed in Google Ads
                UI as 'Import from calls'.
            FIREBASE (14):
                Conversion that comes from a linked Firebase
                event; Displayed in Google Ads UI as 'Firebase'.
            CLICK_TO_CALL (15):
                Conversion that occurs when a user clicks on
                a mobile phone number; Displayed in Google Ads
                UI as 'Phone number clicks'.
            SALESFORCE (16):
                Conversion that comes from Salesforce;
                Displayed in Google Ads UI as 'Salesforce.com'.
            STORE_SALES_CRM (17):
                Conversion that comes from in-store purchases
                recorded by CRM; Displayed in Google Ads UI as
                'Store sales (data partner)'.
            STORE_SALES_PAYMENT_NETWORK (18):
                Conversion that comes from in-store purchases
                from payment network; Displayed in Google Ads UI
                as 'Store sales (payment network)'.
            GOOGLE_PLAY (19):
                Codeless Google Play conversion;
                Displayed in Google Ads UI as 'Google Play'.
            THIRD_PARTY_APP_ANALYTICS (20):
                Conversion that comes from a linked
                third-party app analytics event; Displayed in
                Google Ads UI as 'Third-party app analytics'.
            GOOGLE_ATTRIBUTION (21):
                Conversion that is controlled by Google
                Attribution.
            STORE_SALES_DIRECT_UPLOAD (23):
                Store Sales conversion based on first-party
                or third-party merchant data uploads. Displayed
                in Google Ads UI as 'Store sales (direct
                upload)'.
            STORE_SALES (24):
                Store Sales conversion based on first-party
                or third-party merchant data uploads and/or from
                in-store purchases using cards from payment
                networks. Displayed in Google Ads UI as 'Store
                sales'.
            SEARCH_ADS_360 (25):
                Conversions imported from Search Ads 360
                Floodlight data.
            GOOGLE_HOSTED (27):
                Conversions that track local actions from
                Google's products and services after interacting
                with an ad.
            FLOODLIGHT (29):
                Conversions reported by Floodlight tags.
            ANALYTICS_SEARCH_ADS_360 (31):
                Conversions that come from Google Analytics
                specifically for Search Ads
                360. Displayed in Google Ads UI as Analytics
                    (SA360).
            FIREBASE_SEARCH_ADS_360 (33):
                Conversion that comes from a linked Firebase
                event for Search Ads 360.
            DISPLAY_AND_VIDEO_360_FLOODLIGHT (34):
                Conversion that is reported by Floodlight for
                DV360.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        WEBPAGE = 2
        ANALYTICS = 3
        UPLOAD = 4
        AD_CALL_METRICS = 5
        WEBSITE_CALL_METRICS = 6
        STORE_VISITS = 7
        ANDROID_IN_APP = 8
        IOS_IN_APP = 9
        IOS_FIRST_OPEN = 10
        APP_UNSPECIFIED = 11
        ANDROID_FIRST_OPEN = 12
        UPLOAD_CALLS = 13
        FIREBASE = 14
        CLICK_TO_CALL = 15
        SALESFORCE = 16
        STORE_SALES_CRM = 17
        STORE_SALES_PAYMENT_NETWORK = 18
        GOOGLE_PLAY = 19
        THIRD_PARTY_APP_ANALYTICS = 20
        GOOGLE_ATTRIBUTION = 21
        STORE_SALES_DIRECT_UPLOAD = 23
        STORE_SALES = 24
        SEARCH_ADS_360 = 25
        GOOGLE_HOSTED = 27
        FLOODLIGHT = 29
        ANALYTICS_SEARCH_ADS_360 = 31
        FIREBASE_SEARCH_ADS_360 = 33
        DISPLAY_AND_VIDEO_360_FLOODLIGHT = 34


__all__ = tuple(sorted(__protobuf__.manifest))
