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
    package="google.ads.googleads.v20.errors",
    marshal="google.ads.googleads.v20",
    manifest={
        "ConversionUploadErrorEnum",
    },
)


class ConversionUploadErrorEnum(proto.Message):
    r"""Container for enum describing possible conversion upload
    errors.

    """

    class ConversionUploadError(proto.Enum):
        r"""Enum describing possible conversion upload errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            TOO_MANY_CONVERSIONS_IN_REQUEST (2):
                Upload fewer than 2001 events in a single
                request.
            UNPARSEABLE_GCLID (3):
                The imported gclid could not be decoded.
            CONVERSION_PRECEDES_EVENT (42):
                The imported event has a ``conversion_date_time`` that
                precedes the click. Make sure your ``conversion_date_time``
                is correct and try again.
            EXPIRED_EVENT (43):
                The imported event can't be recorded because
                its click occurred before this conversion's
                click-through window. Make sure you import the
                most recent data.
            TOO_RECENT_EVENT (44):
                The click associated with the given
                identifier or iOS URL parameter occurred less
                than 6 hours ago. Retry after 6 hours have
                passed.
            EVENT_NOT_FOUND (45):
                The imported event could not be attributed to
                a click. This may be because the event did not
                come from a Google Ads campaign.
            UNAUTHORIZED_CUSTOMER (8):
                The click ID or call is associated with an
                Ads account you don't have access to. Make sure
                you import conversions for accounts managed by
                your manager account.
            TOO_RECENT_CONVERSION_ACTION (10):
                Can't import events to a conversion action
                that was just created. Try importing again in 6
                hours.
            CONVERSION_TRACKING_NOT_ENABLED_AT_IMPRESSION_TIME (11):
                At the time of the click, conversion tracking
                was not enabled in the effective conversion
                account of the click's Google Ads account.
            EXTERNAL_ATTRIBUTION_DATA_SET_FOR_NON_EXTERNALLY_ATTRIBUTED_CONVERSION_ACTION (12):
                The imported event includes external
                attribution data, but the conversion action
                isn't set up to use an external attribution
                model. Make sure the conversion action is
                correctly configured and try again.
            EXTERNAL_ATTRIBUTION_DATA_NOT_SET_FOR_EXTERNALLY_ATTRIBUTED_CONVERSION_ACTION (13):
                The conversion action is set up to use an
                external attribution model, but the imported
                event is missing data. Make sure imported events
                include the external attribution credit and all
                necessary fields.
            ORDER_ID_NOT_PERMITTED_FOR_EXTERNALLY_ATTRIBUTED_CONVERSION_ACTION (14):
                Order IDs can't be used for a conversion
                measured with an external attribution model.
                Make sure the conversion is correctly configured
                and imported events include only necessary data
                and try again.
            ORDER_ID_ALREADY_IN_USE (15):
                The imported event includes an order ID that
                was previously recorded, so the event was not
                processed.
            DUPLICATE_ORDER_ID (16):
                Imported events include multiple conversions
                with the same order ID and were not processed.
                Make sure order IDs are unique and try again.
            TOO_RECENT_CALL (17):
                Can't import calls that occurred less than 6
                hours ago. Try uploading again in 6 hours.
            EXPIRED_CALL (18):
                The call can't be recorded because it
                occurred before this conversion action's
                lookback window. Make sure your import is
                configured to get the most recent data.
            CALL_NOT_FOUND (19):
                The call or click leading to the imported
                event can't be found. Make sure your data source
                is set up to include correct identifiers.
            CONVERSION_PRECEDES_CALL (20):
                The call has a ``conversion_date_time`` that precedes the
                associated click. Make sure your ``conversion_date_time`` is
                correct.
            CONVERSION_TRACKING_NOT_ENABLED_AT_CALL_TIME (21):
                At the time of the imported call, conversion
                tracking was not enabled in the effective
                conversion account of the click's Google Ads
                account.
            UNPARSEABLE_CALLERS_PHONE_NUMBER (22):
                Make sure phone numbers are formatted as
                E.164 (+16502531234), International (+64 3-331
                6005), or US national number (6502531234).
            CLICK_CONVERSION_ALREADY_EXISTS (23):
                The imported event has the same click and
                ``conversion_date_time`` as an existing conversion. Use a
                unique ``conversion_date_time`` or order ID for each unique
                event and try again.
            CALL_CONVERSION_ALREADY_EXISTS (24):
                The imported call has the same ``conversion_date_time`` as
                an existing conversion. Make sure your
                ``conversion_date_time`` correctly configured and try again.
            DUPLICATE_CLICK_CONVERSION_IN_REQUEST (25):
                Multiple events have the same click and
                ``conversion_date_time``. Make sure your
                ``conversion_date_time`` is correctly configured and try
                again.
            DUPLICATE_CALL_CONVERSION_IN_REQUEST (26):
                Multiple events have the same call and
                ``conversion_date_time``. Make sure your
                ``conversion_date_time`` is correctly configured and try
                again.
            CUSTOM_VARIABLE_NOT_ENABLED (28):
                Enable the custom variable in your conversion
                settings and try again.
            CUSTOM_VARIABLE_VALUE_CONTAINS_PII (29):
                Can't import events with custom variables
                containing personally-identifiable information
                (PII). Remove these variables and try again.
            INVALID_CUSTOMER_FOR_CLICK (30):
                The click from the imported event is
                associated with a different Google Ads account.
                Make sure you're importing to the correct
                account.
            INVALID_CUSTOMER_FOR_CALL (31):
                The click from the call is associated with a different
                Google Ads account. Make sure you're importing to the
                correct account. Query
                conversion_tracking_setting.google_ads_conversion_customer
                on Customer to identify the correct account.
            CONVERSION_NOT_COMPLIANT_WITH_ATT_POLICY (32):
                The connversion can't be imported because the
                conversion source didn't comply with Apple App
                Transparency Tracking (ATT) policies or because
                the customer didn't consent to tracking.
            CLICK_NOT_FOUND (33):
                The email address or phone number for this
                event can't be matched to a click. This may be
                because it didn't come from a Google Ads
                campaign, and you can safely ignore this
                warning. If this includes more imported events
                than is expected, you may need to check your
                setup.
            INVALID_USER_IDENTIFIER (34):
                Make sure you hash user provided data using
                SHA-256 and ensure you are normalizing according
                to the guidelines.
            EXTERNALLY_ATTRIBUTED_CONVERSION_ACTION_NOT_PERMITTED_WITH_USER_IDENTIFIER (35):
                User provided data can't be used with
                external attribution models. Use a different
                attribution model or omit user identifiers and
                try again.
            UNSUPPORTED_USER_IDENTIFIER (36):
                The provided user identifiers are not
                supported. Use only hashed email or phone number
                and try again.
            GBRAID_WBRAID_BOTH_SET (38):
                Can't use both gbraid and wbraid parameters.
                Use only 1 and try again.
            UNPARSEABLE_WBRAID (39):
                Can't parse event import data. Check if your
                wbraid parameter was not modified and try again.
            UNPARSEABLE_GBRAID (40):
                Can't parse event import data. Check if your
                gbraid parameter was not modified and try again.
            ONE_PER_CLICK_CONVERSION_ACTION_NOT_PERMITTED_WITH_BRAID (46):
                Conversion actions that use one-per-click
                counting can't be used with gbraid or wbraid
                parameters.
            CUSTOMER_DATA_POLICY_PROHIBITS_ENHANCED_CONVERSIONS (47):
                Enhanced conversions can't be used for this
                account because of Google customer data
                policies. Contact your Google representative.
            CUSTOMER_NOT_ACCEPTED_CUSTOMER_DATA_TERMS (48):
                Make sure you agree to the customer data processing terms in
                conversion settings and try again. You can check your
                setting by querying
                conversion_tracking_setting.accepted_customer_data_terms on
                Customer.
            ORDER_ID_CONTAINS_PII (49):
                Can't import events with order IDs containing
                personally-identifiable information (PII).
            CUSTOMER_NOT_ENABLED_ENHANCED_CONVERSIONS_FOR_LEADS (50):
                Make sure you've turned on enhanced conversions for leads in
                conversion settings and try again. You can check your
                setting by querying
                conversion_tracking_setting.enhanced_conversions_for_leads_enabled
                on Customer.
            INVALID_JOB_ID (52):
                The provided job id in the request is not within the allowed
                range. A job ID must be a positive integer in the range [1,
                2^31).
            NO_CONVERSION_ACTION_FOUND (53):
                The conversion action specified in the upload
                request cannot be found. Make sure it's
                available in this account.
            INVALID_CONVERSION_ACTION_TYPE (54):
                The conversion action specified in the upload
                request isn't set up for uploading conversions.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        TOO_MANY_CONVERSIONS_IN_REQUEST = 2
        UNPARSEABLE_GCLID = 3
        CONVERSION_PRECEDES_EVENT = 42
        EXPIRED_EVENT = 43
        TOO_RECENT_EVENT = 44
        EVENT_NOT_FOUND = 45
        UNAUTHORIZED_CUSTOMER = 8
        TOO_RECENT_CONVERSION_ACTION = 10
        CONVERSION_TRACKING_NOT_ENABLED_AT_IMPRESSION_TIME = 11
        EXTERNAL_ATTRIBUTION_DATA_SET_FOR_NON_EXTERNALLY_ATTRIBUTED_CONVERSION_ACTION = (
            12
        )
        EXTERNAL_ATTRIBUTION_DATA_NOT_SET_FOR_EXTERNALLY_ATTRIBUTED_CONVERSION_ACTION = (
            13
        )
        ORDER_ID_NOT_PERMITTED_FOR_EXTERNALLY_ATTRIBUTED_CONVERSION_ACTION = 14
        ORDER_ID_ALREADY_IN_USE = 15
        DUPLICATE_ORDER_ID = 16
        TOO_RECENT_CALL = 17
        EXPIRED_CALL = 18
        CALL_NOT_FOUND = 19
        CONVERSION_PRECEDES_CALL = 20
        CONVERSION_TRACKING_NOT_ENABLED_AT_CALL_TIME = 21
        UNPARSEABLE_CALLERS_PHONE_NUMBER = 22
        CLICK_CONVERSION_ALREADY_EXISTS = 23
        CALL_CONVERSION_ALREADY_EXISTS = 24
        DUPLICATE_CLICK_CONVERSION_IN_REQUEST = 25
        DUPLICATE_CALL_CONVERSION_IN_REQUEST = 26
        CUSTOM_VARIABLE_NOT_ENABLED = 28
        CUSTOM_VARIABLE_VALUE_CONTAINS_PII = 29
        INVALID_CUSTOMER_FOR_CLICK = 30
        INVALID_CUSTOMER_FOR_CALL = 31
        CONVERSION_NOT_COMPLIANT_WITH_ATT_POLICY = 32
        CLICK_NOT_FOUND = 33
        INVALID_USER_IDENTIFIER = 34
        EXTERNALLY_ATTRIBUTED_CONVERSION_ACTION_NOT_PERMITTED_WITH_USER_IDENTIFIER = (
            35
        )
        UNSUPPORTED_USER_IDENTIFIER = 36
        GBRAID_WBRAID_BOTH_SET = 38
        UNPARSEABLE_WBRAID = 39
        UNPARSEABLE_GBRAID = 40
        ONE_PER_CLICK_CONVERSION_ACTION_NOT_PERMITTED_WITH_BRAID = 46
        CUSTOMER_DATA_POLICY_PROHIBITS_ENHANCED_CONVERSIONS = 47
        CUSTOMER_NOT_ACCEPTED_CUSTOMER_DATA_TERMS = 48
        ORDER_ID_CONTAINS_PII = 49
        CUSTOMER_NOT_ENABLED_ENHANCED_CONVERSIONS_FOR_LEADS = 50
        INVALID_JOB_ID = 52
        NO_CONVERSION_ACTION_FOUND = 53
        INVALID_CONVERSION_ACTION_TYPE = 54


__all__ = tuple(sorted(__protobuf__.manifest))
