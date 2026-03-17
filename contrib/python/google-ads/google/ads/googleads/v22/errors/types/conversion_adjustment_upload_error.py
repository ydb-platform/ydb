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
    package="google.ads.googleads.v22.errors",
    marshal="google.ads.googleads.v22",
    manifest={
        "ConversionAdjustmentUploadErrorEnum",
    },
)


class ConversionAdjustmentUploadErrorEnum(proto.Message):
    r"""Container for enum describing possible conversion adjustment
    upload errors.

    """

    class ConversionAdjustmentUploadError(proto.Enum):
        r"""Enum describing possible conversion adjustment upload errors.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            TOO_RECENT_CONVERSION_ACTION (2):
                Can't import events to a conversion action
                that was just created. Try importing again in 6
                hours.
            CONVERSION_ALREADY_RETRACTED (4):
                The conversion was already retracted. This
                adjustment was not processed.
            CONVERSION_NOT_FOUND (5):
                The conversion for this conversion action and
                conversion identifier can't be found. Make sure
                your conversion identifiers are associated with
                the correct conversion action and try again.
            CONVERSION_EXPIRED (6):
                Adjustment can't be made to a conversion that
                occurred more than 54 days ago.
            ADJUSTMENT_PRECEDES_CONVERSION (7):
                Adjustment has an ``adjustment_date_time`` that occurred
                before the associated conversion. Make sure your
                ``adjustment_date_time`` is correct and try again.
            MORE_RECENT_RESTATEMENT_FOUND (8):
                More recent adjustment ``adjustment_date_time`` has already
                been reported for the associated conversion. Make sure your
                adjustment ``adjustment_date_time`` is correct and try
                again.
            TOO_RECENT_CONVERSION (9):
                Adjustment can't be recorded because the
                conversion occurred too recently. Try adjusting
                a conversion that occurred at least 24 hours
                ago.
            CANNOT_RESTATE_CONVERSION_ACTION_THAT_ALWAYS_USES_DEFAULT_CONVERSION_VALUE (10):
                Can't make an adjustment to a conversion that
                is set up to use the default value. Check your
                conversion action value setting and try again.
            TOO_MANY_ADJUSTMENTS_IN_REQUEST (11):
                Try uploading fewer than 2001 adjustments in
                a single API request.
            TOO_MANY_ADJUSTMENTS (12):
                The conversion has already been adjusted the
                maximum number of times. Make sure you're only
                making necessary adjustment to existing
                conversion.
            RESTATEMENT_ALREADY_EXISTS (13):
                The conversion has prior a restatement with the same
                ``adjustment_date_time``. Make sure your adjustment has the
                correct and unique ``adjustment_date_time`` and try again.
            DUPLICATE_ADJUSTMENT_IN_REQUEST (14):
                Imported adjustment has a duplicate conversion adjustment
                with same ``adjustment_date_time``. Make sure your
                adjustment has the correct ``adjustment_date_time`` and try
                again.
            CUSTOMER_NOT_ACCEPTED_CUSTOMER_DATA_TERMS (15):
                Make sure you agree to the customer data
                processing terms in conversion settings and try
                again.
            CONVERSION_ACTION_NOT_ELIGIBLE_FOR_ENHANCEMENT (16):
                Can't use enhanced conversions with the
                specified conversion action.
            INVALID_USER_IDENTIFIER (17):
                Make sure you hash user provided data using
                SHA-256 and ensure you are normalizing according
                to the guidelines.
            UNSUPPORTED_USER_IDENTIFIER (18):
                Use user provided data such as emails or
                phone numbers hashed using SHA-256 and try
                again.
            GCLID_DATE_TIME_PAIR_AND_ORDER_ID_BOTH_SET (20):
                Cannot set both gclid_date_time_pair and order_id. Use only
                1 type and try again.
            CONVERSION_ALREADY_ENHANCED (21):
                Conversion already has enhancements with the
                same Order ID and conversion action. Make sure
                your data is correctly configured and try again.
            DUPLICATE_ENHANCEMENT_IN_REQUEST (22):
                Multiple enhancements have the same
                conversion action and Order ID.  Make sure your
                data is correctly configured and try again.
            CUSTOMER_DATA_POLICY_PROHIBITS_ENHANCEMENT (23):
                Enhanced conversions can't be used for this
                account because of Google customer data
                policies. Contact your Google representative.
            MISSING_ORDER_ID_FOR_WEBPAGE (24):
                Adjustment for website conversion requires
                Order ID (ie, transaction ID). Make sure your
                website tags capture Order IDs and you send the
                same Order IDs with your adjustment.
            ORDER_ID_CONTAINS_PII (25):
                Can't use adjustment with Order IDs
                containing personally-identifiable information
                (PII).
            INVALID_JOB_ID (26):
                The provided job id in the request is not within the allowed
                range. A job ID must be a positive integer in the range [1,
                2^31).
            NO_CONVERSION_ACTION_FOUND (27):
                The conversion action specified in the
                adjustment request cannot be found. Make sure
                it's available in this account.
            INVALID_CONVERSION_ACTION_TYPE (28):
                The type of the conversion action specified in the
                adjustment request isn't supported for uploading
                adjustments. A conversion adjustment of type ``RETRACTION``
                or ``RESTATEMENT`` is only permitted for conversion actions
                of type ``SALESFORCE``, ``UPLOAD_CLICK`` or ``WEBPAGE``. A
                conversion adjustment of type ``ENHANCEMENT`` is only
                permitted for conversion actions of type ``WEBPAGE``.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        TOO_RECENT_CONVERSION_ACTION = 2
        CONVERSION_ALREADY_RETRACTED = 4
        CONVERSION_NOT_FOUND = 5
        CONVERSION_EXPIRED = 6
        ADJUSTMENT_PRECEDES_CONVERSION = 7
        MORE_RECENT_RESTATEMENT_FOUND = 8
        TOO_RECENT_CONVERSION = 9
        CANNOT_RESTATE_CONVERSION_ACTION_THAT_ALWAYS_USES_DEFAULT_CONVERSION_VALUE = (
            10
        )
        TOO_MANY_ADJUSTMENTS_IN_REQUEST = 11
        TOO_MANY_ADJUSTMENTS = 12
        RESTATEMENT_ALREADY_EXISTS = 13
        DUPLICATE_ADJUSTMENT_IN_REQUEST = 14
        CUSTOMER_NOT_ACCEPTED_CUSTOMER_DATA_TERMS = 15
        CONVERSION_ACTION_NOT_ELIGIBLE_FOR_ENHANCEMENT = 16
        INVALID_USER_IDENTIFIER = 17
        UNSUPPORTED_USER_IDENTIFIER = 18
        GCLID_DATE_TIME_PAIR_AND_ORDER_ID_BOTH_SET = 20
        CONVERSION_ALREADY_ENHANCED = 21
        DUPLICATE_ENHANCEMENT_IN_REQUEST = 22
        CUSTOMER_DATA_POLICY_PROHIBITS_ENHANCEMENT = 23
        MISSING_ORDER_ID_FOR_WEBPAGE = 24
        ORDER_ID_CONTAINS_PII = 25
        INVALID_JOB_ID = 26
        NO_CONVERSION_ACTION_FOUND = 27
        INVALID_CONVERSION_ACTION_TYPE = 28


__all__ = tuple(sorted(__protobuf__.manifest))
