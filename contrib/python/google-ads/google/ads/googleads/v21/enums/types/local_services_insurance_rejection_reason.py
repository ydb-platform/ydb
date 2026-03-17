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
        "LocalServicesInsuranceRejectionReasonEnum",
    },
)


class LocalServicesInsuranceRejectionReasonEnum(proto.Message):
    r"""Container for enum describing the rejection reason of a local
    services insurance verification artifact.

    """

    class LocalServicesInsuranceRejectionReason(proto.Enum):
        r"""Enums describing possible rejection reasons of a local
        services insurance verification artifact.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            BUSINESS_NAME_MISMATCH (2):
                Business name doesn't match business name for
                the Local Services Ad.
            INSURANCE_AMOUNT_INSUFFICIENT (3):
                Insurance amount doesn't meet requirement
                listed in the legal summaries documentation for
                that geographic + category ID combination.
            EXPIRED (4):
                Insurance document is expired.
            NO_SIGNATURE (5):
                Insurance document is missing a signature.
            NO_POLICY_NUMBER (6):
                Insurance document is missing a policy
                number.
            NO_COMMERCIAL_GENERAL_LIABILITY (7):
                Commercial General Liability(CGL) box is not
                marked in the insurance document.
            EDITABLE_FORMAT (8):
                Insurance document is in an editable format.
            CATEGORY_MISMATCH (9):
                Insurance document does not cover insurance
                for a particular category.
            MISSING_EXPIRATION_DATE (10):
                Insurance document is missing an expiration
                date.
            POOR_QUALITY (11):
                Insurance document is poor quality - blurry
                images, illegible, etc...
            POTENTIALLY_EDITED (12):
                Insurance document is suspected of being
                edited.
            WRONG_DOCUMENT_TYPE (13):
                Insurance document not accepted. For example,
                documents of insurance proposals, but missing
                coverages are not accepted.
            NON_FINAL (14):
                Insurance document is not final.
            OTHER (15):
                Insurance document has another flaw not
                listed explicitly.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        BUSINESS_NAME_MISMATCH = 2
        INSURANCE_AMOUNT_INSUFFICIENT = 3
        EXPIRED = 4
        NO_SIGNATURE = 5
        NO_POLICY_NUMBER = 6
        NO_COMMERCIAL_GENERAL_LIABILITY = 7
        EDITABLE_FORMAT = 8
        CATEGORY_MISMATCH = 9
        MISSING_EXPIRATION_DATE = 10
        POOR_QUALITY = 11
        POTENTIALLY_EDITED = 12
        WRONG_DOCUMENT_TYPE = 13
        NON_FINAL = 14
        OTHER = 15


__all__ = tuple(sorted(__protobuf__.manifest))
