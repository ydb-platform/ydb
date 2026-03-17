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
        "KeywordPlanConceptGroupTypeEnum",
    },
)


class KeywordPlanConceptGroupTypeEnum(proto.Message):
    r"""Container for enumeration of keyword plan concept group
    types.

    """

    class KeywordPlanConceptGroupType(proto.Enum):
        r"""Enumerates keyword plan concept group types.

        Values:
            UNSPECIFIED (0):
                The concept group classification different
                from brand/non-brand. This is a catch all bucket
                for all classifications that are none of the
                below.
            UNKNOWN (1):
                The value is unknown in this version.
            BRAND (2):
                The concept group classification is based on
                BRAND.
            OTHER_BRANDS (3):
                The concept group classification based on
                BRAND, that didn't fit well with the BRAND
                classifications. These are generally outliers
                and can have very few keywords in this type of
                classification.
            NON_BRAND (4):
                These concept group classification is not
                based on BRAND. This is returned for generic
                keywords that don't have a brand association.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        BRAND = 2
        OTHER_BRANDS = 3
        NON_BRAND = 4


__all__ = tuple(sorted(__protobuf__.manifest))
