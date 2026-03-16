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
        "LabelErrorEnum",
    },
)


class LabelErrorEnum(proto.Message):
    r"""Container for enum describing possible label errors."""

    class LabelError(proto.Enum):
        r"""Enum describing possible label errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            CANNOT_APPLY_INACTIVE_LABEL (2):
                An inactive label cannot be applied.
            CANNOT_APPLY_LABEL_TO_DISABLED_AD_GROUP_CRITERION (3):
                A label cannot be applied to a disabled ad
                group criterion.
            CANNOT_APPLY_LABEL_TO_NEGATIVE_AD_GROUP_CRITERION (4):
                A label cannot be applied to a negative ad
                group criterion.
            EXCEEDED_LABEL_LIMIT_PER_TYPE (5):
                Cannot apply more than 50 labels per
                resource.
            INVALID_RESOURCE_FOR_MANAGER_LABEL (6):
                Labels from a manager account cannot be
                applied to campaign, ad group, ad group ad, or
                ad group criterion resources.
            DUPLICATE_NAME (7):
                Label names must be unique.
            INVALID_LABEL_NAME (8):
                Label names cannot be empty.
            CANNOT_ATTACH_LABEL_TO_DRAFT (9):
                Labels cannot be applied to a draft.
            CANNOT_ATTACH_NON_MANAGER_LABEL_TO_CUSTOMER (10):
                Labels not from a manager account cannot be
                applied to the customer resource.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        CANNOT_APPLY_INACTIVE_LABEL = 2
        CANNOT_APPLY_LABEL_TO_DISABLED_AD_GROUP_CRITERION = 3
        CANNOT_APPLY_LABEL_TO_NEGATIVE_AD_GROUP_CRITERION = 4
        EXCEEDED_LABEL_LIMIT_PER_TYPE = 5
        INVALID_RESOURCE_FOR_MANAGER_LABEL = 6
        DUPLICATE_NAME = 7
        INVALID_LABEL_NAME = 8
        CANNOT_ATTACH_LABEL_TO_DRAFT = 9
        CANNOT_ATTACH_NON_MANAGER_LABEL_TO_CUSTOMER = 10


__all__ = tuple(sorted(__protobuf__.manifest))
