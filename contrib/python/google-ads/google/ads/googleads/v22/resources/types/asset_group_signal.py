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

from typing import MutableSequence

import proto  # type: ignore

from google.ads.googleads.v22.common.types import criteria
from google.ads.googleads.v22.enums.types import (
    asset_group_signal_approval_status,
)


__protobuf__ = proto.module(
    package="google.ads.googleads.v22.resources",
    marshal="google.ads.googleads.v22",
    manifest={
        "AssetGroupSignal",
    },
)


class AssetGroupSignal(proto.Message):
    r"""AssetGroupSignal represents a signal in an asset group. The
    existence of a signal tells the performance max campaign who's
    most likely to convert. Performance Max uses the signal to look
    for new people with similar or stronger intent to find
    conversions across Search, Display, Video, and more.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Immutable. The resource name of the asset group signal.
            Asset group signal resource name have the form:

            ``customers/{customer_id}/assetGroupSignals/{asset_group_id}~{signal_id}``
        asset_group (str):
            Immutable. The asset group which this asset
            group signal belongs to.
        approval_status (google.ads.googleads.v22.enums.types.AssetGroupSignalApprovalStatusEnum.AssetGroupSignalApprovalStatus):
            Output only. Approval status is the output
            value for search theme signal after Google ads
            policy review. When using Audience signal, this
            field is not used and will be absent.
        disapproval_reasons (MutableSequence[str]):
            Output only. Computed for SearchTheme
            signals. When using Audience signal, this field
            is not used and will be absent.
        audience (google.ads.googleads.v22.common.types.AudienceInfo):
            Immutable. The audience signal to be used by
            the performance max campaign.

            This field is a member of `oneof`_ ``signal``.
        search_theme (google.ads.googleads.v22.common.types.SearchThemeInfo):
            Immutable. The search_theme signal to be used by the
            performance max campaign. Mutate errors of search_theme
            criterion includes AssetGroupSignalError.UNSPECIFIED
            AssetGroupSignalError.UNKNOWN
            AssetGroupSignalError.TOO_MANY_WORDS
            AssetGroupSignalError.SEARCH_THEME_POLICY_VIOLATION
            FieldError.REQUIRED StringFormatError.ILLEGAL_CHARS
            StringLengthError.TOO_LONG
            ResourceCountLimitExceededError.RESOURCE_LIMIT

            This field is a member of `oneof`_ ``signal``.
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    asset_group: str = proto.Field(
        proto.STRING,
        number=2,
    )
    approval_status: (
        asset_group_signal_approval_status.AssetGroupSignalApprovalStatusEnum.AssetGroupSignalApprovalStatus
    ) = proto.Field(
        proto.ENUM,
        number=6,
        enum=asset_group_signal_approval_status.AssetGroupSignalApprovalStatusEnum.AssetGroupSignalApprovalStatus,
    )
    disapproval_reasons: MutableSequence[str] = proto.RepeatedField(
        proto.STRING,
        number=7,
    )
    audience: criteria.AudienceInfo = proto.Field(
        proto.MESSAGE,
        number=4,
        oneof="signal",
        message=criteria.AudienceInfo,
    )
    search_theme: criteria.SearchThemeInfo = proto.Field(
        proto.MESSAGE,
        number=5,
        oneof="signal",
        message=criteria.SearchThemeInfo,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
