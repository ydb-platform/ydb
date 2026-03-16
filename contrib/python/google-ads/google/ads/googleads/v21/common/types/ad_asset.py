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

from google.ads.googleads.v21.common.types import asset_policy
from google.ads.googleads.v21.enums.types import (
    asset_performance_label as gage_asset_performance_label,
)
from google.ads.googleads.v21.enums.types import served_asset_field_type


__protobuf__ = proto.module(
    package="google.ads.googleads.v21.common",
    marshal="google.ads.googleads.v21",
    manifest={
        "AdTextAsset",
        "AdImageAsset",
        "AdVideoAsset",
        "AdVideoAssetInfo",
        "AdVideoAssetInventoryPreferences",
        "AdMediaBundleAsset",
        "AdDemandGenCarouselCardAsset",
        "AdCallToActionAsset",
        "AdAppDeepLinkAsset",
    },
)


class AdTextAsset(proto.Message):
    r"""A text asset used inside an ad.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        text (str):
            Asset text.

            This field is a member of `oneof`_ ``_text``.
        pinned_field (google.ads.googleads.v21.enums.types.ServedAssetFieldTypeEnum.ServedAssetFieldType):
            The pinned field of the asset. This restricts
            the asset to only serve within this field.
            Multiple assets can be pinned to the same field.
            An asset that is unpinned or pinned to a
            different field will not serve in a field where
            some other asset has been pinned.
        asset_performance_label (google.ads.googleads.v21.enums.types.AssetPerformanceLabelEnum.AssetPerformanceLabel):
            The performance label of this text asset.
        policy_summary_info (google.ads.googleads.v21.common.types.AdAssetPolicySummary):
            The policy summary of this text asset.
    """

    text: str = proto.Field(
        proto.STRING,
        number=4,
        optional=True,
    )
    pinned_field: (
        served_asset_field_type.ServedAssetFieldTypeEnum.ServedAssetFieldType
    ) = proto.Field(
        proto.ENUM,
        number=2,
        enum=served_asset_field_type.ServedAssetFieldTypeEnum.ServedAssetFieldType,
    )
    asset_performance_label: (
        gage_asset_performance_label.AssetPerformanceLabelEnum.AssetPerformanceLabel
    ) = proto.Field(
        proto.ENUM,
        number=5,
        enum=gage_asset_performance_label.AssetPerformanceLabelEnum.AssetPerformanceLabel,
    )
    policy_summary_info: asset_policy.AdAssetPolicySummary = proto.Field(
        proto.MESSAGE,
        number=6,
        message=asset_policy.AdAssetPolicySummary,
    )


class AdImageAsset(proto.Message):
    r"""An image asset used inside an ad.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        asset (str):
            The Asset resource name of this image.

            This field is a member of `oneof`_ ``_asset``.
    """

    asset: str = proto.Field(
        proto.STRING,
        number=2,
        optional=True,
    )


class AdVideoAsset(proto.Message):
    r"""A video asset used inside an ad.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        asset (str):
            The Asset resource name of this video.

            This field is a member of `oneof`_ ``_asset``.
        ad_video_asset_info (google.ads.googleads.v21.common.types.AdVideoAssetInfo):
            Contains info fields for this AdVideoAsset.

            This field is a member of `oneof`_ ``_ad_video_asset_info``.
    """

    asset: str = proto.Field(
        proto.STRING,
        number=2,
        optional=True,
    )
    ad_video_asset_info: "AdVideoAssetInfo" = proto.Field(
        proto.MESSAGE,
        number=4,
        optional=True,
        message="AdVideoAssetInfo",
    )


class AdVideoAssetInfo(proto.Message):
    r"""Contains info fields for AdVideoAssets.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        ad_video_asset_inventory_preferences (google.ads.googleads.v21.common.types.AdVideoAssetInventoryPreferences):
            List of inventory preferences for this
            AdVideoAsset. This field can only be set for
            DiscoveryVideoResponsiveAd. The video assets
            with an inventory asset preference set will be
            preferred over other videos from the same ad
            during serving time. For example, consider this
            ad being served for a specific inventory. The
            server will first try to match an eligible video
            with a matching preference for that inventory.
            Videos with no preferences are chosen only when
            a video with matching preference and eligible
            for a given ad slot can be found.

            This field is a member of `oneof`_ ``_ad_video_asset_inventory_preferences``.
    """

    ad_video_asset_inventory_preferences: "AdVideoAssetInventoryPreferences" = (
        proto.Field(
            proto.MESSAGE,
            number=1,
            optional=True,
            message="AdVideoAssetInventoryPreferences",
        )
    )


class AdVideoAssetInventoryPreferences(proto.Message):
    r"""YouTube Video Asset inventory preferences.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        in_feed_preference (bool):
            When true, YouTube Video Asset with this
            inventory preference will be preferred when
            choosing a video to serve In Feed.

            This field is a member of `oneof`_ ``_in_feed_preference``.
        in_stream_preference (bool):
            When true, YouTube Video Asset with this
            inventory preference will be preferred when
            choosing a video to serve In Stream.

            This field is a member of `oneof`_ ``_in_stream_preference``.
        shorts_preference (bool):
            When true, YouTube Video Asset with this
            inventory preference will be preferred when
            choosing a video to serve on YouTube Shorts.

            This field is a member of `oneof`_ ``_shorts_preference``.
    """

    in_feed_preference: bool = proto.Field(
        proto.BOOL,
        number=1,
        optional=True,
    )
    in_stream_preference: bool = proto.Field(
        proto.BOOL,
        number=2,
        optional=True,
    )
    shorts_preference: bool = proto.Field(
        proto.BOOL,
        number=3,
        optional=True,
    )


class AdMediaBundleAsset(proto.Message):
    r"""A media bundle asset used inside an ad.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        asset (str):
            The Asset resource name of this media bundle.

            This field is a member of `oneof`_ ``_asset``.
    """

    asset: str = proto.Field(
        proto.STRING,
        number=2,
        optional=True,
    )


class AdDemandGenCarouselCardAsset(proto.Message):
    r"""A Demand Gen carousel card asset used inside an ad.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        asset (str):
            The Asset resource name of this discovery
            carousel card.

            This field is a member of `oneof`_ ``_asset``.
    """

    asset: str = proto.Field(
        proto.STRING,
        number=1,
        optional=True,
    )


class AdCallToActionAsset(proto.Message):
    r"""A call to action asset used inside an ad.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        asset (str):
            The Asset resource name of this call to
            action asset.

            This field is a member of `oneof`_ ``_asset``.
    """

    asset: str = proto.Field(
        proto.STRING,
        number=1,
        optional=True,
    )


class AdAppDeepLinkAsset(proto.Message):
    r"""An app deep link used inside an ad.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        asset (str):
            The Asset resource name of this app deep link
            asset.

            This field is a member of `oneof`_ ``_asset``.
    """

    asset: str = proto.Field(
        proto.STRING,
        number=1,
        optional=True,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
