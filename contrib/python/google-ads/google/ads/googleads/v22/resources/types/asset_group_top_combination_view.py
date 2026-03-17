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

from google.ads.googleads.v22.common.types import asset_usage


__protobuf__ = proto.module(
    package="google.ads.googleads.v22.resources",
    marshal="google.ads.googleads.v22",
    manifest={
        "AssetGroupTopCombinationView",
        "AssetGroupAssetCombinationData",
    },
)


class AssetGroupTopCombinationView(proto.Message):
    r"""A view on the usage of asset group asset top combinations.

    Attributes:
        resource_name (str):
            Output only. The resource name of the asset group top
            combination view. AssetGroup Top Combination view resource
            names have the form:
            \`"customers/{customer_id}/assetGroupTopCombinationViews/{asset_group_id}~{asset_combination_category}".
        asset_group_top_combinations (MutableSequence[google.ads.googleads.v22.resources.types.AssetGroupAssetCombinationData]):
            Output only. The top combinations of assets
            that served together.
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    asset_group_top_combinations: MutableSequence[
        "AssetGroupAssetCombinationData"
    ] = proto.RepeatedField(
        proto.MESSAGE,
        number=2,
        message="AssetGroupAssetCombinationData",
    )


class AssetGroupAssetCombinationData(proto.Message):
    r"""Asset group asset combination data

    Attributes:
        asset_combination_served_assets (MutableSequence[google.ads.googleads.v22.common.types.AssetUsage]):
            Output only. Served assets.
    """

    asset_combination_served_assets: MutableSequence[asset_usage.AssetUsage] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=1,
            message=asset_usage.AssetUsage,
        )
    )


__all__ = tuple(sorted(__protobuf__.manifest))
