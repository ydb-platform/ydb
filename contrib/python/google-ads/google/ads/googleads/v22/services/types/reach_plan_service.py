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

from google.ads.googleads.v22.common.types import additional_application_info
from google.ads.googleads.v22.common.types import criteria
from google.ads.googleads.v22.common.types import dates
from google.ads.googleads.v22.enums.types import frequency_cap_time_unit
from google.ads.googleads.v22.enums.types import reach_plan_age_range
from google.ads.googleads.v22.enums.types import (
    reach_plan_conversion_rate_model,
)
from google.ads.googleads.v22.enums.types import reach_plan_network
from google.ads.googleads.v22.enums.types import (
    reach_plan_plannable_user_list_status,
)
from google.ads.googleads.v22.enums.types import reach_plan_surface
from google.ads.googleads.v22.enums.types import target_frequency_time_unit
from google.ads.googleads.v22.enums.types import user_interest_taxonomy_type
from google.ads.googleads.v22.enums.types import (
    user_list_crm_data_source_type as gage_user_list_crm_data_source_type,
)
from google.ads.googleads.v22.enums.types import (
    user_list_type as gage_user_list_type,
)


__protobuf__ = proto.module(
    package="google.ads.googleads.v22.services",
    marshal="google.ads.googleads.v22",
    manifest={
        "GenerateConversionRatesRequest",
        "GenerateConversionRatesResponse",
        "ConversionRateSuggestion",
        "ListPlannableLocationsRequest",
        "ListPlannableLocationsResponse",
        "PlannableLocation",
        "ListPlannableProductsRequest",
        "ListPlannableProductsResponse",
        "ProductMetadata",
        "ListPlannableUserListsRequest",
        "ListPlannableUserListsResponse",
        "PlannableUserList",
        "PlannableUserListMetadata",
        "PlannableTargeting",
        "ListPlannableUserInterestsRequest",
        "ListPlannableUserInterestsResponse",
        "PlannableUserInterest",
        "GenerateReachForecastRequest",
        "EffectiveFrequencyLimit",
        "FrequencyCap",
        "Targeting",
        "CampaignDuration",
        "PlannedProduct",
        "GenerateReachForecastResponse",
        "ReachCurve",
        "ReachForecast",
        "Forecast",
        "PlannedProductReachForecast",
        "PlannedProductForecast",
        "OnTargetAudienceMetrics",
        "EffectiveFrequencyBreakdown",
        "ForecastMetricOptions",
        "AudienceTargeting",
        "AdvancedProductTargeting",
        "YouTubeSelectSettings",
        "YouTubeSelectLineUp",
        "SurfaceTargetingCombinations",
        "SurfaceTargeting",
        "TargetFrequencySettings",
    },
)


class GenerateConversionRatesRequest(proto.Message):
    r"""Request message for
    [ReachPlanService.GenerateConversionRates][google.ads.googleads.v22.services.ReachPlanService.GenerateConversionRates].


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        customer_id (str):
            Required. The ID of the customer. A
            conversion rate based on the historical data of
            this customer may be suggested.
        customer_reach_group (str):
            The name of the customer being planned for.
            This is a user-defined value.

            This field is a member of `oneof`_ ``_customer_reach_group``.
        reach_application_info (google.ads.googleads.v22.common.types.AdditionalApplicationInfo):
            Optional. Additional information on the
            application issuing the request.
    """

    customer_id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    customer_reach_group: str = proto.Field(
        proto.STRING,
        number=2,
        optional=True,
    )
    reach_application_info: (
        additional_application_info.AdditionalApplicationInfo
    ) = proto.Field(
        proto.MESSAGE,
        number=3,
        message=additional_application_info.AdditionalApplicationInfo,
    )


class GenerateConversionRatesResponse(proto.Message):
    r"""Response message for
    [ReachPlanService.GenerateConversionRates][google.ads.googleads.v22.services.ReachPlanService.GenerateConversionRates],
    containing conversion rate suggestions for supported plannable
    products.

    Attributes:
        conversion_rate_suggestions (MutableSequence[google.ads.googleads.v22.services.types.ConversionRateSuggestion]):
            A list containing conversion rate
            suggestions. Each repeated element will have an
            associated product code. Multiple suggestions
            may share the same product code.
    """

    conversion_rate_suggestions: MutableSequence["ConversionRateSuggestion"] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=1,
            message="ConversionRateSuggestion",
        )
    )


class ConversionRateSuggestion(proto.Message):
    r"""A conversion rate suggestion.

    Attributes:
        conversion_rate_model (google.ads.googleads.v22.enums.types.ReachPlanConversionRateModelEnum.ReachPlanConversionRateModel):
            Model type used to calculate the suggested
            conversion rate.
        plannable_product_code (str):
            The code associated with the plannable product (for example:
            DEMAND_GEN). To list all plannable product codes, use
            [ReachPlanService.ListPlannableProducts][google.ads.googleads.v22.services.ReachPlanService.ListPlannableProducts].
        conversion_rate (float):
            The suggested conversion rate. The value is
            between 0 and 1 (exclusive).
    """

    conversion_rate_model: (
        reach_plan_conversion_rate_model.ReachPlanConversionRateModelEnum.ReachPlanConversionRateModel
    ) = proto.Field(
        proto.ENUM,
        number=1,
        enum=reach_plan_conversion_rate_model.ReachPlanConversionRateModelEnum.ReachPlanConversionRateModel,
    )
    plannable_product_code: str = proto.Field(
        proto.STRING,
        number=2,
    )
    conversion_rate: float = proto.Field(
        proto.DOUBLE,
        number=3,
    )


class ListPlannableLocationsRequest(proto.Message):
    r"""Request message for
    [ReachPlanService.ListPlannableLocations][google.ads.googleads.v22.services.ReachPlanService.ListPlannableLocations].

    Attributes:
        reach_application_info (google.ads.googleads.v22.common.types.AdditionalApplicationInfo):
            Optional. Additional information on the
            application issuing the request.
    """

    reach_application_info: (
        additional_application_info.AdditionalApplicationInfo
    ) = proto.Field(
        proto.MESSAGE,
        number=1,
        message=additional_application_info.AdditionalApplicationInfo,
    )


class ListPlannableLocationsResponse(proto.Message):
    r"""The list of plannable locations.

    Attributes:
        plannable_locations (MutableSequence[google.ads.googleads.v22.services.types.PlannableLocation]):
            The list of locations available for planning.
            See
            https://developers.google.com/google-ads/api/reference/data/geotargets
            for sample locations.
    """

    plannable_locations: MutableSequence["PlannableLocation"] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=1,
            message="PlannableLocation",
        )
    )


class PlannableLocation(proto.Message):
    r"""A plannable location: country, metro region, province, etc.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        id (str):
            The location identifier.

            This field is a member of `oneof`_ ``_id``.
        name (str):
            The unique location name in English.

            This field is a member of `oneof`_ ``_name``.
        parent_country_id (int):
            The parent country (not present if location is a country).
            If present, will always be a GeoTargetConstant ID.
            Additional information such as country name is provided by
            [ReachPlanService.ListPlannableLocations][google.ads.googleads.v22.services.ReachPlanService.ListPlannableLocations]
            or GoogleAdsService.Search/SearchStream.

            This field is a member of `oneof`_ ``_parent_country_id``.
        country_code (str):
            The ISO-3166-1 alpha-2 country code that is
            associated with the location.

            This field is a member of `oneof`_ ``_country_code``.
        location_type (str):
            The location's type. Location types correspond to
            target_type returned by searching location type in
            GoogleAdsService.Search/SearchStream.

            This field is a member of `oneof`_ ``_location_type``.
    """

    id: str = proto.Field(
        proto.STRING,
        number=4,
        optional=True,
    )
    name: str = proto.Field(
        proto.STRING,
        number=5,
        optional=True,
    )
    parent_country_id: int = proto.Field(
        proto.INT64,
        number=6,
        optional=True,
    )
    country_code: str = proto.Field(
        proto.STRING,
        number=7,
        optional=True,
    )
    location_type: str = proto.Field(
        proto.STRING,
        number=8,
        optional=True,
    )


class ListPlannableProductsRequest(proto.Message):
    r"""Request to list available products in a given location.

    Attributes:
        plannable_location_id (str):
            Required. The ID of the selected location for planning. To
            list the available plannable location IDs use
            [ReachPlanService.ListPlannableLocations][google.ads.googleads.v22.services.ReachPlanService.ListPlannableLocations].
        reach_application_info (google.ads.googleads.v22.common.types.AdditionalApplicationInfo):
            Optional. Additional information on the
            application issuing the request.
    """

    plannable_location_id: str = proto.Field(
        proto.STRING,
        number=2,
    )
    reach_application_info: (
        additional_application_info.AdditionalApplicationInfo
    ) = proto.Field(
        proto.MESSAGE,
        number=3,
        message=additional_application_info.AdditionalApplicationInfo,
    )


class ListPlannableProductsResponse(proto.Message):
    r"""A response with all available products.

    Attributes:
        product_metadata (MutableSequence[google.ads.googleads.v22.services.types.ProductMetadata]):
            The list of products available for planning
            and related targeting metadata.
    """

    product_metadata: MutableSequence["ProductMetadata"] = proto.RepeatedField(
        proto.MESSAGE,
        number=1,
        message="ProductMetadata",
    )


class ProductMetadata(proto.Message):
    r"""The metadata associated with an available plannable product.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        plannable_product_code (str):
            The code associated with the ad product (for example:
            BUMPER, TRUEVIEW_IN_STREAM). To list the available plannable
            product codes use
            [ReachPlanService.ListPlannableProducts][google.ads.googleads.v22.services.ReachPlanService.ListPlannableProducts].

            This field is a member of `oneof`_ ``_plannable_product_code``.
        plannable_product_name (str):
            The name associated with the ad product.
        plannable_targeting (google.ads.googleads.v22.services.types.PlannableTargeting):
            The allowed plannable targeting for this
            product.
    """

    plannable_product_code: str = proto.Field(
        proto.STRING,
        number=4,
        optional=True,
    )
    plannable_product_name: str = proto.Field(
        proto.STRING,
        number=3,
    )
    plannable_targeting: "PlannableTargeting" = proto.Field(
        proto.MESSAGE,
        number=2,
        message="PlannableTargeting",
    )


class ListPlannableUserListsRequest(proto.Message):
    r"""Request message for
    [ReachPlanService.ListPlannableUserLists][google.ads.googleads.v22.services.ReachPlanService.ListPlannableUserLists]
    that lists the available user lists for a customer.


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        customer_id (str):
            Required. The ID of the customer.
        customer_reach_group (str):
            The name of the customer being planned for.
            This is a user-defined value.

            This field is a member of `oneof`_ ``_customer_reach_group``.
        reach_application_info (google.ads.googleads.v22.common.types.AdditionalApplicationInfo):
            Optional. Additional information on the
            application issuing the request.
    """

    customer_id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    customer_reach_group: str = proto.Field(
        proto.STRING,
        number=2,
        optional=True,
    )
    reach_application_info: (
        additional_application_info.AdditionalApplicationInfo
    ) = proto.Field(
        proto.MESSAGE,
        number=3,
        message=additional_application_info.AdditionalApplicationInfo,
    )


class ListPlannableUserListsResponse(proto.Message):
    r"""A response with all available user lists with their plannable
    status.

    Attributes:
        plannable_user_lists (MutableSequence[google.ads.googleads.v22.services.types.PlannableUserList]):
            The list of user lists available for planning
            and related targeting metadata.
    """

    plannable_user_lists: MutableSequence["PlannableUserList"] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=1,
            message="PlannableUserList",
        )
    )


class PlannableUserList(proto.Message):
    r"""A plannable user list.

    Attributes:
        user_list_info (google.ads.googleads.v22.common.types.UserListInfo):
            The user list ID.
        display_name (str):
            The name of the user list.
        user_list_type (google.ads.googleads.v22.enums.types.UserListTypeEnum.UserListType):
            The user list type.
        plannable_status (google.ads.googleads.v22.enums.types.ReachPlanPlannableUserListStatusEnum.ReachPlanPlannableUserListStatus):
            The plannable status of the user list.
        plannable_user_list_metadata (google.ads.googleads.v22.services.types.PlannableUserListMetadata):
            The relevant metadata for this user list.
    """

    user_list_info: criteria.UserListInfo = proto.Field(
        proto.MESSAGE,
        number=1,
        message=criteria.UserListInfo,
    )
    display_name: str = proto.Field(
        proto.STRING,
        number=2,
    )
    user_list_type: gage_user_list_type.UserListTypeEnum.UserListType = (
        proto.Field(
            proto.ENUM,
            number=3,
            enum=gage_user_list_type.UserListTypeEnum.UserListType,
        )
    )
    plannable_status: (
        reach_plan_plannable_user_list_status.ReachPlanPlannableUserListStatusEnum.ReachPlanPlannableUserListStatus
    ) = proto.Field(
        proto.ENUM,
        number=4,
        enum=reach_plan_plannable_user_list_status.ReachPlanPlannableUserListStatusEnum.ReachPlanPlannableUserListStatus,
    )
    plannable_user_list_metadata: "PlannableUserListMetadata" = proto.Field(
        proto.MESSAGE,
        number=5,
        message="PlannableUserListMetadata",
    )


class PlannableUserListMetadata(proto.Message):
    r"""The metadata associated with a plannable user list.

    Attributes:
        user_list_crm_data_source_type (google.ads.googleads.v22.enums.types.UserListCrmDataSourceTypeEnum.UserListCrmDataSourceType):
            The data source type of a CRM based user
            list.
    """

    user_list_crm_data_source_type: (
        gage_user_list_crm_data_source_type.UserListCrmDataSourceTypeEnum.UserListCrmDataSourceType
    ) = proto.Field(
        proto.ENUM,
        number=1,
        enum=gage_user_list_crm_data_source_type.UserListCrmDataSourceTypeEnum.UserListCrmDataSourceType,
    )


class PlannableTargeting(proto.Message):
    r"""The targeting for which traffic metrics will be reported.

    Attributes:
        age_ranges (MutableSequence[google.ads.googleads.v22.enums.types.ReachPlanAgeRangeEnum.ReachPlanAgeRange]):
            Allowed plannable age ranges for the product
            for which metrics will be reported. Actual
            targeting is computed by mapping this age range
            onto standard Google common.AgeRangeInfo values.
        genders (MutableSequence[google.ads.googleads.v22.common.types.GenderInfo]):
            Targetable genders for the ad product.
        devices (MutableSequence[google.ads.googleads.v22.common.types.DeviceInfo]):
            Targetable devices for the ad product. TABLET device
            targeting is automatically applied to reported metrics when
            MOBILE targeting is selected for CPM_MASTHEAD,
            GOOGLE_PREFERRED_BUMPER, and GOOGLE_PREFERRED_SHORT
            products.
        networks (MutableSequence[google.ads.googleads.v22.enums.types.ReachPlanNetworkEnum.ReachPlanNetwork]):
            Targetable networks for the ad product.
        youtube_select_lineups (MutableSequence[google.ads.googleads.v22.services.types.YouTubeSelectLineUp]):
            Targetable YouTube Select Lineups for the ad
            product.
        surface_targeting (google.ads.googleads.v22.services.types.SurfaceTargetingCombinations):
            Targetable surface combinations for the ad
            product.
    """

    age_ranges: MutableSequence[
        reach_plan_age_range.ReachPlanAgeRangeEnum.ReachPlanAgeRange
    ] = proto.RepeatedField(
        proto.ENUM,
        number=1,
        enum=reach_plan_age_range.ReachPlanAgeRangeEnum.ReachPlanAgeRange,
    )
    genders: MutableSequence[criteria.GenderInfo] = proto.RepeatedField(
        proto.MESSAGE,
        number=2,
        message=criteria.GenderInfo,
    )
    devices: MutableSequence[criteria.DeviceInfo] = proto.RepeatedField(
        proto.MESSAGE,
        number=3,
        message=criteria.DeviceInfo,
    )
    networks: MutableSequence[
        reach_plan_network.ReachPlanNetworkEnum.ReachPlanNetwork
    ] = proto.RepeatedField(
        proto.ENUM,
        number=4,
        enum=reach_plan_network.ReachPlanNetworkEnum.ReachPlanNetwork,
    )
    youtube_select_lineups: MutableSequence["YouTubeSelectLineUp"] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=5,
            message="YouTubeSelectLineUp",
        )
    )
    surface_targeting: "SurfaceTargetingCombinations" = proto.Field(
        proto.MESSAGE,
        number=6,
        message="SurfaceTargetingCombinations",
    )


class ListPlannableUserInterestsRequest(proto.Message):
    r"""Request message for
    [ReachPlanService.ListPlannableUserInterests][google.ads.googleads.v22.services.ReachPlanService.ListPlannableUserInterests].


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        customer_id (str):
            Required. The ID of the customer.
        user_interest_taxonomy_types (MutableSequence[google.ads.googleads.v22.enums.types.UserInterestTaxonomyTypeEnum.UserInterestTaxonomyType]):
            Optional. A filter by user interest type. If set, only user
            interests with a type listed in the filter will be returned.
            If not set, user interests of all supported types will be
            returned. Supported user interest types are AFFINITY and
            IN_MARKET. Each type must be specified at most once.
        name_query (str):
            A filter by user interest name. If set, only
            user interests with a name containing the
            literal string (case insensitive) in the filter
            will be returned. Maximum length is 200
            characters.

            This field is a member of `oneof`_ ``_name_query``.
        path_query (str):
            A filter by user interest path. If set, only
            user interests with a path containing the
            literal string (case insensitive) in the filter
            will be returned. Maximum length is 200
            characters.

            This field is a member of `oneof`_ ``_path_query``.
        reach_application_info (google.ads.googleads.v22.common.types.AdditionalApplicationInfo):
            Optional. Additional information on the
            application issuing the request.
    """

    customer_id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    user_interest_taxonomy_types: MutableSequence[
        user_interest_taxonomy_type.UserInterestTaxonomyTypeEnum.UserInterestTaxonomyType
    ] = proto.RepeatedField(
        proto.ENUM,
        number=2,
        enum=user_interest_taxonomy_type.UserInterestTaxonomyTypeEnum.UserInterestTaxonomyType,
    )
    name_query: str = proto.Field(
        proto.STRING,
        number=3,
        optional=True,
    )
    path_query: str = proto.Field(
        proto.STRING,
        number=4,
        optional=True,
    )
    reach_application_info: (
        additional_application_info.AdditionalApplicationInfo
    ) = proto.Field(
        proto.MESSAGE,
        number=5,
        message=additional_application_info.AdditionalApplicationInfo,
    )


class ListPlannableUserInterestsResponse(proto.Message):
    r"""Response message for
    [ReachPlanService.ListPlannableUserInterests][google.ads.googleads.v22.services.ReachPlanService.ListPlannableUserInterests].

    Attributes:
        plannable_user_interests (MutableSequence[google.ads.googleads.v22.services.types.PlannableUserInterest]):
            The list of plannable user interests.
    """

    plannable_user_interests: MutableSequence["PlannableUserInterest"] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=1,
            message="PlannableUserInterest",
        )
    )


class PlannableUserInterest(proto.Message):
    r"""A plannable user interest that can be targeted in a reach forecast
    using
    [ReachPlanService.GenerateReachForecast][google.ads.googleads.v22.services.ReachPlanService.GenerateReachForecast].

    Attributes:
        user_interest (google.ads.googleads.v22.common.types.UserInterestInfo):
            The user interest id.
        user_interest_type (google.ads.googleads.v22.enums.types.UserInterestTaxonomyTypeEnum.UserInterestTaxonomyType):
            The user interest type.
        user_interest_display_name (str):
            The user interest display name.
            For example, "Autos & Vehicles".
        user_interest_path (str):
            The user interest path.
            For example, "/Autos & Vehicles/Motor
            Vehicles/Motor Vehicles by Type/Off-Road
            Vehicles".
    """

    user_interest: criteria.UserInterestInfo = proto.Field(
        proto.MESSAGE,
        number=1,
        message=criteria.UserInterestInfo,
    )
    user_interest_type: (
        user_interest_taxonomy_type.UserInterestTaxonomyTypeEnum.UserInterestTaxonomyType
    ) = proto.Field(
        proto.ENUM,
        number=2,
        enum=user_interest_taxonomy_type.UserInterestTaxonomyTypeEnum.UserInterestTaxonomyType,
    )
    user_interest_display_name: str = proto.Field(
        proto.STRING,
        number=3,
    )
    user_interest_path: str = proto.Field(
        proto.STRING,
        number=4,
    )


class GenerateReachForecastRequest(proto.Message):
    r"""Request message for
    [ReachPlanService.GenerateReachForecast][google.ads.googleads.v22.services.ReachPlanService.GenerateReachForecast].


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        customer_id (str):
            Required. The ID of the customer.
        currency_code (str):
            The currency code.
            Three-character ISO 4217 currency code.

            This field is a member of `oneof`_ ``_currency_code``.
        campaign_duration (google.ads.googleads.v22.services.types.CampaignDuration):
            Required. Campaign duration.
        cookie_frequency_cap (int):
            Chosen cookie frequency cap to be applied to each planned
            product. This is equivalent to the frequency cap exposed in
            Google Ads when creating a campaign, it represents the
            maximum number of times an ad can be shown to the same user.
            If not specified, no cap is applied.

            This field is deprecated in v4 and will eventually be
            removed. Use cookie_frequency_cap_setting instead.

            This field is a member of `oneof`_ ``_cookie_frequency_cap``.
        cookie_frequency_cap_setting (google.ads.googleads.v22.services.types.FrequencyCap):
            Chosen cookie frequency cap to be applied to each planned
            product. This is equivalent to the frequency cap exposed in
            Google Ads when creating a campaign, it represents the
            maximum number of times an ad can be shown to the same user
            during a specified time interval. If not specified, a
            default of 0 (no cap) is applied.

            This field replaces the deprecated cookie_frequency_cap
            field.
        min_effective_frequency (int):
            Chosen minimum effective frequency (the number of times a
            person was exposed to the ad) for the reported reach metrics
            [1-10]. This won't affect the targeting, but just the
            reporting. If not specified, a default of 1 is applied.

            This field cannot be combined with the
            effective_frequency_limit field.

            This field is a member of `oneof`_ ``_min_effective_frequency``.
        effective_frequency_limit (google.ads.googleads.v22.services.types.EffectiveFrequencyLimit):
            The highest minimum effective frequency (the number of times
            a person was exposed to the ad) value [1-10] to include in
            Forecast.effective_frequency_breakdowns. If not specified,
            Forecast.effective_frequency_breakdowns will not be
            provided.

            The effective frequency value provided here will also be
            used as the minimum effective frequency for the reported
            reach metrics.

            This field cannot be combined with the
            min_effective_frequency field.

            This field is a member of `oneof`_ ``_effective_frequency_limit``.
        targeting (google.ads.googleads.v22.services.types.Targeting):
            The targeting to be applied to all products
            selected in the product mix.
            This is planned targeting: execution details
            might vary based on the advertising product,
            consult an implementation specialist.

            See specific metrics for details on how
            targeting affects them.
        planned_products (MutableSequence[google.ads.googleads.v22.services.types.PlannedProduct]):
            Required. The products to be forecast.
            The max number of allowed planned products is
            15.
        forecast_metric_options (google.ads.googleads.v22.services.types.ForecastMetricOptions):
            Controls the forecast metrics returned in the
            response.
        customer_reach_group (str):
            The name of the customer being planned for.
            This is a user-defined value.

            This field is a member of `oneof`_ ``_customer_reach_group``.
        reach_application_info (google.ads.googleads.v22.common.types.AdditionalApplicationInfo):
            Optional. Additional information on the
            application issuing the request.
    """

    customer_id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    currency_code: str = proto.Field(
        proto.STRING,
        number=9,
        optional=True,
    )
    campaign_duration: "CampaignDuration" = proto.Field(
        proto.MESSAGE,
        number=3,
        message="CampaignDuration",
    )
    cookie_frequency_cap: int = proto.Field(
        proto.INT32,
        number=10,
        optional=True,
    )
    cookie_frequency_cap_setting: "FrequencyCap" = proto.Field(
        proto.MESSAGE,
        number=8,
        message="FrequencyCap",
    )
    min_effective_frequency: int = proto.Field(
        proto.INT32,
        number=11,
        optional=True,
    )
    effective_frequency_limit: "EffectiveFrequencyLimit" = proto.Field(
        proto.MESSAGE,
        number=12,
        optional=True,
        message="EffectiveFrequencyLimit",
    )
    targeting: "Targeting" = proto.Field(
        proto.MESSAGE,
        number=6,
        message="Targeting",
    )
    planned_products: MutableSequence["PlannedProduct"] = proto.RepeatedField(
        proto.MESSAGE,
        number=7,
        message="PlannedProduct",
    )
    forecast_metric_options: "ForecastMetricOptions" = proto.Field(
        proto.MESSAGE,
        number=13,
        message="ForecastMetricOptions",
    )
    customer_reach_group: str = proto.Field(
        proto.STRING,
        number=14,
        optional=True,
    )
    reach_application_info: (
        additional_application_info.AdditionalApplicationInfo
    ) = proto.Field(
        proto.MESSAGE,
        number=15,
        message=additional_application_info.AdditionalApplicationInfo,
    )


class EffectiveFrequencyLimit(proto.Message):
    r"""Effective frequency limit.

    Attributes:
        effective_frequency_breakdown_limit (int):
            The highest effective frequency value to include in
            Forecast.effective_frequency_breakdowns. This field supports
            frequencies 1-10, inclusive.
    """

    effective_frequency_breakdown_limit: int = proto.Field(
        proto.INT32,
        number=1,
    )


class FrequencyCap(proto.Message):
    r"""A rule specifying the maximum number of times an ad can be
    shown to a user over a particular time period.

    Attributes:
        impressions (int):
            Required. The number of impressions,
            inclusive.
        time_unit (google.ads.googleads.v22.enums.types.FrequencyCapTimeUnitEnum.FrequencyCapTimeUnit):
            Required. The type of time unit.
    """

    impressions: int = proto.Field(
        proto.INT32,
        number=3,
    )
    time_unit: (
        frequency_cap_time_unit.FrequencyCapTimeUnitEnum.FrequencyCapTimeUnit
    ) = proto.Field(
        proto.ENUM,
        number=2,
        enum=frequency_cap_time_unit.FrequencyCapTimeUnitEnum.FrequencyCapTimeUnit,
    )


class Targeting(proto.Message):
    r"""The targeting for which traffic metrics will be reported.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        plannable_location_id (str):
            The ID of the selected location. Plannable location IDs can
            be obtained from
            [ReachPlanService.ListPlannableLocations][google.ads.googleads.v22.services.ReachPlanService.ListPlannableLocations].

            Requests must set either this field or
            ``plannable_location_ids``.

            This field is deprecated as of V12 and will be removed in a
            future release. Use ``plannable_location_ids`` instead.

            This field is a member of `oneof`_ ``_plannable_location_id``.
        plannable_location_ids (MutableSequence[str]):
            The list of plannable location IDs to target with this
            forecast.

            If more than one ID is provided, all IDs must have the same
            ``parent_country_id``. Planning for more than
            ``parent_county`` is not supported. Plannable location IDs
            and their ``parent_country_id`` can be obtained from
            [ReachPlanService.ListPlannableLocations][google.ads.googleads.v22.services.ReachPlanService.ListPlannableLocations].

            Requests must set either this field or
            ``plannable_location_id``.
        age_range (google.ads.googleads.v22.enums.types.ReachPlanAgeRangeEnum.ReachPlanAgeRange):
            Targeted age range.
            An unset value is equivalent to targeting all
            ages.
        genders (MutableSequence[google.ads.googleads.v22.common.types.GenderInfo]):
            Targeted genders.
            An unset value is equivalent to targeting MALE
            and FEMALE.
        devices (MutableSequence[google.ads.googleads.v22.common.types.DeviceInfo]):
            Targeted devices. If not specified, targets all applicable
            devices. Applicable devices vary by product and region and
            can be obtained from
            [ReachPlanService.ListPlannableProducts][google.ads.googleads.v22.services.ReachPlanService.ListPlannableProducts].
        network (google.ads.googleads.v22.enums.types.ReachPlanNetworkEnum.ReachPlanNetwork):
            Targetable network for the ad product. If not specified,
            targets all applicable networks. Applicable networks vary by
            product and region and can be obtained from
            [ReachPlanService.ListPlannableProducts][google.ads.googleads.v22.services.ReachPlanService.ListPlannableProducts].
        audience_targeting (google.ads.googleads.v22.services.types.AudienceTargeting):
            Targeted audiences.
            If not specified, does not target any specific
            audience.
    """

    plannable_location_id: str = proto.Field(
        proto.STRING,
        number=6,
        optional=True,
    )
    plannable_location_ids: MutableSequence[str] = proto.RepeatedField(
        proto.STRING,
        number=8,
    )
    age_range: reach_plan_age_range.ReachPlanAgeRangeEnum.ReachPlanAgeRange = (
        proto.Field(
            proto.ENUM,
            number=2,
            enum=reach_plan_age_range.ReachPlanAgeRangeEnum.ReachPlanAgeRange,
        )
    )
    genders: MutableSequence[criteria.GenderInfo] = proto.RepeatedField(
        proto.MESSAGE,
        number=3,
        message=criteria.GenderInfo,
    )
    devices: MutableSequence[criteria.DeviceInfo] = proto.RepeatedField(
        proto.MESSAGE,
        number=4,
        message=criteria.DeviceInfo,
    )
    network: reach_plan_network.ReachPlanNetworkEnum.ReachPlanNetwork = (
        proto.Field(
            proto.ENUM,
            number=5,
            enum=reach_plan_network.ReachPlanNetworkEnum.ReachPlanNetwork,
        )
    )
    audience_targeting: "AudienceTargeting" = proto.Field(
        proto.MESSAGE,
        number=7,
        message="AudienceTargeting",
    )


class CampaignDuration(proto.Message):
    r"""The duration of a planned campaign.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        duration_in_days (int):
            The duration value in days.

            This field cannot be combined with the date_range field.

            This field is a member of `oneof`_ ``_duration_in_days``.
        date_range (google.ads.googleads.v22.common.types.DateRange):
            Date range of the campaign. Dates are in the yyyy-mm-dd
            format and inclusive. The end date must be < 1 year in the
            future and the date range must be <= 92 days long.

            This field cannot be combined with the duration_in_days
            field.
    """

    duration_in_days: int = proto.Field(
        proto.INT32,
        number=2,
        optional=True,
    )
    date_range: dates.DateRange = proto.Field(
        proto.MESSAGE,
        number=3,
        message=dates.DateRange,
    )


class PlannedProduct(proto.Message):
    r"""A product being planned for reach.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        plannable_product_code (str):
            Required. Selected product for planning. The code associated
            with the ad product (for example: Trueview, Bumper). To list
            the available plannable product codes use
            [ReachPlanService.ListPlannableProducts][google.ads.googleads.v22.services.ReachPlanService.ListPlannableProducts].

            This field is a member of `oneof`_ ``_plannable_product_code``.
        budget_micros (int):
            Required. Maximum budget allocation in micros for the
            selected product. The value is specified in the selected
            planning currency_code. For example: 1 000 000$ = 1 000 000
            000 000 micros.

            This field is a member of `oneof`_ ``_budget_micros``.
        conversion_rate (float):
            Conversion rate as a decimal between 0 and 1, exclusive. For
            example: if 2% of ad interactions are expected to lead to
            conversions, conversion_rate should be 0.02. This field is
            required for DEMAND_GEN plannable products. It is not
            supported for other plannable products.

            This field is a member of `oneof`_ ``_conversion_rate``.
        advanced_product_targeting (google.ads.googleads.v22.services.types.AdvancedProductTargeting):
            Targeting settings for the selected product. To list the
            available targeting for each product use
            [ReachPlanService.ListPlannableProducts][google.ads.googleads.v22.services.ReachPlanService.ListPlannableProducts].
    """

    plannable_product_code: str = proto.Field(
        proto.STRING,
        number=3,
        optional=True,
    )
    budget_micros: int = proto.Field(
        proto.INT64,
        number=4,
        optional=True,
    )
    conversion_rate: float = proto.Field(
        proto.DOUBLE,
        number=6,
        optional=True,
    )
    advanced_product_targeting: "AdvancedProductTargeting" = proto.Field(
        proto.MESSAGE,
        number=5,
        message="AdvancedProductTargeting",
    )


class GenerateReachForecastResponse(proto.Message):
    r"""Response message containing the generated reach curve.

    Attributes:
        on_target_audience_metrics (google.ads.googleads.v22.services.types.OnTargetAudienceMetrics):
            Reference on target audiences for this curve.
        reach_curve (google.ads.googleads.v22.services.types.ReachCurve):
            The generated reach curve for the planned
            product mix.
    """

    on_target_audience_metrics: "OnTargetAudienceMetrics" = proto.Field(
        proto.MESSAGE,
        number=1,
        message="OnTargetAudienceMetrics",
    )
    reach_curve: "ReachCurve" = proto.Field(
        proto.MESSAGE,
        number=2,
        message="ReachCurve",
    )


class ReachCurve(proto.Message):
    r"""The reach curve for the planned products.

    Attributes:
        reach_forecasts (MutableSequence[google.ads.googleads.v22.services.types.ReachForecast]):
            All points on the reach curve.
    """

    reach_forecasts: MutableSequence["ReachForecast"] = proto.RepeatedField(
        proto.MESSAGE,
        number=1,
        message="ReachForecast",
    )


class ReachForecast(proto.Message):
    r"""A point on reach curve.

    Attributes:
        cost_micros (int):
            The cost in micros.
        forecast (google.ads.googleads.v22.services.types.Forecast):
            Forecasted traffic metrics for this point.
        planned_product_reach_forecasts (MutableSequence[google.ads.googleads.v22.services.types.PlannedProductReachForecast]):
            The forecasted allocation and traffic metrics
            for each planned product at this point on the
            reach curve.
    """

    cost_micros: int = proto.Field(
        proto.INT64,
        number=5,
    )
    forecast: "Forecast" = proto.Field(
        proto.MESSAGE,
        number=2,
        message="Forecast",
    )
    planned_product_reach_forecasts: MutableSequence[
        "PlannedProductReachForecast"
    ] = proto.RepeatedField(
        proto.MESSAGE,
        number=4,
        message="PlannedProductReachForecast",
    )


class Forecast(proto.Message):
    r"""Forecasted traffic metrics for the planned products and
    targeting.


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        on_target_reach (int):
            Number of unique people reached at least
            GenerateReachForecastRequest.min_effective_frequency or
            GenerateReachForecastRequest.effective_frequency_limit times
            that exactly matches the Targeting.

            Note that a minimum number of unique people must be reached
            in order for data to be reported. If the minimum number is
            not met, the on_target_reach value will be rounded to 0.

            This field is a member of `oneof`_ ``_on_target_reach``.
        total_reach (int):
            Total number of unique people reached at least
            GenerateReachForecastRequest.min_effective_frequency or
            GenerateReachForecastRequest.effective_frequency_limit
            times. This includes people that may fall outside the
            specified Targeting.

            Note that a minimum number of unique people must be reached
            in order for data to be reported. If the minimum number is
            not met, the total_reach value will be rounded to 0.

            This field is a member of `oneof`_ ``_total_reach``.
        on_target_impressions (int):
            Number of ad impressions that exactly matches
            the Targeting.

            This field is a member of `oneof`_ ``_on_target_impressions``.
        total_impressions (int):
            Total number of ad impressions. This includes
            impressions that may fall outside the specified
            Targeting, due to insufficient information on
            signed-in users.

            This field is a member of `oneof`_ ``_total_impressions``.
        viewable_impressions (int):
            Number of times the ad's impressions were
            considered viewable. See
            https://support.google.com/google-ads/answer/7029393
            for more information about what makes an ad
            viewable and how viewability is measured.

            This field is a member of `oneof`_ ``_viewable_impressions``.
        effective_frequency_breakdowns (MutableSequence[google.ads.googleads.v22.services.types.EffectiveFrequencyBreakdown]):
            A list of effective frequency forecasts. The list is ordered
            starting with 1+ and ending with the value set in
            GenerateReachForecastRequest.effective_frequency_limit. If
            no effective_frequency_limit was set, this list will be
            empty.
        on_target_coview_reach (int):
            Number of unique people reached that exactly
            matches the Targeting including co-viewers.

            This field is a member of `oneof`_ ``_on_target_coview_reach``.
        total_coview_reach (int):
            Number of unique people reached including
            co-viewers. This includes people that may fall
            outside the specified Targeting.

            This field is a member of `oneof`_ ``_total_coview_reach``.
        on_target_coview_impressions (int):
            Number of ad impressions that exactly matches
            the Targeting including co-viewers.

            This field is a member of `oneof`_ ``_on_target_coview_impressions``.
        total_coview_impressions (int):
            Total number of ad impressions including
            co-viewers. This includes impressions that may
            fall outside the specified Targeting, due to
            insufficient information on signed-in users.

            This field is a member of `oneof`_ ``_total_coview_impressions``.
        conversions (float):
            The number of conversions. This metric is only available for
            DEMAND_GEN plannable products.

            See https://support.google.com/google-ads/answer/2375431 for
            more information on conversions.

            This field is a member of `oneof`_ ``_conversions``.
        trueview_views (int):
            Number of ad views forecasted for the
            specified product and targeting. A TrueView View
            is counted when a viewer views a larger portion
            or the entirety of an ad beyond an impression.

            See
            https://support.google.com/google-ads/answer/2375431
            for more information on TrueView Views.

            This field is a member of `oneof`_ ``_trueview_views``.
    """

    on_target_reach: int = proto.Field(
        proto.INT64,
        number=5,
        optional=True,
    )
    total_reach: int = proto.Field(
        proto.INT64,
        number=6,
        optional=True,
    )
    on_target_impressions: int = proto.Field(
        proto.INT64,
        number=7,
        optional=True,
    )
    total_impressions: int = proto.Field(
        proto.INT64,
        number=8,
        optional=True,
    )
    viewable_impressions: int = proto.Field(
        proto.INT64,
        number=9,
        optional=True,
    )
    effective_frequency_breakdowns: MutableSequence[
        "EffectiveFrequencyBreakdown"
    ] = proto.RepeatedField(
        proto.MESSAGE,
        number=10,
        message="EffectiveFrequencyBreakdown",
    )
    on_target_coview_reach: int = proto.Field(
        proto.INT64,
        number=11,
        optional=True,
    )
    total_coview_reach: int = proto.Field(
        proto.INT64,
        number=12,
        optional=True,
    )
    on_target_coview_impressions: int = proto.Field(
        proto.INT64,
        number=13,
        optional=True,
    )
    total_coview_impressions: int = proto.Field(
        proto.INT64,
        number=14,
        optional=True,
    )
    conversions: float = proto.Field(
        proto.DOUBLE,
        number=16,
        optional=True,
    )
    trueview_views: int = proto.Field(
        proto.INT64,
        number=17,
        optional=True,
    )


class PlannedProductReachForecast(proto.Message):
    r"""The forecasted allocation and traffic metrics for a specific
    product at a point on the reach curve.

    Attributes:
        plannable_product_code (str):
            Selected product for planning. The product
            codes returned are within the set of the ones
            returned by ListPlannableProducts when using the
            same location ID.
        cost_micros (int):
            The cost in micros. This may differ from the
            product's input allocation if one or more
            planned products cannot fulfill the budget
            because of limited inventory.
        planned_product_forecast (google.ads.googleads.v22.services.types.PlannedProductForecast):
            Forecasted traffic metrics for this product.
    """

    plannable_product_code: str = proto.Field(
        proto.STRING,
        number=1,
    )
    cost_micros: int = proto.Field(
        proto.INT64,
        number=2,
    )
    planned_product_forecast: "PlannedProductForecast" = proto.Field(
        proto.MESSAGE,
        number=3,
        message="PlannedProductForecast",
    )


class PlannedProductForecast(proto.Message):
    r"""Forecasted traffic metrics for a planned product.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        on_target_reach (int):
            Number of unique people reached that exactly matches the
            Targeting.

            Note that a minimum number of unique people must be reached
            in order for data to be reported. If the minimum number is
            not met, the on_target_reach value will be rounded to 0.
        total_reach (int):
            Number of unique people reached. This includes people that
            may fall outside the specified Targeting.

            Note that a minimum number of unique people must be reached
            in order for data to be reported. If the minimum number is
            not met, the total_reach value will be rounded to 0.
        on_target_impressions (int):
            Number of ad impressions that exactly matches
            the Targeting.
        total_impressions (int):
            Total number of ad impressions. This includes
            impressions that may fall outside the specified
            Targeting, due to insufficient information on
            signed-in users.
        viewable_impressions (int):
            Number of times the ad's impressions were
            considered viewable. See
            https://support.google.com/google-ads/answer/7029393
            for more information about what makes an ad
            viewable and how viewability is measured.

            This field is a member of `oneof`_ ``_viewable_impressions``.
        on_target_coview_reach (int):
            Number of unique people reached that exactly
            matches the Targeting including co-viewers.

            This field is a member of `oneof`_ ``_on_target_coview_reach``.
        total_coview_reach (int):
            Number of unique people reached including
            co-viewers. This includes people that may fall
            outside the specified Targeting.

            This field is a member of `oneof`_ ``_total_coview_reach``.
        on_target_coview_impressions (int):
            Number of ad impressions that exactly matches
            the Targeting including co-viewers.

            This field is a member of `oneof`_ ``_on_target_coview_impressions``.
        total_coview_impressions (int):
            Total number of ad impressions including
            co-viewers. This includes impressions that may
            fall outside the specified Targeting, due to
            insufficient information on signed-in users.

            This field is a member of `oneof`_ ``_total_coview_impressions``.
        average_frequency (float):
            The number of times per selected time unit a
            user will see an ad, averaged over the number of
            time units in the forecast length. This field
            will only be populated for a Target Frequency
            campaign.

            See
            https://support.google.com/google-ads/answer/12400225
            for more information about Target Frequency
            campaigns.

            This field is a member of `oneof`_ ``_average_frequency``.
        conversions (float):
            The number of conversions. This metric is only available for
            DEMAND_GEN plannable products.

            See https://support.google.com/google-ads/answer/2375431 for
            more information on conversions.

            This field is a member of `oneof`_ ``_conversions``.
        trueview_views (int):
            Number of ad views forecasted for the
            specified product and targeting. A TrueView View
            is counted when a viewer views a larger portion
            or the entirety of an ad beyond an impression.

            See
            https://support.google.com/google-ads/answer/2375431
            for more information on TrueView Views.

            This field is a member of `oneof`_ ``_trueview_views``.
    """

    on_target_reach: int = proto.Field(
        proto.INT64,
        number=1,
    )
    total_reach: int = proto.Field(
        proto.INT64,
        number=2,
    )
    on_target_impressions: int = proto.Field(
        proto.INT64,
        number=3,
    )
    total_impressions: int = proto.Field(
        proto.INT64,
        number=4,
    )
    viewable_impressions: int = proto.Field(
        proto.INT64,
        number=5,
        optional=True,
    )
    on_target_coview_reach: int = proto.Field(
        proto.INT64,
        number=6,
        optional=True,
    )
    total_coview_reach: int = proto.Field(
        proto.INT64,
        number=7,
        optional=True,
    )
    on_target_coview_impressions: int = proto.Field(
        proto.INT64,
        number=8,
        optional=True,
    )
    total_coview_impressions: int = proto.Field(
        proto.INT64,
        number=9,
        optional=True,
    )
    average_frequency: float = proto.Field(
        proto.DOUBLE,
        number=10,
        optional=True,
    )
    conversions: float = proto.Field(
        proto.DOUBLE,
        number=12,
        optional=True,
    )
    trueview_views: int = proto.Field(
        proto.INT64,
        number=13,
        optional=True,
    )


class OnTargetAudienceMetrics(proto.Message):
    r"""Audience metrics for the planned products. These metrics consider
    the following targeting dimensions:

    - Location
    - PlannableAgeRange
    - Gender
    - AudienceTargeting (only for youtube_audience_size)


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        youtube_audience_size (int):
            Reference audience size matching the
            considered targeting for YouTube.

            This field is a member of `oneof`_ ``_youtube_audience_size``.
        census_audience_size (int):
            Reference audience size matching the
            considered targeting for Census.

            This field is a member of `oneof`_ ``_census_audience_size``.
    """

    youtube_audience_size: int = proto.Field(
        proto.INT64,
        number=3,
        optional=True,
    )
    census_audience_size: int = proto.Field(
        proto.INT64,
        number=4,
        optional=True,
    )


class EffectiveFrequencyBreakdown(proto.Message):
    r"""A breakdown of the number of unique people reached at a given
    effective frequency.


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        effective_frequency (int):
            The effective frequency [1-10].
        on_target_reach (int):
            The number of unique people reached at least
            effective_frequency times that exactly matches the
            Targeting.

            Note that a minimum number of unique people must be reached
            in order for data to be reported. If the minimum number is
            not met, the on_target_reach value will be rounded to 0.
        total_reach (int):
            Total number of unique people reached at least
            effective_frequency times. This includes people that may
            fall outside the specified Targeting.

            Note that a minimum number of unique people must be reached
            in order for data to be reported. If the minimum number is
            not met, the total_reach value will be rounded to 0.
        effective_coview_reach (int):
            The number of users (including co-viewing users) reached for
            the associated effective_frequency value.

            This field is a member of `oneof`_ ``_effective_coview_reach``.
        on_target_effective_coview_reach (int):
            The number of users (including co-viewing users) reached for
            the associated effective_frequency value within the
            specified plan demographic.

            This field is a member of `oneof`_ ``_on_target_effective_coview_reach``.
    """

    effective_frequency: int = proto.Field(
        proto.INT32,
        number=1,
    )
    on_target_reach: int = proto.Field(
        proto.INT64,
        number=2,
    )
    total_reach: int = proto.Field(
        proto.INT64,
        number=3,
    )
    effective_coview_reach: int = proto.Field(
        proto.INT64,
        number=4,
        optional=True,
    )
    on_target_effective_coview_reach: int = proto.Field(
        proto.INT64,
        number=5,
        optional=True,
    )


class ForecastMetricOptions(proto.Message):
    r"""Controls forecast metrics to return.

    Attributes:
        include_coview (bool):
            Indicates whether to include co-view metrics
            in the response forecast.
    """

    include_coview: bool = proto.Field(
        proto.BOOL,
        number=1,
    )


class AudienceTargeting(proto.Message):
    r"""Audience targeting for reach forecast.

    Attributes:
        user_interest (MutableSequence[google.ads.googleads.v22.common.types.UserInterestInfo]):
            List of audiences based on user interests to
            be targeted.
        user_lists (MutableSequence[google.ads.googleads.v22.common.types.UserListInfo]):
            List of audiences based on user lists to be
            targeted.
    """

    user_interest: MutableSequence[criteria.UserInterestInfo] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=1,
            message=criteria.UserInterestInfo,
        )
    )
    user_lists: MutableSequence[criteria.UserListInfo] = proto.RepeatedField(
        proto.MESSAGE,
        number=2,
        message=criteria.UserListInfo,
    )


class AdvancedProductTargeting(proto.Message):
    r"""Advanced targeting settings for products.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        surface_targeting_settings (google.ads.googleads.v22.services.types.SurfaceTargeting):
            Surface targeting settings for this product.
        target_frequency_settings (google.ads.googleads.v22.services.types.TargetFrequencySettings):
            Settings for a Target frequency campaign. Must be set when
            selecting the TARGET_FREQUENCY product.

            See https://support.google.com/google-ads/answer/12400225
            for more information about Target Frequency campaigns.
        youtube_select_settings (google.ads.googleads.v22.services.types.YouTubeSelectSettings):
            Settings for YouTube Select targeting.

            This field is a member of `oneof`_ ``advanced_targeting``.
    """

    surface_targeting_settings: "SurfaceTargeting" = proto.Field(
        proto.MESSAGE,
        number=2,
        message="SurfaceTargeting",
    )
    target_frequency_settings: "TargetFrequencySettings" = proto.Field(
        proto.MESSAGE,
        number=3,
        message="TargetFrequencySettings",
    )
    youtube_select_settings: "YouTubeSelectSettings" = proto.Field(
        proto.MESSAGE,
        number=1,
        oneof="advanced_targeting",
        message="YouTubeSelectSettings",
    )


class YouTubeSelectSettings(proto.Message):
    r"""Request settings for YouTube Select Lineups

    Attributes:
        lineup_id (int):
            Lineup for YouTube Select Targeting.
    """

    lineup_id: int = proto.Field(
        proto.INT64,
        number=1,
    )


class YouTubeSelectLineUp(proto.Message):
    r"""A Plannable YouTube Select Lineup for product targeting.

    Attributes:
        lineup_id (int):
            The ID of the YouTube Select Lineup.
        lineup_name (str):
            The unique name of the YouTube Select Lineup.
    """

    lineup_id: int = proto.Field(
        proto.INT64,
        number=1,
    )
    lineup_name: str = proto.Field(
        proto.STRING,
        number=2,
    )


class SurfaceTargetingCombinations(proto.Message):
    r"""The surface targeting combinations available for an ad
    product.

    Attributes:
        default_targeting (google.ads.googleads.v22.services.types.SurfaceTargeting):
            Default surface targeting applied to the ad
            product.
        available_targeting_combinations (MutableSequence[google.ads.googleads.v22.services.types.SurfaceTargeting]):
            Available surface target combinations for the
            ad product.
    """

    default_targeting: "SurfaceTargeting" = proto.Field(
        proto.MESSAGE,
        number=1,
        message="SurfaceTargeting",
    )
    available_targeting_combinations: MutableSequence["SurfaceTargeting"] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=2,
            message="SurfaceTargeting",
        )
    )


class SurfaceTargeting(proto.Message):
    r"""Container for surfaces for a product. Surfaces refer to the
    available types of ad inventories such as In-Feed, In-Stream,
    and Shorts.

    Attributes:
        surfaces (MutableSequence[google.ads.googleads.v22.enums.types.ReachPlanSurfaceEnum.ReachPlanSurface]):
            List of surfaces available to target.
    """

    surfaces: MutableSequence[
        reach_plan_surface.ReachPlanSurfaceEnum.ReachPlanSurface
    ] = proto.RepeatedField(
        proto.ENUM,
        number=1,
        enum=reach_plan_surface.ReachPlanSurfaceEnum.ReachPlanSurface,
    )


class TargetFrequencySettings(proto.Message):
    r"""Target Frequency settings for a supported product.

    Attributes:
        time_unit (google.ads.googleads.v22.enums.types.TargetFrequencyTimeUnitEnum.TargetFrequencyTimeUnit):
            Required. The time unit used to describe the time frame for
            target_frequency.
        target_frequency (int):
            Required. The target frequency goal per
            selected time unit.
    """

    time_unit: (
        target_frequency_time_unit.TargetFrequencyTimeUnitEnum.TargetFrequencyTimeUnit
    ) = proto.Field(
        proto.ENUM,
        number=1,
        enum=target_frequency_time_unit.TargetFrequencyTimeUnitEnum.TargetFrequencyTimeUnit,
    )
    target_frequency: int = proto.Field(
        proto.INT32,
        number=2,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
