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

from google.ads.googleads.v23.common.types import additional_application_info
from google.ads.googleads.v23.common.types import criteria
from google.ads.googleads.v23.common.types import dates as gagc_dates
from google.ads.googleads.v23.enums.types import benchmarks_marketing_objective
from google.ads.googleads.v23.enums.types import (
    benchmarks_source_type as gage_benchmarks_source_type,
)
from google.ads.googleads.v23.enums.types import benchmarks_time_granularity


__protobuf__ = proto.module(
    package="google.ads.googleads.v23.services",
    marshal="google.ads.googleads.v23",
    manifest={
        "ListBenchmarksAvailableDatesRequest",
        "ListBenchmarksAvailableDatesResponse",
        "ListBenchmarksLocationsRequest",
        "ListBenchmarksLocationsResponse",
        "BenchmarksLocation",
        "ListBenchmarksProductsRequest",
        "ListBenchmarksProductsResponse",
        "BenchmarksProductMetadata",
        "ListBenchmarksSourcesRequest",
        "ListBenchmarksSourcesResponse",
        "BenchmarksSourceMetadata",
        "IndustryVerticalInfo",
        "GenerateBenchmarksMetricsRequest",
        "BenchmarksSource",
        "ProductFilter",
        "BreakdownDefinition",
        "GenerateBenchmarksMetricsResponse",
        "BreakdownMetrics",
        "BreakdownKey",
        "Metrics",
        "RateMetrics",
    },
)


class ListBenchmarksAvailableDatesRequest(proto.Message):
    r"""Request message for
    [BenchmarksService.ListBenchmarksAvailableDates][google.ads.googleads.v23.services.BenchmarksService.ListBenchmarksAvailableDates].

    Attributes:
        application_info (google.ads.googleads.v23.common.types.AdditionalApplicationInfo):
            Additional information on the application
            issuing the request.
    """

    application_info: additional_application_info.AdditionalApplicationInfo = (
        proto.Field(
            proto.MESSAGE,
            number=1,
            message=additional_application_info.AdditionalApplicationInfo,
        )
    )


class ListBenchmarksAvailableDatesResponse(proto.Message):
    r"""Response message for
    [BenchmarksService.ListBenchmarksAvailableDates][google.ads.googleads.v23.services.BenchmarksService.ListBenchmarksAvailableDates].

    Attributes:
        supported_dates (google.ads.googleads.v23.common.types.DateRange):
            The dates that support benchmarks metrics.
            Data is supported for any dates within this date
            range inclusive.
    """

    supported_dates: gagc_dates.DateRange = proto.Field(
        proto.MESSAGE,
        number=1,
        message=gagc_dates.DateRange,
    )


class ListBenchmarksLocationsRequest(proto.Message):
    r"""Request message for
    [BenchmarksService.ListBenchmarksLocations][google.ads.googleads.v23.services.BenchmarksService.ListBenchmarksLocations].

    Attributes:
        application_info (google.ads.googleads.v23.common.types.AdditionalApplicationInfo):
            Additional information on the application
            issuing the request.
    """

    application_info: additional_application_info.AdditionalApplicationInfo = (
        proto.Field(
            proto.MESSAGE,
            number=1,
            message=additional_application_info.AdditionalApplicationInfo,
        )
    )


class ListBenchmarksLocationsResponse(proto.Message):
    r"""Response message for
    [BenchmarksService.ListBenchmarksLocations][google.ads.googleads.v23.services.BenchmarksService.ListBenchmarksLocations].

    Attributes:
        benchmarks_locations (MutableSequence[google.ads.googleads.v23.services.types.BenchmarksLocation]):
            The list of locations supported for
            benchmarks data.
    """

    benchmarks_locations: MutableSequence["BenchmarksLocation"] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=1,
            message="BenchmarksLocation",
        )
    )


class BenchmarksLocation(proto.Message):
    r"""A location that supports benchmarks data.

    Attributes:
        location_name (str):
            The unique location name in English.
        location_type (str):
            The location's type. Location types correspond to
            target_type returned by searching location type in
            GoogleAdsService.Search/SearchStream.
        location_info (google.ads.googleads.v23.common.types.LocationInfo):
            Information on the geographic location,
            including the location ID.
    """

    location_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    location_type: str = proto.Field(
        proto.STRING,
        number=2,
    )
    location_info: criteria.LocationInfo = proto.Field(
        proto.MESSAGE,
        number=3,
        message=criteria.LocationInfo,
    )


class ListBenchmarksProductsRequest(proto.Message):
    r"""Request message for
    [BenchmarksService.ListBenchmarksProducts][google.ads.googleads.v23.services.BenchmarksService.ListBenchmarksProducts].

    Attributes:
        application_info (google.ads.googleads.v23.common.types.AdditionalApplicationInfo):
            Additional information on the application
            issuing the request.
    """

    application_info: additional_application_info.AdditionalApplicationInfo = (
        proto.Field(
            proto.MESSAGE,
            number=1,
            message=additional_application_info.AdditionalApplicationInfo,
        )
    )


class ListBenchmarksProductsResponse(proto.Message):
    r"""Response message for
    [BenchmarksService.ListBenchmarksProducts][google.ads.googleads.v23.services.BenchmarksService.ListBenchmarksProducts].

    Attributes:
        benchmarks_products (MutableSequence[google.ads.googleads.v23.services.types.BenchmarksProductMetadata]):
            The list of products available for benchmarks
            data.
    """

    benchmarks_products: MutableSequence["BenchmarksProductMetadata"] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=1,
            message="BenchmarksProductMetadata",
        )
    )


class BenchmarksProductMetadata(proto.Message):
    r"""The metadata associated with a product supported for
    benchmarks data.

    Attributes:
        product_name (str):
            The name of the product.
        product_code (str):
            The identifier of the product. The identifier can be used as
            inputs for
            [BenchmarksService.GenerateBenchmarksMetrics][google.ads.googleads.v23.services.BenchmarksService.GenerateBenchmarksMetrics].
        marketing_objective (google.ads.googleads.v23.enums.types.BenchmarksMarketingObjectiveEnum.BenchmarksMarketingObjective):
            The marketing objective associated with the
            product. A marketing objective is a broader
            classification of products.
    """

    product_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    product_code: str = proto.Field(
        proto.STRING,
        number=2,
    )
    marketing_objective: (
        benchmarks_marketing_objective.BenchmarksMarketingObjectiveEnum.BenchmarksMarketingObjective
    ) = proto.Field(
        proto.ENUM,
        number=3,
        enum=benchmarks_marketing_objective.BenchmarksMarketingObjectiveEnum.BenchmarksMarketingObjective,
    )


class ListBenchmarksSourcesRequest(proto.Message):
    r"""Request message for
    [BenchmarksService.ListBenchmarksSources][google.ads.googleads.v23.services.BenchmarksService.ListBenchmarksSources].

    Attributes:
        benchmarks_sources (MutableSequence[google.ads.googleads.v23.enums.types.BenchmarksSourceTypeEnum.BenchmarksSourceType]):
            Required. The types of benchmarks sources to be returned
            (for example, INDUSTRY_VERTICAL).
        application_info (google.ads.googleads.v23.common.types.AdditionalApplicationInfo):
            Additional information on the application
            issuing the request.
    """

    benchmarks_sources: MutableSequence[
        gage_benchmarks_source_type.BenchmarksSourceTypeEnum.BenchmarksSourceType
    ] = proto.RepeatedField(
        proto.ENUM,
        number=1,
        enum=gage_benchmarks_source_type.BenchmarksSourceTypeEnum.BenchmarksSourceType,
    )
    application_info: additional_application_info.AdditionalApplicationInfo = (
        proto.Field(
            proto.MESSAGE,
            number=2,
            message=additional_application_info.AdditionalApplicationInfo,
        )
    )


class ListBenchmarksSourcesResponse(proto.Message):
    r"""Response message for
    [BenchmarksService.ListBenchmarksSources][google.ads.googleads.v23.services.BenchmarksService.ListBenchmarksSources].

    Attributes:
        benchmarks_sources (MutableSequence[google.ads.googleads.v23.services.types.BenchmarksSourceMetadata]):
            The list of available source used to generate
            benchmarks data for.
    """

    benchmarks_sources: MutableSequence["BenchmarksSourceMetadata"] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=1,
            message="BenchmarksSourceMetadata",
        )
    )


class BenchmarksSourceMetadata(proto.Message):
    r"""The metadata associated with a benchmarks source.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        benchmarks_source_type (google.ads.googleads.v23.enums.types.BenchmarksSourceTypeEnum.BenchmarksSourceType):
            The type of benchmarks source.
        industry_vertical_info (google.ads.googleads.v23.services.types.IndustryVerticalInfo):
            Information on the Industry Vertical.

            This field is a member of `oneof`_ ``benchmarks_source_info``.
    """

    benchmarks_source_type: (
        gage_benchmarks_source_type.BenchmarksSourceTypeEnum.BenchmarksSourceType
    ) = proto.Field(
        proto.ENUM,
        number=1,
        enum=gage_benchmarks_source_type.BenchmarksSourceTypeEnum.BenchmarksSourceType,
    )
    industry_vertical_info: "IndustryVerticalInfo" = proto.Field(
        proto.MESSAGE,
        number=2,
        oneof="benchmarks_source_info",
        message="IndustryVerticalInfo",
    )


class IndustryVerticalInfo(proto.Message):
    r"""The information associated with an Industry Vertical.

    Attributes:
        industry_vertical_name (str):
            The name of the Industry Vertical.
        industry_vertical_id (int):
            The unique identifier of the Industry
            Vertical.
        parent_industry_vertical_id (int):
            The unique identifier of the parent Industry
            Vertical, if exists.
    """

    industry_vertical_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    industry_vertical_id: int = proto.Field(
        proto.INT64,
        number=2,
    )
    parent_industry_vertical_id: int = proto.Field(
        proto.INT64,
        number=3,
    )


class GenerateBenchmarksMetricsRequest(proto.Message):
    r"""Request message for
    [BenchmarksService.GenerateBenchmarksMetrics][google.ads.googleads.v23.services.BenchmarksService.GenerateBenchmarksMetrics].

    Attributes:
        customer_id (str):
            Required. The ID of the customer. Supply a
            client customer ID to generate metrics for the
            customer. A manager account customer ID will not
            return customer metrics since it does not have
            any associated direct ad campaigns.
        date_range (google.ads.googleads.v23.common.types.DateRange):
            The date range to aggregate metrics over. If unset, data
            will be returned for the most recent quarter for which data
            is available. Dates can be retrieved using
            [BenchmarksService.ListBenchmarksAvailableDates][google.ads.googleads.v23.services.BenchmarksService.ListBenchmarksAvailableDates].
        location (google.ads.googleads.v23.common.types.LocationInfo):
            Required. The location to generate benchmarks
            metrics for.
        benchmarks_source (google.ads.googleads.v23.services.types.BenchmarksSource):
            Required. The source used to generate
            benchmarks metrics for.
        product_filter (google.ads.googleads.v23.services.types.ProductFilter):
            Required. The products to aggregate metrics
            over. Product filter settings support a list of
            product IDs or a list of marketing objectives.
        breakdown_definition (google.ads.googleads.v23.services.types.BreakdownDefinition):
            Optional. The set of dimensions to group
            metrics by. If multiple dimensions are selected,
            cross-dimension breakdowns are returned.
        currency_code (str):
            Optional. The three-character ISO 4217
            currency code. If unspecified, the default
            currency for monetary values is USD.
        customer_benchmarks_group (str):
            The name of the customer being planned for.
            This is a user-defined value.
        application_info (google.ads.googleads.v23.common.types.AdditionalApplicationInfo):
            Additional information on the application
            issuing the request.
    """

    customer_id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    date_range: gagc_dates.DateRange = proto.Field(
        proto.MESSAGE,
        number=2,
        message=gagc_dates.DateRange,
    )
    location: criteria.LocationInfo = proto.Field(
        proto.MESSAGE,
        number=3,
        message=criteria.LocationInfo,
    )
    benchmarks_source: "BenchmarksSource" = proto.Field(
        proto.MESSAGE,
        number=4,
        message="BenchmarksSource",
    )
    product_filter: "ProductFilter" = proto.Field(
        proto.MESSAGE,
        number=5,
        message="ProductFilter",
    )
    breakdown_definition: "BreakdownDefinition" = proto.Field(
        proto.MESSAGE,
        number=9,
        message="BreakdownDefinition",
    )
    currency_code: str = proto.Field(
        proto.STRING,
        number=6,
    )
    customer_benchmarks_group: str = proto.Field(
        proto.STRING,
        number=7,
    )
    application_info: additional_application_info.AdditionalApplicationInfo = (
        proto.Field(
            proto.MESSAGE,
            number=8,
            message=additional_application_info.AdditionalApplicationInfo,
        )
    )


class BenchmarksSource(proto.Message):
    r"""The source used to generate benchmarks metrics for. The ID of the
    source can be obtained from
    [BenchmarksService.ListBenchmarksSources][google.ads.googleads.v23.services.BenchmarksService.ListBenchmarksSources].


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        industry_vertical_id (int):
            The ID of the Industry Vertical.

            This field is a member of `oneof`_ ``benchmarks_source_id``.
    """

    industry_vertical_id: int = proto.Field(
        proto.INT64,
        number=1,
        oneof="benchmarks_source_id",
    )


class ProductFilter(proto.Message):
    r"""The type and list of products to aggregate benchmarks metrics
    over.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        product_list (google.ads.googleads.v23.services.types.ProductFilter.ProductList):
            The list of products.

            This field is a member of `oneof`_ ``filter_settings``.
        marketing_objective_list (google.ads.googleads.v23.services.types.ProductFilter.MarketingObjectiveList):
            The list of marketing goals. Marketing
            objective is a broader product classification of
            products.

            This field is a member of `oneof`_ ``filter_settings``.
    """

    class ProductList(proto.Message):
        r"""The list of products to generate benchmarks metrics for.

        Attributes:
            product_codes (MutableSequence[str]):
                Required. Products to generate benchmarks
                metrics for.
        """

        product_codes: MutableSequence[str] = proto.RepeatedField(
            proto.STRING,
            number=1,
        )

    class MarketingObjectiveList(proto.Message):
        r"""The list of marketing objectives to generate benchmarks
        metrics for.

        Attributes:
            marketing_objectives (MutableSequence[google.ads.googleads.v23.enums.types.BenchmarksMarketingObjectiveEnum.BenchmarksMarketingObjective]):
                Required. Marketing objectives to generate
                benchmarks metrics for.
        """

        marketing_objectives: MutableSequence[
            benchmarks_marketing_objective.BenchmarksMarketingObjectiveEnum.BenchmarksMarketingObjective
        ] = proto.RepeatedField(
            proto.ENUM,
            number=1,
            enum=benchmarks_marketing_objective.BenchmarksMarketingObjectiveEnum.BenchmarksMarketingObjective,
        )

    product_list: ProductList = proto.Field(
        proto.MESSAGE,
        number=1,
        oneof="filter_settings",
        message=ProductList,
    )
    marketing_objective_list: MarketingObjectiveList = proto.Field(
        proto.MESSAGE,
        number=2,
        oneof="filter_settings",
        message=MarketingObjectiveList,
    )


class BreakdownDefinition(proto.Message):
    r"""The set of dimensions to group metrics by.

    Attributes:
        date_breakdown (google.ads.googleads.v23.enums.types.BenchmarksTimeGranularityEnum.BenchmarksTimeGranularity):
            A date breakdown using the selected
            granularity. The effective date range is
            extended to include the full time periods that
            overlap with the selected start and end dates.
            For example, a monthly breakdown with a start
            date of 2025-06-15 will include a breakdown for
            June. Weeks start on Sunday and end on Saturday.
            This is different from the ISO 8601 standard,
            where weeks start on Monday.
    """

    date_breakdown: (
        benchmarks_time_granularity.BenchmarksTimeGranularityEnum.BenchmarksTimeGranularity
    ) = proto.Field(
        proto.ENUM,
        number=1,
        enum=benchmarks_time_granularity.BenchmarksTimeGranularityEnum.BenchmarksTimeGranularity,
    )


class GenerateBenchmarksMetricsResponse(proto.Message):
    r"""Response message for
    [BenchmarksService.GenerateBenchmarksMetrics][google.ads.googleads.v23.services.BenchmarksService.GenerateBenchmarksMetrics].

    Attributes:
        customer_metrics (google.ads.googleads.v23.services.types.Metrics):
            Metrics belonging to the customer.
        average_benchmarks_metrics (google.ads.googleads.v23.services.types.Metrics):
            Metrics for the selected benchmarks source.
        breakdown_metrics (MutableSequence[google.ads.googleads.v23.services.types.BreakdownMetrics]):
            Breakdown metrics grouped by dimensions.
    """

    customer_metrics: "Metrics" = proto.Field(
        proto.MESSAGE,
        number=1,
        message="Metrics",
    )
    average_benchmarks_metrics: "Metrics" = proto.Field(
        proto.MESSAGE,
        number=2,
        message="Metrics",
    )
    breakdown_metrics: MutableSequence["BreakdownMetrics"] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=3,
            message="BreakdownMetrics",
        )
    )


class BreakdownMetrics(proto.Message):
    r"""Metrics for a given breakdown.

    Attributes:
        breakdown_key (google.ads.googleads.v23.services.types.BreakdownKey):
            Dimensions by which the breakdown metrics are
            grouped by.
        customer_metrics (google.ads.googleads.v23.services.types.Metrics):
            Metrics belonging to the customer.
        average_benchmarks_metrics (google.ads.googleads.v23.services.types.Metrics):
            Metrics for the selected benchmarks source.
    """

    breakdown_key: "BreakdownKey" = proto.Field(
        proto.MESSAGE,
        number=1,
        message="BreakdownKey",
    )
    customer_metrics: "Metrics" = proto.Field(
        proto.MESSAGE,
        number=2,
        message="Metrics",
    )
    average_benchmarks_metrics: "Metrics" = proto.Field(
        proto.MESSAGE,
        number=3,
        message="Metrics",
    )


class BreakdownKey(proto.Message):
    r"""Dimensions by which the breakdown metrics are grouped by.

    Attributes:
        dates (google.ads.googleads.v23.common.types.DateRange):
            Dates used for the breakdown. For example,
            this represents the start and end dates of the
            week for a weekly breakdown.
    """

    dates: gagc_dates.DateRange = proto.Field(
        proto.MESSAGE,
        number=1,
        message=gagc_dates.DateRange,
    )


class Metrics(proto.Message):
    r"""All metrics returned against a criteria.

    Attributes:
        average_rate_metrics (google.ads.googleads.v23.services.types.RateMetrics):
            Average rate metrics calculated by dividing
            one metric by another.
    """

    average_rate_metrics: "RateMetrics" = proto.Field(
        proto.MESSAGE,
        number=1,
        message="RateMetrics",
    )


class RateMetrics(proto.Message):
    r"""Average rate metrics. Metrics that represent monetary values
    are returned in USD by default, if unspecified in the request.

    Attributes:
        average_cpm (float):
            Average cost-per-thousand impressions (CPM).
        average_active_view_cpm (float):
            Average cost-per-thousand viewable
            impressions.
        trueview_average_cpv (float):
            The average TrueView cost-per-view (CPV) is
            defined by the total cost of all ad TrueView
            views divided by the number of TrueView views.
        average_cpc (float):
            The average cost-per-click (CPC) is defined
            by the total cost of all clicks divided by the
            total number of clicks received.
        average_cpi (float):
            The average cost-per-interaction (CPI) is
            defined by the total cost of all interactions
            divided by the total number of interactions.
        average_cpe (float):
            The average cost-per-engagement (CPE) is
            defined by the total cost of all ad engagements
            divided by the total number of ad engagements.
        interaction_rate (float):
            How often people interact with your ad after
            it is shown to them. This is the number of
            interactions divided by the number of times your
            ad is shown.
        engagement_rate (float):
            How often people engage with your ad after
            it's shown to them. This is the number of ad
            expansions divided by the number of times your
            ad is shown.
        active_view_viewability (float):
            The percentage of time when your ad appeared
            on an Active View enabled site (measurable
            impressions) and was viewable (viewable
            impressions).
        trueview_view_rate (float):
            Number of completed TrueView views divided by
            the number of impressions.
        click_through_rate (float):
            The number of clicks your ad receives
            (Clicks) divided by the number of times your ad
            is shown (Impressions).
        video_completion_p25_rate (float):
            Percentage of impressions where the viewer
            watched 25% of your video.
        video_completion_p50_rate (float):
            Percentage of impressions where the viewer
            watched 50% of your video.
        video_completion_p75_rate (float):
            Percentage of impressions where the viewer
            watched 75% of your video.
        video_completion_p100_rate (float):
            Percentage of impressions where the viewer
            watched all of your video.
    """

    average_cpm: float = proto.Field(
        proto.DOUBLE,
        number=1,
    )
    average_active_view_cpm: float = proto.Field(
        proto.DOUBLE,
        number=2,
    )
    trueview_average_cpv: float = proto.Field(
        proto.DOUBLE,
        number=3,
    )
    average_cpc: float = proto.Field(
        proto.DOUBLE,
        number=4,
    )
    average_cpi: float = proto.Field(
        proto.DOUBLE,
        number=5,
    )
    average_cpe: float = proto.Field(
        proto.DOUBLE,
        number=6,
    )
    interaction_rate: float = proto.Field(
        proto.DOUBLE,
        number=7,
    )
    engagement_rate: float = proto.Field(
        proto.DOUBLE,
        number=8,
    )
    active_view_viewability: float = proto.Field(
        proto.DOUBLE,
        number=9,
    )
    trueview_view_rate: float = proto.Field(
        proto.DOUBLE,
        number=10,
    )
    click_through_rate: float = proto.Field(
        proto.DOUBLE,
        number=11,
    )
    video_completion_p25_rate: float = proto.Field(
        proto.DOUBLE,
        number=12,
    )
    video_completion_p50_rate: float = proto.Field(
        proto.DOUBLE,
        number=13,
    )
    video_completion_p75_rate: float = proto.Field(
        proto.DOUBLE,
        number=14,
    )
    video_completion_p100_rate: float = proto.Field(
        proto.DOUBLE,
        number=15,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
