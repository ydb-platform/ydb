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
from google.ads.googleads.v23.common.types import audience_insights_attribute
from google.ads.googleads.v23.common.types import criteria
from google.ads.googleads.v23.common.types import dates
from google.ads.googleads.v23.enums.types import audience_insights_dimension
from google.ads.googleads.v23.enums.types import (
    audience_insights_marketing_objective,
)


__protobuf__ = proto.module(
    package="google.ads.googleads.v23.services",
    marshal="google.ads.googleads.v23",
    manifest={
        "GenerateInsightsFinderReportRequest",
        "GenerateInsightsFinderReportResponse",
        "GenerateAudienceCompositionInsightsRequest",
        "GenerateAudienceCompositionInsightsResponse",
        "GenerateSuggestedTargetingInsightsRequest",
        "GenerateSuggestedTargetingInsightsResponse",
        "TargetingSuggestionMetrics",
        "ListAudienceInsightsAttributesRequest",
        "ListAudienceInsightsAttributesResponse",
        "ListInsightsEligibleDatesRequest",
        "ListInsightsEligibleDatesResponse",
        "GenerateAudienceOverlapInsightsRequest",
        "GenerateAudienceOverlapInsightsResponse",
        "DimensionOverlapResult",
        "AudienceOverlapItem",
        "GenerateTargetingSuggestionMetricsRequest",
        "GenerateTargetingSuggestionMetricsResponse",
        "GenerateAudienceDefinitionRequest",
        "GenerateAudienceDefinitionResponse",
        "AudienceInsightsDimensions",
        "InsightsAudienceDefinition",
        "InsightsAudienceDescription",
        "InsightsAudience",
        "InsightsAudienceAttributeGroup",
        "AudienceCompositionSection",
        "AudienceCompositionAttributeCluster",
        "AudienceCompositionMetrics",
        "AudienceCompositionAttribute",
    },
)


class GenerateInsightsFinderReportRequest(proto.Message):
    r"""Request message for
    [AudienceInsightsService.GenerateInsightsFinderReport][google.ads.googleads.v23.services.AudienceInsightsService.GenerateInsightsFinderReport].

    Attributes:
        customer_id (str):
            Required. The ID of the customer.
        baseline_audience (google.ads.googleads.v23.services.types.InsightsAudience):
            Required. A baseline audience for this
            report, typically all people in a region.
        specific_audience (google.ads.googleads.v23.services.types.InsightsAudience):
            Required. The specific audience of interest
            for this report.  The insights in the report
            will be based on attributes more prevalent in
            this audience than in the report's baseline
            audience.
        customer_insights_group (str):
            The name of the customer being planned for.
            This is a user-defined value.
        insights_application_info (google.ads.googleads.v23.common.types.AdditionalApplicationInfo):
            Optional. Additional information on the
            application issuing the request.
    """

    customer_id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    baseline_audience: "InsightsAudience" = proto.Field(
        proto.MESSAGE,
        number=6,
        message="InsightsAudience",
    )
    specific_audience: "InsightsAudience" = proto.Field(
        proto.MESSAGE,
        number=7,
        message="InsightsAudience",
    )
    customer_insights_group: str = proto.Field(
        proto.STRING,
        number=4,
    )
    insights_application_info: (
        additional_application_info.AdditionalApplicationInfo
    ) = proto.Field(
        proto.MESSAGE,
        number=5,
        message=additional_application_info.AdditionalApplicationInfo,
    )


class GenerateInsightsFinderReportResponse(proto.Message):
    r"""The response message for
    [AudienceInsightsService.GenerateInsightsFinderReport][google.ads.googleads.v23.services.AudienceInsightsService.GenerateInsightsFinderReport],
    containing the shareable URL for the report.

    Attributes:
        saved_report_url (str):
            An HTTPS URL providing a deep link into the
            Insights Finder UI with the report inputs filled
            in according to the request.
    """

    saved_report_url: str = proto.Field(
        proto.STRING,
        number=1,
    )


class GenerateAudienceCompositionInsightsRequest(proto.Message):
    r"""Request message for
    [AudienceInsightsService.GenerateAudienceCompositionInsights][google.ads.googleads.v23.services.AudienceInsightsService.GenerateAudienceCompositionInsights].

    Attributes:
        customer_id (str):
            Required. The ID of the customer.
        audience (google.ads.googleads.v23.services.types.InsightsAudience):
            Required. The audience of interest for which
            insights are being requested.
        baseline_audience (google.ads.googleads.v23.services.types.InsightsAudience):
            The baseline audience to which the audience
            of interest is being compared.
        data_month (str):
            The one-month range of historical data to use
            for insights, in the format "yyyy-mm". If unset,
            insights will be returned for the last thirty
            days of data.
        dimensions (MutableSequence[google.ads.googleads.v23.enums.types.AudienceInsightsDimensionEnum.AudienceInsightsDimension]):
            Required. The audience dimensions for which composition
            insights should be returned. Supported dimensions are
            KNOWLEDGE_GRAPH, GEO_TARGET_COUNTRY, SUB_COUNTRY_LOCATION,
            YOUTUBE_CHANNEL, YOUTUBE_LINEUP, AFFINITY_USER_INTEREST,
            IN_MARKET_USER_INTEREST, LIFE_EVENT_USER_INTEREST,
            PARENTAL_STATUS, INCOME_RANGE, AGE_RANGE, and GENDER.
        customer_insights_group (str):
            The name of the customer being planned for.
            This is a user-defined value.
        insights_application_info (google.ads.googleads.v23.common.types.AdditionalApplicationInfo):
            Optional. Additional information on the
            application issuing the request.
    """

    customer_id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    audience: "InsightsAudience" = proto.Field(
        proto.MESSAGE,
        number=2,
        message="InsightsAudience",
    )
    baseline_audience: "InsightsAudience" = proto.Field(
        proto.MESSAGE,
        number=6,
        message="InsightsAudience",
    )
    data_month: str = proto.Field(
        proto.STRING,
        number=3,
    )
    dimensions: MutableSequence[
        audience_insights_dimension.AudienceInsightsDimensionEnum.AudienceInsightsDimension
    ] = proto.RepeatedField(
        proto.ENUM,
        number=4,
        enum=audience_insights_dimension.AudienceInsightsDimensionEnum.AudienceInsightsDimension,
    )
    customer_insights_group: str = proto.Field(
        proto.STRING,
        number=5,
    )
    insights_application_info: (
        additional_application_info.AdditionalApplicationInfo
    ) = proto.Field(
        proto.MESSAGE,
        number=7,
        message=additional_application_info.AdditionalApplicationInfo,
    )


class GenerateAudienceCompositionInsightsResponse(proto.Message):
    r"""Response message for
    [AudienceInsightsService.GenerateAudienceCompositionInsights][google.ads.googleads.v23.services.AudienceInsightsService.GenerateAudienceCompositionInsights].

    Attributes:
        sections (MutableSequence[google.ads.googleads.v23.services.types.AudienceCompositionSection]):
            The contents of the insights report,
            organized into sections. Each section is
            associated with one of the
            AudienceInsightsDimension values in the request.
            There may be more than one section per
            dimension.
    """

    sections: MutableSequence["AudienceCompositionSection"] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=1,
            message="AudienceCompositionSection",
        )
    )


class GenerateSuggestedTargetingInsightsRequest(proto.Message):
    r"""Request message for
    [AudienceInsightsService.GenerateSuggestedTargetingInsights][google.ads.googleads.v23.services.AudienceInsightsService.GenerateSuggestedTargetingInsights].

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        customer_id (str):
            Required. The ID of the customer.
        customer_insights_group (str):
            Optional. The name of the customer being
            planned for.  This is a user-defined value.
        insights_application_info (google.ads.googleads.v23.common.types.AdditionalApplicationInfo):
            Optional. Additional information on the
            application issuing the request.
        audience_definition (google.ads.googleads.v23.services.types.InsightsAudienceDefinition):
            Provide a seed audience to get suggestions
            for.

            This field is a member of `oneof`_ ``audience_input``.
        audience_description (google.ads.googleads.v23.services.types.InsightsAudienceDescription):
            Provide a text description of an audience to get
            AI-generated targeting suggestions. This can take around 5
            or more seconds to complete. Supported marketing objectives
            are: AWARENESS and CONSIDERATION. Supported dimensions are:
            AGE_RANGE, GENDER, PARENTAL_STATUS, AFFINITY_USER_INTEREST,
            IN_MARKET_USER_INTEREST and LIFE_EVENT_USER_INTEREST.

            This field is a member of `oneof`_ ``audience_input``.
    """

    customer_id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    customer_insights_group: str = proto.Field(
        proto.STRING,
        number=5,
    )
    insights_application_info: (
        additional_application_info.AdditionalApplicationInfo
    ) = proto.Field(
        proto.MESSAGE,
        number=8,
        message=additional_application_info.AdditionalApplicationInfo,
    )
    audience_definition: "InsightsAudienceDefinition" = proto.Field(
        proto.MESSAGE,
        number=6,
        oneof="audience_input",
        message="InsightsAudienceDefinition",
    )
    audience_description: "InsightsAudienceDescription" = proto.Field(
        proto.MESSAGE,
        number=7,
        oneof="audience_input",
        message="InsightsAudienceDescription",
    )


class GenerateSuggestedTargetingInsightsResponse(proto.Message):
    r"""Response message for
    [AudienceInsightsService.GenerateSuggestedTargetingInsights][google.ads.googleads.v23.services.AudienceInsightsService.GenerateSuggestedTargetingInsights].

    Attributes:
        suggestions (MutableSequence[google.ads.googleads.v23.services.types.TargetingSuggestionMetrics]):
            Suggested insights for targetable audiences.
    """

    suggestions: MutableSequence["TargetingSuggestionMetrics"] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=1,
            message="TargetingSuggestionMetrics",
        )
    )


class TargetingSuggestionMetrics(proto.Message):
    r"""A suggested targetable audience relevant to the requested
    audience.

    Attributes:
        locations (MutableSequence[google.ads.googleads.v23.common.types.AudienceInsightsAttributeMetadata]):
            Suggested location targeting. These attributes all have
            dimension GEO_TARGET_COUNTRY or SUB_COUNTRY_LOCATION
        age_ranges (MutableSequence[google.ads.googleads.v23.common.types.AgeRangeInfo]):
            Suggested age targeting; may be empty
            indicating no age targeting.
        gender (google.ads.googleads.v23.common.types.GenderInfo):
            Suggested gender targeting.  If present, this
            attribute has dimension GENDER.
        parental_status (google.ads.googleads.v23.common.types.ParentalStatusInfo):
            A Parental Status value (parent, or not a
            parent).
        user_interests (MutableSequence[google.ads.googleads.v23.common.types.AudienceInsightsAttributeMetadataGroup]):
            List of user interest attributes with
            metadata defining the audience. The combination
            has a logical AND-of-ORs structure: The
            attributes within each
            AudienceInsightsAttributeMetadataGroup are ORed,
            and the groups themselves are ANDed.
        coverage (float):
            The fraction (from 0 to 1 inclusive) of the
            requested audience that can be reached using the
            suggested targeting.
        index (float):
            The ratio of coverage to the coverage of the
            baseline audience or zero if this ratio is
            undefined or is not meaningful.
        potential_youtube_reach (int):
            The approximate estimated number of people
            that can be reached on YouTube using this
            targeting.
    """

    locations: MutableSequence[
        audience_insights_attribute.AudienceInsightsAttributeMetadata
    ] = proto.RepeatedField(
        proto.MESSAGE,
        number=9,
        message=audience_insights_attribute.AudienceInsightsAttributeMetadata,
    )
    age_ranges: MutableSequence[criteria.AgeRangeInfo] = proto.RepeatedField(
        proto.MESSAGE,
        number=2,
        message=criteria.AgeRangeInfo,
    )
    gender: criteria.GenderInfo = proto.Field(
        proto.MESSAGE,
        number=3,
        message=criteria.GenderInfo,
    )
    parental_status: criteria.ParentalStatusInfo = proto.Field(
        proto.MESSAGE,
        number=8,
        message=criteria.ParentalStatusInfo,
    )
    user_interests: MutableSequence[
        audience_insights_attribute.AudienceInsightsAttributeMetadataGroup
    ] = proto.RepeatedField(
        proto.MESSAGE,
        number=11,
        message=audience_insights_attribute.AudienceInsightsAttributeMetadataGroup,
    )
    coverage: float = proto.Field(
        proto.DOUBLE,
        number=5,
    )
    index: float = proto.Field(
        proto.DOUBLE,
        number=6,
    )
    potential_youtube_reach: int = proto.Field(
        proto.INT64,
        number=7,
    )


class ListAudienceInsightsAttributesRequest(proto.Message):
    r"""Request message for
    [AudienceInsightsService.ListAudienceInsightsAttributes][google.ads.googleads.v23.services.AudienceInsightsService.ListAudienceInsightsAttributes].

    Attributes:
        customer_id (str):
            Required. The ID of the customer.
        dimensions (MutableSequence[google.ads.googleads.v23.enums.types.AudienceInsightsDimensionEnum.AudienceInsightsDimension]):
            Required. The types of attributes to be returned. Supported
            dimensions are CATEGORY, KNOWLEDGE_GRAPH, DEVICE,
            GEO_TARGET_COUNTRY, SUB_COUNTRY_LOCATION, YOUTUBE_LINEUP,
            AFFINITY_USER_INTEREST, IN_MARKET_USER_INTEREST,
            LIFE_EVENT_USER_INTEREST, PARENTAL_STATUS, INCOME_RANGE,
            AGE_RANGE, and GENDER.
        query_text (str):
            Required. A free text query. If the requested dimensions
            include Attributes CATEGORY or KNOWLEDGE_GRAPH, then the
            attributes returned for those dimensions will match or be
            related to this string. For other dimensions, this field is
            ignored and all available attributes are returned.
        customer_insights_group (str):
            The name of the customer being planned for.
            This is a user-defined value.
        insights_application_info (google.ads.googleads.v23.common.types.AdditionalApplicationInfo):
            Optional. Additional information on the
            application issuing the request.
        location_country_filters (MutableSequence[google.ads.googleads.v23.common.types.LocationInfo]):
            If SUB_COUNTRY_LOCATION attributes are one of the requested
            dimensions and this field is present, then the
            SUB_COUNTRY_LOCATION attributes returned will be located in
            these countries. If this field is absent, then location
            attributes are not filtered by country. Setting this field
            when SUB_COUNTRY_LOCATION attributes are not requested will
            return an error.
        youtube_reach_location (google.ads.googleads.v23.common.types.LocationInfo):
            If present, potential YouTube reach estimates within the
            specified market will be returned for attributes for which
            they are available. Reach is only available for the
            AGE_RANGE, GENDER, AFFINITY_USER_INTEREST and
            IN_MARKET_USER_INTEREST dimensions, and may not be available
            for every attribute of those dimensions in every market.
    """

    customer_id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    dimensions: MutableSequence[
        audience_insights_dimension.AudienceInsightsDimensionEnum.AudienceInsightsDimension
    ] = proto.RepeatedField(
        proto.ENUM,
        number=2,
        enum=audience_insights_dimension.AudienceInsightsDimensionEnum.AudienceInsightsDimension,
    )
    query_text: str = proto.Field(
        proto.STRING,
        number=3,
    )
    customer_insights_group: str = proto.Field(
        proto.STRING,
        number=4,
    )
    insights_application_info: (
        additional_application_info.AdditionalApplicationInfo
    ) = proto.Field(
        proto.MESSAGE,
        number=7,
        message=additional_application_info.AdditionalApplicationInfo,
    )
    location_country_filters: MutableSequence[criteria.LocationInfo] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=5,
            message=criteria.LocationInfo,
        )
    )
    youtube_reach_location: criteria.LocationInfo = proto.Field(
        proto.MESSAGE,
        number=6,
        message=criteria.LocationInfo,
    )


class ListAudienceInsightsAttributesResponse(proto.Message):
    r"""Response message for
    [AudienceInsightsService.ListAudienceInsightsAttributes][google.ads.googleads.v23.services.AudienceInsightsService.ListAudienceInsightsAttributes].

    Attributes:
        attributes (MutableSequence[google.ads.googleads.v23.common.types.AudienceInsightsAttributeMetadata]):
            The attributes matching the search query.
    """

    attributes: MutableSequence[
        audience_insights_attribute.AudienceInsightsAttributeMetadata
    ] = proto.RepeatedField(
        proto.MESSAGE,
        number=2,
        message=audience_insights_attribute.AudienceInsightsAttributeMetadata,
    )


class ListInsightsEligibleDatesRequest(proto.Message):
    r"""Request message for
    [AudienceInsightsService.ListInsightsEligibleDates][google.ads.googleads.v23.services.AudienceInsightsService.ListInsightsEligibleDates].

    Attributes:
        insights_application_info (google.ads.googleads.v23.common.types.AdditionalApplicationInfo):
            Optional. Additional information on the
            application issuing the request.
    """

    insights_application_info: (
        additional_application_info.AdditionalApplicationInfo
    ) = proto.Field(
        proto.MESSAGE,
        number=1,
        message=additional_application_info.AdditionalApplicationInfo,
    )


class ListInsightsEligibleDatesResponse(proto.Message):
    r"""Response message for
    [AudienceInsightsService.ListInsightsEligibleDates][google.ads.googleads.v23.services.AudienceInsightsService.ListInsightsEligibleDates].

    Attributes:
        data_months (MutableSequence[str]):
            The months for which AudienceInsights data is
            currently available, each represented as a
            string in the form "YYYY-MM".
        last_thirty_days (google.ads.googleads.v23.common.types.DateRange):
            The actual dates covered by the "last 30 days" date range
            that will be used implicitly for
            [AudienceInsightsService.GenerateAudienceCompositionInsights][google.ads.googleads.v23.services.AudienceInsightsService.GenerateAudienceCompositionInsights]
            requests that have no data_month set.
    """

    data_months: MutableSequence[str] = proto.RepeatedField(
        proto.STRING,
        number=1,
    )
    last_thirty_days: dates.DateRange = proto.Field(
        proto.MESSAGE,
        number=2,
        message=dates.DateRange,
    )


class GenerateAudienceOverlapInsightsRequest(proto.Message):
    r"""Request message for
    [AudienceInsightsService.GenerateAudienceOverlapInsights][google.ads.googleads.v23.services.AudienceInsightsService.GenerateAudienceOverlapInsights].

    Attributes:
        customer_id (str):
            Required. The ID of the customer.
        country_location (google.ads.googleads.v23.common.types.LocationInfo):
            Required. The country in which to calculate
            the sizes and overlaps of audiences.
        primary_attribute (google.ads.googleads.v23.common.types.AudienceInsightsAttribute):
            Required. The audience attribute that should
            be intersected with all other eligible
            audiences.  This must be an Affinity or
            In-Market UserInterest, an AgeRange or a Gender.
        dimensions (MutableSequence[google.ads.googleads.v23.enums.types.AudienceInsightsDimensionEnum.AudienceInsightsDimension]):
            Required. The types of attributes of which to calculate the
            overlap with the primary_attribute. The values must be a
            subset of AFFINITY_USER_INTEREST, IN_MARKET_USER_INTEREST,
            AGE_RANGE and GENDER.
        customer_insights_group (str):
            The name of the customer being planned for.
            This is a user-defined value.
        insights_application_info (google.ads.googleads.v23.common.types.AdditionalApplicationInfo):
            Optional. Additional information on the
            application issuing the request.
    """

    customer_id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    country_location: criteria.LocationInfo = proto.Field(
        proto.MESSAGE,
        number=2,
        message=criteria.LocationInfo,
    )
    primary_attribute: audience_insights_attribute.AudienceInsightsAttribute = (
        proto.Field(
            proto.MESSAGE,
            number=6,
            message=audience_insights_attribute.AudienceInsightsAttribute,
        )
    )
    dimensions: MutableSequence[
        audience_insights_dimension.AudienceInsightsDimensionEnum.AudienceInsightsDimension
    ] = proto.RepeatedField(
        proto.ENUM,
        number=4,
        enum=audience_insights_dimension.AudienceInsightsDimensionEnum.AudienceInsightsDimension,
    )
    customer_insights_group: str = proto.Field(
        proto.STRING,
        number=5,
    )
    insights_application_info: (
        additional_application_info.AdditionalApplicationInfo
    ) = proto.Field(
        proto.MESSAGE,
        number=7,
        message=additional_application_info.AdditionalApplicationInfo,
    )


class GenerateAudienceOverlapInsightsResponse(proto.Message):
    r"""Response message for
    [AudienceInsightsService.GenerateAudienceOverlapInsights][google.ads.googleads.v23.services.AudienceInsightsService.GenerateAudienceOverlapInsights].

    Attributes:
        primary_attribute_metadata (google.ads.googleads.v23.common.types.AudienceInsightsAttributeMetadata):
            Metadata for the primary attribute, including
            potential YouTube reach.
        dimension_results (MutableSequence[google.ads.googleads.v23.services.types.DimensionOverlapResult]):
            Lists of attributes and their overlap with
            the primary attribute, one list per requested
            dimension.
    """

    primary_attribute_metadata: (
        audience_insights_attribute.AudienceInsightsAttributeMetadata
    ) = proto.Field(
        proto.MESSAGE,
        number=3,
        message=audience_insights_attribute.AudienceInsightsAttributeMetadata,
    )
    dimension_results: MutableSequence["DimensionOverlapResult"] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=2,
            message="DimensionOverlapResult",
        )
    )


class DimensionOverlapResult(proto.Message):
    r"""A list of audience attributes of a single dimension, including their
    overlap with a primary attribute, returned as part of a
    [GenerateAudienceOverlapInsightsResponse][google.ads.googleads.v23.services.GenerateAudienceOverlapInsightsResponse].

    Attributes:
        dimension (google.ads.googleads.v23.enums.types.AudienceInsightsDimensionEnum.AudienceInsightsDimension):
            The dimension of all the attributes in this
            section.
        items (MutableSequence[google.ads.googleads.v23.services.types.AudienceOverlapItem]):
            The attributes and their overlap with the
            primary attribute.
    """

    dimension: (
        audience_insights_dimension.AudienceInsightsDimensionEnum.AudienceInsightsDimension
    ) = proto.Field(
        proto.ENUM,
        number=1,
        enum=audience_insights_dimension.AudienceInsightsDimensionEnum.AudienceInsightsDimension,
    )
    items: MutableSequence["AudienceOverlapItem"] = proto.RepeatedField(
        proto.MESSAGE,
        number=2,
        message="AudienceOverlapItem",
    )


class AudienceOverlapItem(proto.Message):
    r"""An audience attribute, with metadata including the overlap
    between this attribute's potential YouTube reach and that of a
    primary attribute.

    Attributes:
        attribute_metadata (google.ads.googleads.v23.common.types.AudienceInsightsAttributeMetadata):
            The attribute and its metadata, including
            potential YouTube reach.
        potential_youtube_reach_intersection (int):
            The estimated size of the intersection of
            this audience attribute with the primary
            attribute, that is, the number of reachable
            YouTube users who match BOTH the primary
            attribute and this one.
    """

    attribute_metadata: (
        audience_insights_attribute.AudienceInsightsAttributeMetadata
    ) = proto.Field(
        proto.MESSAGE,
        number=3,
        message=audience_insights_attribute.AudienceInsightsAttributeMetadata,
    )
    potential_youtube_reach_intersection: int = proto.Field(
        proto.INT64,
        number=2,
    )


class GenerateTargetingSuggestionMetricsRequest(proto.Message):
    r"""Request message for
    [AudienceInsightsService.GenerateTargetingSuggestionMetrics][google.ads.googleads.v23.services.AudienceInsightsService.GenerateTargetingSuggestionMetrics].

    Attributes:
        customer_id (str):
            Required. The ID of the customer.
        audiences (MutableSequence[google.ads.googleads.v23.services.types.InsightsAudience]):
            Required. Audiences to request metrics for.
        customer_insights_group (str):
            Optional. The name of the customer being
            planned for.  This is a user-defined value.
        insights_application_info (google.ads.googleads.v23.common.types.AdditionalApplicationInfo):
            Optional. Additional information on the
            application issuing the request.
    """

    customer_id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    audiences: MutableSequence["InsightsAudience"] = proto.RepeatedField(
        proto.MESSAGE,
        number=5,
        message="InsightsAudience",
    )
    customer_insights_group: str = proto.Field(
        proto.STRING,
        number=3,
    )
    insights_application_info: (
        additional_application_info.AdditionalApplicationInfo
    ) = proto.Field(
        proto.MESSAGE,
        number=4,
        message=additional_application_info.AdditionalApplicationInfo,
    )


class GenerateTargetingSuggestionMetricsResponse(proto.Message):
    r"""Response message for
    [AudienceInsightsService.GenerateTargetingSuggestionMetrics][google.ads.googleads.v23.services.AudienceInsightsService.GenerateTargetingSuggestionMetrics].

    Attributes:
        suggestions (MutableSequence[google.ads.googleads.v23.services.types.TargetingSuggestionMetrics]):
            Suggested targetable audiences. There will be one suggestion
            for each
            [GenerateTargetingSuggestionMetricsRequest.audiences][google.ads.googleads.v23.services.GenerateTargetingSuggestionMetricsRequest.audiences]
            requested, matching the order requested.
    """

    suggestions: MutableSequence["TargetingSuggestionMetrics"] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=1,
            message="TargetingSuggestionMetrics",
        )
    )


class GenerateAudienceDefinitionRequest(proto.Message):
    r"""Request message for
    [AudienceInsightsService.GenerateAudienceDefinition][google.ads.googleads.v23.services.AudienceInsightsService.GenerateAudienceDefinition].

    Attributes:
        customer_id (str):
            Required. The ID of the customer.
        audience_description (google.ads.googleads.v23.services.types.InsightsAudienceDescription):
            Required. Provide a text description of an audience to get
            AI-generated structured suggestions. This can take around 5
            or more seconds to complete Supported marketing objectives
            are: AWARENESS, CONSIDERATION and RESEARCH. Supported
            dimensions are: AGE_RANGE, GENDER, PARENTAL_STATUS,
            AFFINITY_USER_INTEREST, IN_MARKET_USER_INTEREST,
            LIFE_EVENT_USER_INTEREST, CATEGORY and KNOWLEDGE_GRAPH.
        customer_insights_group (str):
            Optional. The name of the customer being
            planned for.  This is a user-defined value.
        insights_application_info (google.ads.googleads.v23.common.types.AdditionalApplicationInfo):
            Optional. Additional information on the
            application issuing the request.
    """

    customer_id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    audience_description: "InsightsAudienceDescription" = proto.Field(
        proto.MESSAGE,
        number=2,
        message="InsightsAudienceDescription",
    )
    customer_insights_group: str = proto.Field(
        proto.STRING,
        number=3,
    )
    insights_application_info: (
        additional_application_info.AdditionalApplicationInfo
    ) = proto.Field(
        proto.MESSAGE,
        number=4,
        message=additional_application_info.AdditionalApplicationInfo,
    )


class GenerateAudienceDefinitionResponse(proto.Message):
    r"""Response message for
    [AudienceInsightsService.GenerateAudienceDefinition][google.ads.googleads.v23.services.AudienceInsightsService.GenerateAudienceDefinition].

    Attributes:
        high_relevance_attributes (MutableSequence[google.ads.googleads.v23.common.types.AudienceInsightsAttributeMetadata]):
            The attributes that make up the audience
            definition.
        medium_relevance_attributes (MutableSequence[google.ads.googleads.v23.common.types.AudienceInsightsAttributeMetadata]):
            Additional attributes that are less relevant
            but still related to the audience description.
            Use these attributes to broaden the audience
            definition to reach more users.
    """

    high_relevance_attributes: MutableSequence[
        audience_insights_attribute.AudienceInsightsAttributeMetadata
    ] = proto.RepeatedField(
        proto.MESSAGE,
        number=1,
        message=audience_insights_attribute.AudienceInsightsAttributeMetadata,
    )
    medium_relevance_attributes: MutableSequence[
        audience_insights_attribute.AudienceInsightsAttributeMetadata
    ] = proto.RepeatedField(
        proto.MESSAGE,
        number=2,
        message=audience_insights_attribute.AudienceInsightsAttributeMetadata,
    )


class AudienceInsightsDimensions(proto.Message):
    r"""A collection of dimensions to be used for generating
    insights.

    Attributes:
        dimensions (MutableSequence[google.ads.googleads.v23.enums.types.AudienceInsightsDimensionEnum.AudienceInsightsDimension]):
            Required. A list of dimensions.
    """

    dimensions: MutableSequence[
        audience_insights_dimension.AudienceInsightsDimensionEnum.AudienceInsightsDimension
    ] = proto.RepeatedField(
        proto.ENUM,
        number=1,
        enum=audience_insights_dimension.AudienceInsightsDimensionEnum.AudienceInsightsDimension,
    )


class InsightsAudienceDefinition(proto.Message):
    r"""A structured definition of the audience of interest for which
    insights are being requested in AudienceInsightsService.

    Attributes:
        audience (google.ads.googleads.v23.services.types.InsightsAudience):
            Required. The audience of interest for which
            insights are being requested.
        baseline_audience (google.ads.googleads.v23.services.types.InsightsAudience):
            Optional. The baseline audience. The default,
            if unspecified, is all people in the same
            country as the audience of interest.
        data_month (str):
            Optional. The one-month range of historical
            data to use for insights, in the format
            "yyyy-mm". If unset, insights will be returned
            for the last thirty days of data.
    """

    audience: "InsightsAudience" = proto.Field(
        proto.MESSAGE,
        number=1,
        message="InsightsAudience",
    )
    baseline_audience: "InsightsAudience" = proto.Field(
        proto.MESSAGE,
        number=2,
        message="InsightsAudience",
    )
    data_month: str = proto.Field(
        proto.STRING,
        number=3,
    )


class InsightsAudienceDescription(proto.Message):
    r"""A text description of the audience of interest for which
    insights are being requested in AudienceInsightsService.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        country_locations (MutableSequence[google.ads.googleads.v23.common.types.LocationInfo]):
            Required. The countries for the audience.
        audience_description (str):
            Required. An English language text
            description of an audience to get suggestions
            for. Maximum length is 2000 characters. For
            example, "Women in their 30s who love to
            travel".
        marketing_objective (google.ads.googleads.v23.enums.types.AudienceInsightsMarketingObjectiveEnum.AudienceInsightsMarketingObjective):
            Optional. An optional marketing objective
            which will influence the type of suggestions
            produced.

            This field is a member of `oneof`_ ``output_types``.
        audience_dimensions (google.ads.googleads.v23.services.types.AudienceInsightsDimensions):
            Optional. An optional list of audience
            dimensions to return.

            This field is a member of `oneof`_ ``output_types``.
    """

    country_locations: MutableSequence[criteria.LocationInfo] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=1,
            message=criteria.LocationInfo,
        )
    )
    audience_description: str = proto.Field(
        proto.STRING,
        number=2,
    )
    marketing_objective: (
        audience_insights_marketing_objective.AudienceInsightsMarketingObjectiveEnum.AudienceInsightsMarketingObjective
    ) = proto.Field(
        proto.ENUM,
        number=4,
        oneof="output_types",
        enum=audience_insights_marketing_objective.AudienceInsightsMarketingObjectiveEnum.AudienceInsightsMarketingObjective,
    )
    audience_dimensions: "AudienceInsightsDimensions" = proto.Field(
        proto.MESSAGE,
        number=5,
        oneof="output_types",
        message="AudienceInsightsDimensions",
    )


class InsightsAudience(proto.Message):
    r"""A set of users, defined by various characteristics, for which
    insights can be requested in AudienceInsightsService.

    Attributes:
        country_locations (MutableSequence[google.ads.googleads.v23.common.types.LocationInfo]):
            Required. The countries for the audience.
        sub_country_locations (MutableSequence[google.ads.googleads.v23.common.types.LocationInfo]):
            Sub-country geographic location attributes. If present, each
            of these must be contained in one of the countries in this
            audience. If absent, the audience is geographically to the
            country_locations and no further.
        gender (google.ads.googleads.v23.common.types.GenderInfo):
            Gender for the audience.  If absent, the
            audience does not restrict by gender.
        age_ranges (MutableSequence[google.ads.googleads.v23.common.types.AgeRangeInfo]):
            Age ranges for the audience.  If absent, the
            audience represents all people over 18 that
            match the other attributes.
        parental_status (google.ads.googleads.v23.common.types.ParentalStatusInfo):
            Parental status for the audience.  If absent,
            the audience does not restrict by parental
            status.
        income_ranges (MutableSequence[google.ads.googleads.v23.common.types.IncomeRangeInfo]):
            Household income percentile ranges for the
            audience.  If absent, the audience does not
            restrict by household income range.
        lineups (MutableSequence[google.ads.googleads.v23.common.types.AudienceInsightsLineup]):
            Lineups representing the YouTube content
            viewed by the audience.
        user_list (google.ads.googleads.v23.common.types.UserListInfo):
            User list to be targeted by the audience.
        topic_audience_combinations (MutableSequence[google.ads.googleads.v23.services.types.InsightsAudienceAttributeGroup]):
            A combination of entity, category and user
            interest attributes defining the audience. The
            combination has a logical AND-of-ORs structure:
            Attributes within each
            InsightsAudienceAttributeGroup are combined with
            OR, and the combinations themselves are combined
            together with AND.  For example, the expression
            (Entity OR Affinity) AND (In-Market OR Category)
            can be formed using two
            InsightsAudienceAttributeGroups with two
            Attributes each.
    """

    country_locations: MutableSequence[criteria.LocationInfo] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=1,
            message=criteria.LocationInfo,
        )
    )
    sub_country_locations: MutableSequence[criteria.LocationInfo] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=2,
            message=criteria.LocationInfo,
        )
    )
    gender: criteria.GenderInfo = proto.Field(
        proto.MESSAGE,
        number=3,
        message=criteria.GenderInfo,
    )
    age_ranges: MutableSequence[criteria.AgeRangeInfo] = proto.RepeatedField(
        proto.MESSAGE,
        number=4,
        message=criteria.AgeRangeInfo,
    )
    parental_status: criteria.ParentalStatusInfo = proto.Field(
        proto.MESSAGE,
        number=5,
        message=criteria.ParentalStatusInfo,
    )
    income_ranges: MutableSequence[criteria.IncomeRangeInfo] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=6,
            message=criteria.IncomeRangeInfo,
        )
    )
    lineups: MutableSequence[
        audience_insights_attribute.AudienceInsightsLineup
    ] = proto.RepeatedField(
        proto.MESSAGE,
        number=10,
        message=audience_insights_attribute.AudienceInsightsLineup,
    )
    user_list: criteria.UserListInfo = proto.Field(
        proto.MESSAGE,
        number=11,
        message=criteria.UserListInfo,
    )
    topic_audience_combinations: MutableSequence[
        "InsightsAudienceAttributeGroup"
    ] = proto.RepeatedField(
        proto.MESSAGE,
        number=8,
        message="InsightsAudienceAttributeGroup",
    )


class InsightsAudienceAttributeGroup(proto.Message):
    r"""A list of AudienceInsightsAttributes.

    Attributes:
        attributes (MutableSequence[google.ads.googleads.v23.common.types.AudienceInsightsAttribute]):
            Required. A collection of audience attributes
            to be combined with logical OR. Attributes need
            not all be the same dimension.  Only Knowledge
            Graph entities, Product & Service Categories,
            and Affinity and In-Market audiences are
            supported in this context.
    """

    attributes: MutableSequence[
        audience_insights_attribute.AudienceInsightsAttribute
    ] = proto.RepeatedField(
        proto.MESSAGE,
        number=2,
        message=audience_insights_attribute.AudienceInsightsAttribute,
    )


class AudienceCompositionSection(proto.Message):
    r"""A collection of related attributes of the same type in an
    audience composition insights report.

    Attributes:
        dimension (google.ads.googleads.v23.enums.types.AudienceInsightsDimensionEnum.AudienceInsightsDimension):
            The type of the attributes in this section.
        top_attributes (MutableSequence[google.ads.googleads.v23.services.types.AudienceCompositionAttribute]):
            The most relevant segments for this audience. If dimension
            is GENDER, AGE_RANGE or PARENTAL_STATUS, then this list of
            attributes is exhaustive.
        clustered_attributes (MutableSequence[google.ads.googleads.v23.services.types.AudienceCompositionAttributeCluster]):
            Additional attributes for this audience, grouped into
            clusters. Only populated if dimension is YOUTUBE_CHANNEL.
    """

    dimension: (
        audience_insights_dimension.AudienceInsightsDimensionEnum.AudienceInsightsDimension
    ) = proto.Field(
        proto.ENUM,
        number=1,
        enum=audience_insights_dimension.AudienceInsightsDimensionEnum.AudienceInsightsDimension,
    )
    top_attributes: MutableSequence["AudienceCompositionAttribute"] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=3,
            message="AudienceCompositionAttribute",
        )
    )
    clustered_attributes: MutableSequence[
        "AudienceCompositionAttributeCluster"
    ] = proto.RepeatedField(
        proto.MESSAGE,
        number=4,
        message="AudienceCompositionAttributeCluster",
    )


class AudienceCompositionAttributeCluster(proto.Message):
    r"""A collection of related attributes, with metadata and
    metrics, in an audience composition insights report.

    Attributes:
        cluster_display_name (str):
            The name of this cluster of attributes
        cluster_metrics (google.ads.googleads.v23.services.types.AudienceCompositionMetrics):
            If the dimension associated with this cluster is
            YOUTUBE_CHANNEL, then cluster_metrics are metrics associated
            with the cluster as a whole. For other dimensions, this
            field is unset.
        attributes (MutableSequence[google.ads.googleads.v23.services.types.AudienceCompositionAttribute]):
            The individual attributes that make up this
            cluster, with metadata and metrics.
    """

    cluster_display_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    cluster_metrics: "AudienceCompositionMetrics" = proto.Field(
        proto.MESSAGE,
        number=3,
        message="AudienceCompositionMetrics",
    )
    attributes: MutableSequence["AudienceCompositionAttribute"] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=4,
            message="AudienceCompositionAttribute",
        )
    )


class AudienceCompositionMetrics(proto.Message):
    r"""The share and index metrics associated with an attribute in
    an audience composition insights report.

    Attributes:
        baseline_audience_share (float):
            The fraction (from 0 to 1 inclusive) of the
            baseline audience that match the attribute.
        audience_share (float):
            The fraction (from 0 to 1 inclusive) of the
            specific audience that match the attribute.
        index (float):
            The ratio of audience_share to baseline_audience_share, or
            zero if this ratio is undefined or is not meaningful.
        score (float):
            A relevance score from 0 to 1 inclusive.
    """

    baseline_audience_share: float = proto.Field(
        proto.DOUBLE,
        number=1,
    )
    audience_share: float = proto.Field(
        proto.DOUBLE,
        number=2,
    )
    index: float = proto.Field(
        proto.DOUBLE,
        number=3,
    )
    score: float = proto.Field(
        proto.DOUBLE,
        number=4,
    )


class AudienceCompositionAttribute(proto.Message):
    r"""An audience attribute with metadata and metrics.

    Attributes:
        attribute_metadata (google.ads.googleads.v23.common.types.AudienceInsightsAttributeMetadata):
            The attribute with its metadata.
        metrics (google.ads.googleads.v23.services.types.AudienceCompositionMetrics):
            Share and index metrics for the attribute.
    """

    attribute_metadata: (
        audience_insights_attribute.AudienceInsightsAttributeMetadata
    ) = proto.Field(
        proto.MESSAGE,
        number=3,
        message=audience_insights_attribute.AudienceInsightsAttributeMetadata,
    )
    metrics: "AudienceCompositionMetrics" = proto.Field(
        proto.MESSAGE,
        number=2,
        message="AudienceCompositionMetrics",
    )


__all__ = tuple(sorted(__protobuf__.manifest))
