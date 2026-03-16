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
from google.ads.googleads.v23.enums.types import insights_trend


__protobuf__ = proto.module(
    package="google.ads.googleads.v23.services",
    marshal="google.ads.googleads.v23",
    manifest={
        "GenerateCreatorInsightsRequest",
        "GenerateCreatorInsightsResponse",
        "GenerateTrendingInsightsRequest",
        "GenerateTrendingInsightsResponse",
        "YouTubeCreatorInsights",
        "YouTubeMetrics",
        "YouTubeChannelInsights",
        "SearchAudience",
        "SearchTopics",
        "TrendInsight",
        "TrendInsightMetrics",
        "TrendInsightDataPoint",
        "LanguageDistribution",
    },
)


class GenerateCreatorInsightsRequest(proto.Message):
    r"""Request message for
    [ContentCreatorInsightsService.GenerateCreatorInsights][google.ads.googleads.v23.services.ContentCreatorInsightsService.GenerateCreatorInsights].

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        customer_id (str):
            Required. The ID of the customer.
        customer_insights_group (str):
            Required. The name of the customer being
            planned for.  This is a user-defined value.
        insights_application_info (google.ads.googleads.v23.common.types.AdditionalApplicationInfo):
            Optional. Additional information on the
            application issuing the request.
        country_locations (MutableSequence[google.ads.googleads.v23.common.types.LocationInfo]):
            Required. The countries to search that apply
            to the criteria.
        sub_country_locations (MutableSequence[google.ads.googleads.v23.common.types.LocationInfo]):
            The sub-country geographic locations to search that apply to
            the criteria. Only supported for
            [SearchAttributes][google.ads.googleads.v23.services.GenerateCreatorInsightsRequest.SearchAttributes]
            criteria.
        search_attributes (google.ads.googleads.v23.services.types.GenerateCreatorInsightsRequest.SearchAttributes):
            The attributes used to identify top creators. Data fetched
            is based on the list of countries or sub-country locations
            specified in
            [country_locations][google.ads.googleads.v23.services.GenerateCreatorInsightsRequest.country_locations]
            or
            [sub_country_locations][google.ads.googleads.v23.services.GenerateCreatorInsightsRequest.sub_country_locations].

            This field is a member of `oneof`_ ``criteria``.
        search_brand (google.ads.googleads.v23.services.types.GenerateCreatorInsightsRequest.SearchBrand):
            A brand used to search for top creators. Data fetched is
            based on the list of countries specified in
            [country_locations][google.ads.googleads.v23.services.GenerateCreatorInsightsRequest.country_locations].

            This field is a member of `oneof`_ ``criteria``.
        search_channels (google.ads.googleads.v23.services.types.GenerateCreatorInsightsRequest.YouTubeChannels):
            YouTube Channel IDs for Creator Insights. Data fetched for
            channels is based on the list of countries specified in
            [country_locations][google.ads.googleads.v23.services.GenerateCreatorInsightsRequest.country_locations].

            This field is a member of `oneof`_ ``criteria``.
    """

    class SearchAttributes(proto.Message):
        r"""The audience attributes (such as Age, Gender, Affinity, and
        In-Market) and creator attributes (such as creator's content
        topics) used to search for top creators.

        Attributes:
            audience_attributes (MutableSequence[google.ads.googleads.v23.common.types.AudienceInsightsAttribute]):
                Optional. Audience attributes that describe an audience of
                viewers. This is used to search for creators whose own
                viewers match the input audience. Attributes age_range,
                gender, user_interest, entity, category, device,
                parental_status, and income_range are supported. Attribute
                location is not supported.
            creator_attributes (MutableSequence[google.ads.googleads.v23.common.types.AudienceInsightsAttribute]):
                Optional. Creator attributes that describe a collection of
                types of content. This is used to search for creators whose
                content matches the input creator attributes. Only Knowledge
                Graph Entities tagged with
                [CREATOR_ATTRIBUTE][google.ads.googleads.v23.enums.InsightsKnowledgeGraphEntityCapabilitiesEnum.InsightsKnowledgeGraphEntityCapabilities.CREATOR_ATTRIBUTE]
                are supported. Use
                [AudienceInsightsService.ListAudienceInsightsAttributes][google.ads.googleads.v23.services.AudienceInsightsService.ListAudienceInsightsAttributes]
                to get the list of supported entities. Other attributes
                including location are not supported.
        """

        audience_attributes: MutableSequence[
            audience_insights_attribute.AudienceInsightsAttribute
        ] = proto.RepeatedField(
            proto.MESSAGE,
            number=1,
            message=audience_insights_attribute.AudienceInsightsAttribute,
        )
        creator_attributes: MutableSequence[
            audience_insights_attribute.AudienceInsightsAttribute
        ] = proto.RepeatedField(
            proto.MESSAGE,
            number=2,
            message=audience_insights_attribute.AudienceInsightsAttribute,
        )

    class SearchBrand(proto.Message):
        r"""The brand used to search for top creators.

        Attributes:
            brand_entities (MutableSequence[google.ads.googleads.v23.common.types.AudienceInsightsAttribute]):
                Optional. One or more Knowledge Graph
                Entities that represent the brand for which to
                find insights.
            include_related_topics (bool):
                Optional. When true, we will expand the search to beyond
                just the entities specified in
                [brand_entities][google.ads.googleads.v23.services.GenerateCreatorInsightsRequest.SearchBrand.brand_entities]
                to other related knowledge graph entities similar to the
                brand. The default value is ``false``.
        """

        brand_entities: MutableSequence[
            audience_insights_attribute.AudienceInsightsAttribute
        ] = proto.RepeatedField(
            proto.MESSAGE,
            number=1,
            message=audience_insights_attribute.AudienceInsightsAttribute,
        )
        include_related_topics: bool = proto.Field(
            proto.BOOL,
            number=2,
        )

    class YouTubeChannels(proto.Message):
        r"""A collection of YouTube Channels.

        Attributes:
            youtube_channels (MutableSequence[google.ads.googleads.v23.common.types.YouTubeChannelInfo]):
                Optional. The YouTube Channel IDs to fetch
                creator insights for.
        """

        youtube_channels: MutableSequence[criteria.YouTubeChannelInfo] = (
            proto.RepeatedField(
                proto.MESSAGE,
                number=1,
                message=criteria.YouTubeChannelInfo,
            )
        )

    customer_id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    customer_insights_group: str = proto.Field(
        proto.STRING,
        number=2,
    )
    insights_application_info: (
        additional_application_info.AdditionalApplicationInfo
    ) = proto.Field(
        proto.MESSAGE,
        number=8,
        message=additional_application_info.AdditionalApplicationInfo,
    )
    country_locations: MutableSequence[criteria.LocationInfo] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=6,
            message=criteria.LocationInfo,
        )
    )
    sub_country_locations: MutableSequence[criteria.LocationInfo] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=7,
            message=criteria.LocationInfo,
        )
    )
    search_attributes: SearchAttributes = proto.Field(
        proto.MESSAGE,
        number=3,
        oneof="criteria",
        message=SearchAttributes,
    )
    search_brand: SearchBrand = proto.Field(
        proto.MESSAGE,
        number=5,
        oneof="criteria",
        message=SearchBrand,
    )
    search_channels: YouTubeChannels = proto.Field(
        proto.MESSAGE,
        number=4,
        oneof="criteria",
        message=YouTubeChannels,
    )


class GenerateCreatorInsightsResponse(proto.Message):
    r"""Response message for
    [ContentCreatorInsightsService.GenerateCreatorInsights][google.ads.googleads.v23.services.ContentCreatorInsightsService.GenerateCreatorInsights].

    Attributes:
        creator_insights (MutableSequence[google.ads.googleads.v23.services.types.YouTubeCreatorInsights]):
            A collection of YouTube Creators, each
            containing a collection of YouTube Channels
            maintained by the YouTube Creator.
    """

    creator_insights: MutableSequence["YouTubeCreatorInsights"] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=1,
            message="YouTubeCreatorInsights",
        )
    )


class GenerateTrendingInsightsRequest(proto.Message):
    r"""Request message for
    [ContentCreatorInsightsService.GenerateTrendingInsights][google.ads.googleads.v23.services.ContentCreatorInsightsService.GenerateTrendingInsights].

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        customer_id (str):
            Required. The ID of the customer.
        customer_insights_group (str):
            Required. The name of the customer being
            planned for. This is a user-defined value.
        insights_application_info (google.ads.googleads.v23.common.types.AdditionalApplicationInfo):
            Optional. Additional information on the
            application issuing the request.
        country_location (google.ads.googleads.v23.common.types.LocationInfo):
            Required. The country to find trends in.
        search_audience (google.ads.googleads.v23.services.types.SearchAudience):
            An audience to search for trending content
            in.

            This field is a member of `oneof`_ ``criteria``.
        search_topics (google.ads.googleads.v23.services.types.SearchTopics):
            Content topics to return trend information
            for.

            This field is a member of `oneof`_ ``criteria``.
    """

    customer_id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    customer_insights_group: str = proto.Field(
        proto.STRING,
        number=2,
    )
    insights_application_info: (
        additional_application_info.AdditionalApplicationInfo
    ) = proto.Field(
        proto.MESSAGE,
        number=6,
        message=additional_application_info.AdditionalApplicationInfo,
    )
    country_location: criteria.LocationInfo = proto.Field(
        proto.MESSAGE,
        number=3,
        message=criteria.LocationInfo,
    )
    search_audience: "SearchAudience" = proto.Field(
        proto.MESSAGE,
        number=4,
        oneof="criteria",
        message="SearchAudience",
    )
    search_topics: "SearchTopics" = proto.Field(
        proto.MESSAGE,
        number=5,
        oneof="criteria",
        message="SearchTopics",
    )


class GenerateTrendingInsightsResponse(proto.Message):
    r"""Response message for
    [ContentCreatorInsightsService.GenerateTrendingInsights][google.ads.googleads.v23.services.ContentCreatorInsightsService.GenerateTrendingInsights].

    Attributes:
        trend_insights (MutableSequence[google.ads.googleads.v23.services.types.TrendInsight]):
            The list of trending insights for the given
            criteria.
    """

    trend_insights: MutableSequence["TrendInsight"] = proto.RepeatedField(
        proto.MESSAGE,
        number=1,
        message="TrendInsight",
    )


class YouTubeCreatorInsights(proto.Message):
    r"""A YouTube creator and the insights for this creator.

    Attributes:
        creator_name (str):
            The name of the creator.
        creator_channels (MutableSequence[google.ads.googleads.v23.services.types.YouTubeChannelInsights]):
            The list of YouTube Channels
    """

    creator_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    creator_channels: MutableSequence["YouTubeChannelInsights"] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=2,
            message="YouTubeChannelInsights",
        )
    )


class YouTubeMetrics(proto.Message):
    r"""YouTube Channel metrics.

    Attributes:
        subscriber_count (int):
            The number of subscribers.
        views_count (int):
            The total number of views.
        video_count (int):
            The total number of videos.
        likes_count (int):
            The total number of likes across all videos
            of this channel.
        shares_count (int):
            The total number of shares across all videos
            of this channel.
        comments_count (int):
            The total number of comments across all
            videos of this channel.
        engagement_rate (float):
            The lifetime engagement rate of this channel.
            The value is computed as the total number of
            likes, shares, and comments across all videos
            divided by the total number of video views.
        average_views_per_video (float):
            The average number of views per video in the
            last 28 days.
        average_likes_per_video (float):
            The average number of likes per video in the
            last 28 days.
        average_shares_per_video (float):
            The average number of shares per video in the
            last 28 days.
        average_comments_per_video (float):
            The average number of comments per video in
            the last 28 days.
        shorts_views_count (int):
            The total number of views across all Shorts
            videos of this channel.
        shorts_video_count (int):
            The total number of Shorts videos.
        is_active_shorts_creator (bool):
            When true, this channel has published a
            Shorts video in the last 90 days.
        is_active_live_stream_creator (bool):
            When true, this channel has published a live
            stream in the last 90 days.
        is_brand_connect_creator (bool):
            When true, this creator can be partnered with
            to create original branded content using the
            Google Ads creator partnership platform,
            BrandConnect.

            See
            https://support.google.com/google-ads/answer/13828964
            for more information about BrandConnect.
    """

    subscriber_count: int = proto.Field(
        proto.INT64,
        number=1,
    )
    views_count: int = proto.Field(
        proto.INT64,
        number=2,
    )
    video_count: int = proto.Field(
        proto.INT64,
        number=3,
    )
    likes_count: int = proto.Field(
        proto.INT64,
        number=5,
    )
    shares_count: int = proto.Field(
        proto.INT64,
        number=6,
    )
    comments_count: int = proto.Field(
        proto.INT64,
        number=7,
    )
    engagement_rate: float = proto.Field(
        proto.DOUBLE,
        number=8,
    )
    average_views_per_video: float = proto.Field(
        proto.DOUBLE,
        number=9,
    )
    average_likes_per_video: float = proto.Field(
        proto.DOUBLE,
        number=10,
    )
    average_shares_per_video: float = proto.Field(
        proto.DOUBLE,
        number=11,
    )
    average_comments_per_video: float = proto.Field(
        proto.DOUBLE,
        number=12,
    )
    shorts_views_count: int = proto.Field(
        proto.INT64,
        number=13,
    )
    shorts_video_count: int = proto.Field(
        proto.INT64,
        number=14,
    )
    is_active_shorts_creator: bool = proto.Field(
        proto.BOOL,
        number=4,
    )
    is_active_live_stream_creator: bool = proto.Field(
        proto.BOOL,
        number=16,
    )
    is_brand_connect_creator: bool = proto.Field(
        proto.BOOL,
        number=15,
    )


class YouTubeChannelInsights(proto.Message):
    r"""YouTube Channel insights, and its metadata (such as channel
    name and channel ID), returned for a creator insights response.

    Attributes:
        display_name (str):
            The name of the YouTube Channel.
        youtube_channel (google.ads.googleads.v23.common.types.YouTubeChannelInfo):
            The YouTube Channel ID.
        channel_url (str):
            URL for the channel in the form of
            https://www.youtube.com/channel/{channel_id}.
        channel_description (str):
            Description of the channel.
        handle (str):
            The unique, short, and user-visible
            identifier for the channel starting with an "@"
            symbol (such as "@youtubecreators"). See
            https://support.google.com/youtube/answer/11585688
            for more information.
        thumbnail_url (str):
            URL for a 240px by 240px thumbnail image of
            the channel.
        publish_date (str):
            The date that the channel was created.
            Formatted as "yyyy-mm-dd".
        country_location (google.ads.googleads.v23.common.types.LocationInfo):
            The country with which the channel is
            associated.
        channel_metrics (google.ads.googleads.v23.services.types.YouTubeMetrics):
            The metrics for a YouTube Channel.
        channel_audience_attributes (MutableSequence[google.ads.googleads.v23.common.types.AudienceInsightsAttributeMetadata]):
            The types of audiences and demographics
            linked to the channel's main audience. Audiences
            and demographics have a breakdown of subscriber
            share across dimensions of the same value, such
            as Age Range, Gender, and User Interest.
        channel_attributes (MutableSequence[google.ads.googleads.v23.common.types.AudienceInsightsAttributeMetadata]):
            The attributes associated with the content
            made by a channel.
        top_videos (MutableSequence[google.ads.googleads.v23.common.types.AudienceInsightsAttributeMetadata]):
            The top 10 videos for the channel.
        language_distributions (MutableSequence[google.ads.googleads.v23.services.types.LanguageDistribution]):
            The languages associated with the content
            made by a channel.
        channel_type (str):
            Metadata string associated with the type of
            channel.
        relevance_score (float):
            The relevance score of the channel to the topic being
            searched for weighted by views. The value is between 0 and
            1, with 1 being the most relevant. Only populated for trends
            using search_topics.
    """

    display_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    youtube_channel: criteria.YouTubeChannelInfo = proto.Field(
        proto.MESSAGE,
        number=2,
        message=criteria.YouTubeChannelInfo,
    )
    channel_url: str = proto.Field(
        proto.STRING,
        number=9,
    )
    channel_description: str = proto.Field(
        proto.STRING,
        number=10,
    )
    handle: str = proto.Field(
        proto.STRING,
        number=11,
    )
    thumbnail_url: str = proto.Field(
        proto.STRING,
        number=12,
    )
    publish_date: str = proto.Field(
        proto.STRING,
        number=13,
    )
    country_location: criteria.LocationInfo = proto.Field(
        proto.MESSAGE,
        number=14,
        message=criteria.LocationInfo,
    )
    channel_metrics: "YouTubeMetrics" = proto.Field(
        proto.MESSAGE,
        number=3,
        message="YouTubeMetrics",
    )
    channel_audience_attributes: MutableSequence[
        audience_insights_attribute.AudienceInsightsAttributeMetadata
    ] = proto.RepeatedField(
        proto.MESSAGE,
        number=7,
        message=audience_insights_attribute.AudienceInsightsAttributeMetadata,
    )
    channel_attributes: MutableSequence[
        audience_insights_attribute.AudienceInsightsAttributeMetadata
    ] = proto.RepeatedField(
        proto.MESSAGE,
        number=5,
        message=audience_insights_attribute.AudienceInsightsAttributeMetadata,
    )
    top_videos: MutableSequence[
        audience_insights_attribute.AudienceInsightsAttributeMetadata
    ] = proto.RepeatedField(
        proto.MESSAGE,
        number=8,
        message=audience_insights_attribute.AudienceInsightsAttributeMetadata,
    )
    language_distributions: MutableSequence["LanguageDistribution"] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=15,
            message="LanguageDistribution",
        )
    )
    channel_type: str = proto.Field(
        proto.STRING,
        number=6,
    )
    relevance_score: float = proto.Field(
        proto.DOUBLE,
        number=16,
    )


class SearchAudience(proto.Message):
    r"""A collection of audience attributes that describe an audience
    of viewers. This is used to search for topics trending for the
    defined audience.

    Attributes:
        audience_attributes (MutableSequence[google.ads.googleads.v23.common.types.AudienceInsightsAttribute]):
            Required. Audience attributes that describe
            an audience of viewers. This is used to search
            for topics trending for the defined audience.
    """

    audience_attributes: MutableSequence[
        audience_insights_attribute.AudienceInsightsAttribute
    ] = proto.RepeatedField(
        proto.MESSAGE,
        number=1,
        message=audience_insights_attribute.AudienceInsightsAttribute,
    )


class SearchTopics(proto.Message):
    r"""A collection of content topics to return trend information
    for.

    Attributes:
        entities (MutableSequence[google.ads.googleads.v23.common.types.AudienceInsightsEntity]):
            Required. A list of knowledge graph entities to retrieve
            trend information for. Supported entities are tagged with
            [CONTENT_TRENDING_INSIGHTS][google.ads.googleads.v23.enums.InsightsKnowledgeGraphEntityCapabilitiesEnum.InsightsKnowledgeGraphEntityCapabilities.CONTENT_TRENDING_INSIGHTS].
            Use
            [AudienceInsightsService.ListAudienceInsightsAttributes][google.ads.googleads.v23.services.AudienceInsightsService.ListAudienceInsightsAttributes]
            to get the list of supported entities.
    """

    entities: MutableSequence[
        audience_insights_attribute.AudienceInsightsEntity
    ] = proto.RepeatedField(
        proto.MESSAGE,
        number=1,
        message=audience_insights_attribute.AudienceInsightsEntity,
    )


class TrendInsight(proto.Message):
    r"""A trend insight for a given attribute.

    Attributes:
        trend_attribute (google.ads.googleads.v23.common.types.AudienceInsightsAttributeMetadata):
            The attribute this trend is for.
        trend_metrics (google.ads.googleads.v23.services.types.TrendInsightMetrics):
            Metrics associated with this trend. These
            metrics are for the latest available month and
            the comparison period is 3 months.
        trend (google.ads.googleads.v23.enums.types.InsightsTrendEnum.InsightsTrend):
            The direction of trend (such as RISING or
            DECLINING).
        trend_data_points (MutableSequence[google.ads.googleads.v23.services.types.TrendInsightDataPoint]):
            12 months of historical data for the trend, including the
            most recent month the TrendInsight represents. Each data
            point represents 1 month of data and the comparison period
            is 1 month. The data points are ordered from most recent
            month to least recent month. Only populated for trends using
            search_topics.
        related_videos (MutableSequence[google.ads.googleads.v23.common.types.AudienceInsightsAttributeMetadata]):
            Related videos for this topic. Only populated for trends
            using search_topics.
        related_creators (MutableSequence[google.ads.googleads.v23.services.types.YouTubeCreatorInsights]):
            Related creators for this topic. Only populated for trends
            using search_topics.
    """

    trend_attribute: (
        audience_insights_attribute.AudienceInsightsAttributeMetadata
    ) = proto.Field(
        proto.MESSAGE,
        number=1,
        message=audience_insights_attribute.AudienceInsightsAttributeMetadata,
    )
    trend_metrics: "TrendInsightMetrics" = proto.Field(
        proto.MESSAGE,
        number=2,
        message="TrendInsightMetrics",
    )
    trend: insights_trend.InsightsTrendEnum.InsightsTrend = proto.Field(
        proto.ENUM,
        number=3,
        enum=insights_trend.InsightsTrendEnum.InsightsTrend,
    )
    trend_data_points: MutableSequence["TrendInsightDataPoint"] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=6,
            message="TrendInsightDataPoint",
        )
    )
    related_videos: MutableSequence[
        audience_insights_attribute.AudienceInsightsAttributeMetadata
    ] = proto.RepeatedField(
        proto.MESSAGE,
        number=4,
        message=audience_insights_attribute.AudienceInsightsAttributeMetadata,
    )
    related_creators: MutableSequence["YouTubeCreatorInsights"] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=5,
            message="YouTubeCreatorInsights",
        )
    )


class TrendInsightMetrics(proto.Message):
    r"""Metrics associated with a trend insight.

    Attributes:
        views_count (int):
            The number of views for this trend. This is
            only populated for the latest month of data for
            SearchTopics requests.
        views_indexed_value (int):
            Views value normalized to be in the range
            0-100. This is only populated for SearchTopics
            requests.
        audience_share (float):
            The fraction (from 0 to 1 inclusive) of the
            requested audience that has has searched or
            viewed this trend. This is only populated for
            SearchAudience requests.
        trend_change_percent (float):
            The percentage of the change in the trend's
            value over the comparison period, where 1.0
            represents 100%. If this is 0, it means that the
            trend is emerging (new) or sustained (existing
            but unchanged).
    """

    views_count: int = proto.Field(
        proto.INT64,
        number=1,
    )
    views_indexed_value: int = proto.Field(
        proto.INT64,
        number=4,
    )
    audience_share: float = proto.Field(
        proto.DOUBLE,
        number=2,
    )
    trend_change_percent: float = proto.Field(
        proto.DOUBLE,
        number=3,
    )


class TrendInsightDataPoint(proto.Message):
    r"""Trend data for a specific month.

    Attributes:
        month (str):
            The month that the trend data point
            represents in the string format "YYYY-MM".
        trend_metrics (google.ads.googleads.v23.services.types.TrendInsightMetrics):
            Metrics associated with this trend and month.
            The comparison period for these metrics is 1
            month.
    """

    month: str = proto.Field(
        proto.STRING,
        number=1,
    )
    trend_metrics: "TrendInsightMetrics" = proto.Field(
        proto.MESSAGE,
        number=2,
        message="TrendInsightMetrics",
    )


class LanguageDistribution(proto.Message):
    r"""Languages that pertain to a YouTube channel based on the
    channel content. Only languages above a certain proportion
    threshold are included.

    Attributes:
        language_code (str):
            `Language
            code <https://developers.google.com/google-ads/api/reference/data/codes-formats#languages>`__
            of the language for the YouTube channel.
        proportion (float):
            The proportion (between 0 and 1)  of the
            channel's videos in the language.
    """

    language_code: str = proto.Field(
        proto.STRING,
        number=1,
    )
    proportion: float = proto.Field(
        proto.DOUBLE,
        number=2,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
