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

from google.ads.googleads.v21.common.types import criteria
from google.ads.googleads.v21.enums.types import audience_insights_dimension
from google.ads.googleads.v21.enums.types import (
    insights_knowledge_graph_entity_capabilities,
)
from google.ads.googleads.v21.enums.types import (
    user_list_type as gage_user_list_type,
)

__protobuf__ = proto.module(
    package="google.ads.googleads.v21.common",
    marshal="google.ads.googleads.v21",
    manifest={
        "AudienceInsightsAttributeMetadata",
        "AudienceInsightsAttribute",
        "AudienceInsightsTopic",
        "AudienceInsightsEntity",
        "AudienceInsightsCategory",
        "AudienceInsightsLineup",
        "YouTubeChannelAttributeMetadata",
        "YouTubeVideoAttributeMetadata",
        "LineupAttributeMetadata",
        "LocationAttributeMetadata",
        "UserInterestAttributeMetadata",
        "KnowledgeGraphAttributeMetadata",
        "UserListAttributeMetadata",
        "AudienceInsightsAttributeMetadataGroup",
    },
)


class AudienceInsightsAttributeMetadata(proto.Message):
    r"""An audience attribute, with metadata about it, returned in
    response to a search.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        dimension (google.ads.googleads.v21.enums.types.AudienceInsightsDimensionEnum.AudienceInsightsDimension):
            The type of the attribute.
        attribute (google.ads.googleads.v21.common.types.AudienceInsightsAttribute):
            The attribute itself.
        display_name (str):
            The human-readable name of the attribute.
        display_info (str):
            A string that supplements the display_name to identify the
            attribute. If the dimension is TOPIC, this is a brief
            description of the Knowledge Graph entity, such as "American
            singer-songwriter". If the dimension is CATEGORY, this is
            the complete path to the category in The Product & Service
            taxonomy, for example "/Apparel/Clothing/Outerwear".
        potential_youtube_reach (int):
            An estimate of the number of reachable
            YouTube users matching this attribute in the
            requested location, or zero if that information
            is not available for this attribute. This field
            is not populated in every response.
        subscriber_share (float):
            The share of subscribers within this
            attribute, between and including 0 and
            1. This field is not populated in every
                response.
        viewer_share (float):
            The share of viewers within this attribute,
            between and including 0 and
            1. This field is not populated in every
                response.
        youtube_channel_metadata (google.ads.googleads.v21.common.types.YouTubeChannelAttributeMetadata):
            Special metadata for a YouTube channel.

            This field is a member of `oneof`_ ``dimension_metadata``.
        youtube_video_metadata (google.ads.googleads.v21.common.types.YouTubeVideoAttributeMetadata):
            Special metadata for a YouTube video.

            This field is a member of `oneof`_ ``dimension_metadata``.
        lineup_attribute_metadata (google.ads.googleads.v21.common.types.LineupAttributeMetadata):
            Special metadata for a YouTube Lineup.

            This field is a member of `oneof`_ ``dimension_metadata``.
        location_attribute_metadata (google.ads.googleads.v21.common.types.LocationAttributeMetadata):
            Special metadata for a Location.

            This field is a member of `oneof`_ ``dimension_metadata``.
        user_interest_attribute_metadata (google.ads.googleads.v21.common.types.UserInterestAttributeMetadata):
            Special metadata for a User Interest.

            This field is a member of `oneof`_ ``dimension_metadata``.
        knowledge_graph_attribute_metadata (google.ads.googleads.v21.common.types.KnowledgeGraphAttributeMetadata):
            Special metadata for a Knowledge Graph
            Entity.

            This field is a member of `oneof`_ ``dimension_metadata``.
        user_list_attribute_metadata (google.ads.googleads.v21.common.types.UserListAttributeMetadata):
            Special metadata for a User List.

            This field is a member of `oneof`_ ``dimension_metadata``.
    """

    dimension: (
        audience_insights_dimension.AudienceInsightsDimensionEnum.AudienceInsightsDimension
    ) = proto.Field(
        proto.ENUM,
        number=1,
        enum=audience_insights_dimension.AudienceInsightsDimensionEnum.AudienceInsightsDimension,
    )
    attribute: "AudienceInsightsAttribute" = proto.Field(
        proto.MESSAGE,
        number=2,
        message="AudienceInsightsAttribute",
    )
    display_name: str = proto.Field(
        proto.STRING,
        number=3,
    )
    display_info: str = proto.Field(
        proto.STRING,
        number=4,
    )
    potential_youtube_reach: int = proto.Field(
        proto.INT64,
        number=8,
    )
    subscriber_share: float = proto.Field(
        proto.DOUBLE,
        number=9,
    )
    viewer_share: float = proto.Field(
        proto.DOUBLE,
        number=13,
    )
    youtube_channel_metadata: "YouTubeChannelAttributeMetadata" = proto.Field(
        proto.MESSAGE,
        number=5,
        oneof="dimension_metadata",
        message="YouTubeChannelAttributeMetadata",
    )
    youtube_video_metadata: "YouTubeVideoAttributeMetadata" = proto.Field(
        proto.MESSAGE,
        number=10,
        oneof="dimension_metadata",
        message="YouTubeVideoAttributeMetadata",
    )
    lineup_attribute_metadata: "LineupAttributeMetadata" = proto.Field(
        proto.MESSAGE,
        number=14,
        oneof="dimension_metadata",
        message="LineupAttributeMetadata",
    )
    location_attribute_metadata: "LocationAttributeMetadata" = proto.Field(
        proto.MESSAGE,
        number=7,
        oneof="dimension_metadata",
        message="LocationAttributeMetadata",
    )
    user_interest_attribute_metadata: "UserInterestAttributeMetadata" = (
        proto.Field(
            proto.MESSAGE,
            number=11,
            oneof="dimension_metadata",
            message="UserInterestAttributeMetadata",
        )
    )
    knowledge_graph_attribute_metadata: "KnowledgeGraphAttributeMetadata" = (
        proto.Field(
            proto.MESSAGE,
            number=12,
            oneof="dimension_metadata",
            message="KnowledgeGraphAttributeMetadata",
        )
    )
    user_list_attribute_metadata: "UserListAttributeMetadata" = proto.Field(
        proto.MESSAGE,
        number=15,
        oneof="dimension_metadata",
        message="UserListAttributeMetadata",
    )


class AudienceInsightsAttribute(proto.Message):
    r"""An audience attribute that can be used to request insights about the
    audience. Valid inputs for these fields are available from
    [AudienceInsightsService.ListAudienceInsightsAttributes][google.ads.googleads.v21.services.AudienceInsightsService.ListAudienceInsightsAttributes].

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        age_range (google.ads.googleads.v21.common.types.AgeRangeInfo):
            An audience attribute defined by an age
            range.

            This field is a member of `oneof`_ ``attribute``.
        gender (google.ads.googleads.v21.common.types.GenderInfo):
            An audience attribute defined by a gender.

            This field is a member of `oneof`_ ``attribute``.
        location (google.ads.googleads.v21.common.types.LocationInfo):
            An audience attribute defined by a geographic
            location.

            This field is a member of `oneof`_ ``attribute``.
        user_interest (google.ads.googleads.v21.common.types.UserInterestInfo):
            An Affinity or In-Market audience.

            This field is a member of `oneof`_ ``attribute``.
        entity (google.ads.googleads.v21.common.types.AudienceInsightsEntity):
            An audience attribute defined by interest in
            a topic represented by a Knowledge Graph entity.

            This field is a member of `oneof`_ ``attribute``.
        category (google.ads.googleads.v21.common.types.AudienceInsightsCategory):
            An audience attribute defined by interest in
            a Product & Service category.

            This field is a member of `oneof`_ ``attribute``.
        lineup (google.ads.googleads.v21.common.types.AudienceInsightsLineup):
            A YouTube Lineup.

            This field is a member of `oneof`_ ``attribute``.
        parental_status (google.ads.googleads.v21.common.types.ParentalStatusInfo):
            A Parental Status value (parent, or not a
            parent).

            This field is a member of `oneof`_ ``attribute``.
        income_range (google.ads.googleads.v21.common.types.IncomeRangeInfo):
            A household income percentile range.

            This field is a member of `oneof`_ ``attribute``.
        youtube_channel (google.ads.googleads.v21.common.types.YouTubeChannelInfo):
            A YouTube channel.

            This field is a member of `oneof`_ ``attribute``.
        youtube_video (google.ads.googleads.v21.common.types.YouTubeVideoInfo):
            A YouTube video.

            This field is a member of `oneof`_ ``attribute``.
        device (google.ads.googleads.v21.common.types.DeviceInfo):
            A device type. (Mobile, Desktop, Tablet)

            This field is a member of `oneof`_ ``attribute``.
        user_list (google.ads.googleads.v21.common.types.UserListInfo):
            A User List.

            This field is a member of `oneof`_ ``attribute``.
    """

    age_range: criteria.AgeRangeInfo = proto.Field(
        proto.MESSAGE,
        number=1,
        oneof="attribute",
        message=criteria.AgeRangeInfo,
    )
    gender: criteria.GenderInfo = proto.Field(
        proto.MESSAGE,
        number=2,
        oneof="attribute",
        message=criteria.GenderInfo,
    )
    location: criteria.LocationInfo = proto.Field(
        proto.MESSAGE,
        number=3,
        oneof="attribute",
        message=criteria.LocationInfo,
    )
    user_interest: criteria.UserInterestInfo = proto.Field(
        proto.MESSAGE,
        number=4,
        oneof="attribute",
        message=criteria.UserInterestInfo,
    )
    entity: "AudienceInsightsEntity" = proto.Field(
        proto.MESSAGE,
        number=5,
        oneof="attribute",
        message="AudienceInsightsEntity",
    )
    category: "AudienceInsightsCategory" = proto.Field(
        proto.MESSAGE,
        number=6,
        oneof="attribute",
        message="AudienceInsightsCategory",
    )
    lineup: "AudienceInsightsLineup" = proto.Field(
        proto.MESSAGE,
        number=13,
        oneof="attribute",
        message="AudienceInsightsLineup",
    )
    parental_status: criteria.ParentalStatusInfo = proto.Field(
        proto.MESSAGE,
        number=8,
        oneof="attribute",
        message=criteria.ParentalStatusInfo,
    )
    income_range: criteria.IncomeRangeInfo = proto.Field(
        proto.MESSAGE,
        number=9,
        oneof="attribute",
        message=criteria.IncomeRangeInfo,
    )
    youtube_channel: criteria.YouTubeChannelInfo = proto.Field(
        proto.MESSAGE,
        number=10,
        oneof="attribute",
        message=criteria.YouTubeChannelInfo,
    )
    youtube_video: criteria.YouTubeVideoInfo = proto.Field(
        proto.MESSAGE,
        number=11,
        oneof="attribute",
        message=criteria.YouTubeVideoInfo,
    )
    device: criteria.DeviceInfo = proto.Field(
        proto.MESSAGE,
        number=12,
        oneof="attribute",
        message=criteria.DeviceInfo,
    )
    user_list: criteria.UserListInfo = proto.Field(
        proto.MESSAGE,
        number=14,
        oneof="attribute",
        message=criteria.UserListInfo,
    )


class AudienceInsightsTopic(proto.Message):
    r"""An entity or category representing a topic that defines an
    audience.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        entity (google.ads.googleads.v21.common.types.AudienceInsightsEntity):
            A Knowledge Graph entity

            This field is a member of `oneof`_ ``topic``.
        category (google.ads.googleads.v21.common.types.AudienceInsightsCategory):
            A Product & Service category

            This field is a member of `oneof`_ ``topic``.
    """

    entity: "AudienceInsightsEntity" = proto.Field(
        proto.MESSAGE,
        number=1,
        oneof="topic",
        message="AudienceInsightsEntity",
    )
    category: "AudienceInsightsCategory" = proto.Field(
        proto.MESSAGE,
        number=2,
        oneof="topic",
        message="AudienceInsightsCategory",
    )


class AudienceInsightsEntity(proto.Message):
    r"""A Knowledge Graph entity, represented by its machine id.

    Attributes:
        knowledge_graph_machine_id (str):
            Required. The machine ID (mid) of the
            Knowledge Graph entity.
    """

    knowledge_graph_machine_id: str = proto.Field(
        proto.STRING,
        number=1,
    )


class AudienceInsightsCategory(proto.Message):
    r"""A Product and Service category.

    Attributes:
        category_id (str):
            Required. The criterion ID of the category.
    """

    category_id: str = proto.Field(
        proto.STRING,
        number=1,
    )


class AudienceInsightsLineup(proto.Message):
    r"""A YouTube Lineup.

    Attributes:
        lineup_id (str):
            Required. The numeric ID of the lineup.
    """

    lineup_id: str = proto.Field(
        proto.STRING,
        number=1,
    )


class YouTubeChannelAttributeMetadata(proto.Message):
    r"""Metadata associated with a YouTube channel attribute.

    Attributes:
        subscriber_count (int):
            The approximate number of subscribers to the
            YouTube channel.
    """

    subscriber_count: int = proto.Field(
        proto.INT64,
        number=1,
    )


class YouTubeVideoAttributeMetadata(proto.Message):
    r"""Metadata for a YouTube video attribute.

    Attributes:
        thumbnail_url (str):
            The URL of the video thumbnail, prefixed by
            "https://img.youtube.com/".
        video_url (str):
            The URL of the video, prefixed by
            "https://www.youtube.com/".
        views_count (int):
            The total number of views.
        likes_count (int):
            The total number of likes.
        comments_count (int):
            The total number of comments.
    """

    thumbnail_url: str = proto.Field(
        proto.STRING,
        number=1,
    )
    video_url: str = proto.Field(
        proto.STRING,
        number=2,
    )
    views_count: int = proto.Field(
        proto.INT64,
        number=3,
    )
    likes_count: int = proto.Field(
        proto.INT64,
        number=4,
    )
    comments_count: int = proto.Field(
        proto.INT64,
        number=5,
    )


class LineupAttributeMetadata(proto.Message):
    r"""Metadata associated with a Lineup attribute.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        inventory_country (google.ads.googleads.v21.common.types.LocationInfo):
            The national market associated with the
            lineup.
        median_monthly_inventory (int):
            The median number of impressions per month on
            this lineup.

            This field is a member of `oneof`_ ``_median_monthly_inventory``.
        channel_count_lower_bound (int):
            The lower end of a range containing the
            number of channels in the lineup.

            This field is a member of `oneof`_ ``_channel_count_lower_bound``.
        channel_count_upper_bound (int):
            The upper end of a range containing the
            number of channels in the lineup.

            This field is a member of `oneof`_ ``_channel_count_upper_bound``.
        sample_channels (MutableSequence[google.ads.googleads.v21.common.types.LineupAttributeMetadata.SampleChannel]):
            Examples of channels that are included in the
            lineup.
    """

    class SampleChannel(proto.Message):
        r"""A YouTube channel returned as an example of the content in a
        lineup.

        Attributes:
            youtube_channel (google.ads.googleads.v21.common.types.YouTubeChannelInfo):
                A YouTube channel.
            display_name (str):
                The name of the sample channel.
            youtube_channel_metadata (google.ads.googleads.v21.common.types.YouTubeChannelAttributeMetadata):
                Metadata for the sample channel.
        """

        youtube_channel: criteria.YouTubeChannelInfo = proto.Field(
            proto.MESSAGE,
            number=1,
            message=criteria.YouTubeChannelInfo,
        )
        display_name: str = proto.Field(
            proto.STRING,
            number=2,
        )
        youtube_channel_metadata: "YouTubeChannelAttributeMetadata" = (
            proto.Field(
                proto.MESSAGE,
                number=3,
                message="YouTubeChannelAttributeMetadata",
            )
        )

    inventory_country: criteria.LocationInfo = proto.Field(
        proto.MESSAGE,
        number=1,
        message=criteria.LocationInfo,
    )
    median_monthly_inventory: int = proto.Field(
        proto.INT64,
        number=2,
        optional=True,
    )
    channel_count_lower_bound: int = proto.Field(
        proto.INT64,
        number=3,
        optional=True,
    )
    channel_count_upper_bound: int = proto.Field(
        proto.INT64,
        number=4,
        optional=True,
    )
    sample_channels: MutableSequence[SampleChannel] = proto.RepeatedField(
        proto.MESSAGE,
        number=5,
        message=SampleChannel,
    )


class LocationAttributeMetadata(proto.Message):
    r"""Metadata associated with a Location attribute.

    Attributes:
        country_location (google.ads.googleads.v21.common.types.LocationInfo):
            The country location that this attribute’s
            sub country location is located in.
    """

    country_location: criteria.LocationInfo = proto.Field(
        proto.MESSAGE,
        number=1,
        message=criteria.LocationInfo,
    )


class UserInterestAttributeMetadata(proto.Message):
    r"""Metadata associated with a User Interest attribute.

    Attributes:
        user_interest_description (str):
            English language text description of the user
            interest category (200 characters max).
    """

    user_interest_description: str = proto.Field(
        proto.STRING,
        number=1,
    )


class KnowledgeGraphAttributeMetadata(proto.Message):
    r"""Metadata associated with a Knowledge Graph Entity attribute.

    Attributes:
        entity_capabilities (MutableSequence[google.ads.googleads.v21.enums.types.InsightsKnowledgeGraphEntityCapabilitiesEnum.InsightsKnowledgeGraphEntityCapabilities]):
            The capabilities of the entity used in
            [ContentCreatorInsightsService][google.ads.googleads.v21.services.ContentCreatorInsightsService].
        related_categories (MutableSequence[google.ads.googleads.v21.common.types.AudienceInsightsAttributeMetadata]):
            A list of CATEGORY attributes related to this
            entity.
    """

    entity_capabilities: MutableSequence[
        insights_knowledge_graph_entity_capabilities.InsightsKnowledgeGraphEntityCapabilitiesEnum.InsightsKnowledgeGraphEntityCapabilities
    ] = proto.RepeatedField(
        proto.ENUM,
        number=1,
        enum=insights_knowledge_graph_entity_capabilities.InsightsKnowledgeGraphEntityCapabilitiesEnum.InsightsKnowledgeGraphEntityCapabilities,
    )
    related_categories: MutableSequence["AudienceInsightsAttributeMetadata"] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=2,
            message="AudienceInsightsAttributeMetadata",
        )
    )


class UserListAttributeMetadata(proto.Message):
    r"""Metadata associated with a User List attribute.

    Attributes:
        user_list_type (google.ads.googleads.v21.enums.types.UserListTypeEnum.UserListType):
            The user list type.
    """

    user_list_type: gage_user_list_type.UserListTypeEnum.UserListType = (
        proto.Field(
            proto.ENUM,
            number=1,
            enum=gage_user_list_type.UserListTypeEnum.UserListType,
        )
    )


class AudienceInsightsAttributeMetadataGroup(proto.Message):
    r"""A group of audience attributes with metadata, returned in
    response to a search.

    Attributes:
        attributes (MutableSequence[google.ads.googleads.v21.common.types.AudienceInsightsAttributeMetadata]):
            Attributes with metadata returned in response
            to a search.
    """

    attributes: MutableSequence["AudienceInsightsAttributeMetadata"] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=1,
            message="AudienceInsightsAttributeMetadata",
        )
    )


__all__ = tuple(sorted(__protobuf__.manifest))
