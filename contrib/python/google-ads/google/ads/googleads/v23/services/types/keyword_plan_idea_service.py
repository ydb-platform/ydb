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

from google.ads.googleads.v23.common.types import criteria
from google.ads.googleads.v23.common.types import dates
from google.ads.googleads.v23.common.types import keyword_plan_common
from google.ads.googleads.v23.enums.types import keyword_match_type
from google.ads.googleads.v23.enums.types import keyword_plan_keyword_annotation
from google.ads.googleads.v23.enums.types import (
    keyword_plan_network as gage_keyword_plan_network,
)


__protobuf__ = proto.module(
    package="google.ads.googleads.v23.services",
    marshal="google.ads.googleads.v23",
    manifest={
        "GenerateKeywordIdeasRequest",
        "KeywordAndUrlSeed",
        "KeywordSeed",
        "SiteSeed",
        "UrlSeed",
        "GenerateKeywordIdeaResponse",
        "GenerateKeywordIdeaResult",
        "GenerateKeywordHistoricalMetricsRequest",
        "GenerateKeywordHistoricalMetricsResponse",
        "GenerateKeywordHistoricalMetricsResult",
        "GenerateAdGroupThemesRequest",
        "GenerateAdGroupThemesResponse",
        "AdGroupKeywordSuggestion",
        "UnusableAdGroup",
        "GenerateKeywordForecastMetricsRequest",
        "CampaignToForecast",
        "ForecastAdGroup",
        "BiddableKeyword",
        "CriterionBidModifier",
        "ManualCpcBiddingStrategy",
        "MaximizeClicksBiddingStrategy",
        "MaximizeConversionsBiddingStrategy",
        "GenerateKeywordForecastMetricsResponse",
        "KeywordForecastMetrics",
    },
)


class GenerateKeywordIdeasRequest(proto.Message):
    r"""Request message for
    [KeywordPlanIdeaService.GenerateKeywordIdeas][google.ads.googleads.v23.services.KeywordPlanIdeaService.GenerateKeywordIdeas].

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        customer_id (str):
            The ID of the customer with the
            recommendation.
        language (str):
            The resource name of the language to target.
            Each keyword belongs to some set of languages; a
            keyword is included if language is one of its
            languages.
            If not set, all keywords will be included.

            This field is a member of `oneof`_ ``_language``.
        geo_target_constants (MutableSequence[str]):
            The resource names of the location to target.
            Maximum is 10. An empty list MAY be used to
            specify all targeting geos.
        include_adult_keywords (bool):
            If true, adult keywords will be included in
            response. The default value is false.
        page_token (str):
            Token of the page to retrieve. If not specified, the first
            page of results will be returned. To request next page of
            results use the value obtained from ``next_page_token`` in
            the previous response. The request fields must match across
            pages.
        page_size (int):
            Number of results to retrieve in a single page. A maximum of
            10,000 results may be returned, if the page_size exceeds
            this, it is ignored. If unspecified, at most 10,000 results
            will be returned. The server may decide to further limit the
            number of returned resources. If the response contains fewer
            than 10,000 results it may not be assumed as last page of
            results.
        keyword_plan_network (google.ads.googleads.v23.enums.types.KeywordPlanNetworkEnum.KeywordPlanNetwork):
            Targeting network.
            If not set, Google Search And Partners Network
            will be used.
        keyword_annotation (MutableSequence[google.ads.googleads.v23.enums.types.KeywordPlanKeywordAnnotationEnum.KeywordPlanKeywordAnnotation]):
            The keyword annotations to include in
            response.
        aggregate_metrics (google.ads.googleads.v23.common.types.KeywordPlanAggregateMetrics):
            The aggregate fields to include in response.
        historical_metrics_options (google.ads.googleads.v23.common.types.HistoricalMetricsOptions):
            The options for historical metrics data.
        keyword_and_url_seed (google.ads.googleads.v23.services.types.KeywordAndUrlSeed):
            A Keyword and a specific Url to generate
            ideas from for example, cars,
            www.example.com/cars.

            This field is a member of `oneof`_ ``seed``.
        keyword_seed (google.ads.googleads.v23.services.types.KeywordSeed):
            A Keyword or phrase to generate ideas from,
            for example, cars.

            This field is a member of `oneof`_ ``seed``.
        url_seed (google.ads.googleads.v23.services.types.UrlSeed):
            A specific url to generate ideas from, for
            example, www.example.com/cars.

            This field is a member of `oneof`_ ``seed``.
        site_seed (google.ads.googleads.v23.services.types.SiteSeed):
            The site to generate ideas from, for example,
            www.example.com.

            This field is a member of `oneof`_ ``seed``.
    """

    customer_id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    language: str = proto.Field(
        proto.STRING,
        number=14,
        optional=True,
    )
    geo_target_constants: MutableSequence[str] = proto.RepeatedField(
        proto.STRING,
        number=15,
    )
    include_adult_keywords: bool = proto.Field(
        proto.BOOL,
        number=10,
    )
    page_token: str = proto.Field(
        proto.STRING,
        number=12,
    )
    page_size: int = proto.Field(
        proto.INT32,
        number=13,
    )
    keyword_plan_network: (
        gage_keyword_plan_network.KeywordPlanNetworkEnum.KeywordPlanNetwork
    ) = proto.Field(
        proto.ENUM,
        number=9,
        enum=gage_keyword_plan_network.KeywordPlanNetworkEnum.KeywordPlanNetwork,
    )
    keyword_annotation: MutableSequence[
        keyword_plan_keyword_annotation.KeywordPlanKeywordAnnotationEnum.KeywordPlanKeywordAnnotation
    ] = proto.RepeatedField(
        proto.ENUM,
        number=17,
        enum=keyword_plan_keyword_annotation.KeywordPlanKeywordAnnotationEnum.KeywordPlanKeywordAnnotation,
    )
    aggregate_metrics: keyword_plan_common.KeywordPlanAggregateMetrics = (
        proto.Field(
            proto.MESSAGE,
            number=16,
            message=keyword_plan_common.KeywordPlanAggregateMetrics,
        )
    )
    historical_metrics_options: keyword_plan_common.HistoricalMetricsOptions = (
        proto.Field(
            proto.MESSAGE,
            number=18,
            message=keyword_plan_common.HistoricalMetricsOptions,
        )
    )
    keyword_and_url_seed: "KeywordAndUrlSeed" = proto.Field(
        proto.MESSAGE,
        number=2,
        oneof="seed",
        message="KeywordAndUrlSeed",
    )
    keyword_seed: "KeywordSeed" = proto.Field(
        proto.MESSAGE,
        number=3,
        oneof="seed",
        message="KeywordSeed",
    )
    url_seed: "UrlSeed" = proto.Field(
        proto.MESSAGE,
        number=5,
        oneof="seed",
        message="UrlSeed",
    )
    site_seed: "SiteSeed" = proto.Field(
        proto.MESSAGE,
        number=11,
        oneof="seed",
        message="SiteSeed",
    )


class KeywordAndUrlSeed(proto.Message):
    r"""Keyword And Url Seed

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        url (str):
            The URL to crawl in order to generate keyword
            ideas.

            This field is a member of `oneof`_ ``_url``.
        keywords (MutableSequence[str]):
            Requires at least one keyword and no more
            than 20 keywords.
    """

    url: str = proto.Field(
        proto.STRING,
        number=3,
        optional=True,
    )
    keywords: MutableSequence[str] = proto.RepeatedField(
        proto.STRING,
        number=4,
    )


class KeywordSeed(proto.Message):
    r"""Keyword Seed

    Attributes:
        keywords (MutableSequence[str]):
            Requires at least one keyword and no more
            than 20 keywords.
    """

    keywords: MutableSequence[str] = proto.RepeatedField(
        proto.STRING,
        number=2,
    )


class SiteSeed(proto.Message):
    r"""Site Seed

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        site (str):
            The domain name of the site. If the customer
            requesting the ideas doesn't own the site
            provided only public information is returned.

            This field is a member of `oneof`_ ``_site``.
    """

    site: str = proto.Field(
        proto.STRING,
        number=2,
        optional=True,
    )


class UrlSeed(proto.Message):
    r"""Url Seed

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        url (str):
            The URL to crawl in order to generate keyword
            ideas.

            This field is a member of `oneof`_ ``_url``.
    """

    url: str = proto.Field(
        proto.STRING,
        number=2,
        optional=True,
    )


class GenerateKeywordIdeaResponse(proto.Message):
    r"""Response message for
    [KeywordPlanIdeaService.GenerateKeywordIdeas][google.ads.googleads.v23.services.KeywordPlanIdeaService.GenerateKeywordIdeas].

    Attributes:
        results (MutableSequence[google.ads.googleads.v23.services.types.GenerateKeywordIdeaResult]):
            Results of generating keyword ideas.
        aggregate_metric_results (google.ads.googleads.v23.common.types.KeywordPlanAggregateMetricResults):
            The aggregate metrics for all keyword ideas.
        next_page_token (str):
            Pagination token used to retrieve the next page of results.
            Pass the content of this string as the ``page_token``
            attribute of the next request. ``next_page_token`` is not
            returned for the last page.
        total_size (int):
            Total number of results available.
    """

    @property
    def raw_page(self):
        return self

    results: MutableSequence["GenerateKeywordIdeaResult"] = proto.RepeatedField(
        proto.MESSAGE,
        number=1,
        message="GenerateKeywordIdeaResult",
    )
    aggregate_metric_results: (
        keyword_plan_common.KeywordPlanAggregateMetricResults
    ) = proto.Field(
        proto.MESSAGE,
        number=4,
        message=keyword_plan_common.KeywordPlanAggregateMetricResults,
    )
    next_page_token: str = proto.Field(
        proto.STRING,
        number=2,
    )
    total_size: int = proto.Field(
        proto.INT64,
        number=3,
    )


class GenerateKeywordIdeaResult(proto.Message):
    r"""The result of generating keyword ideas.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        text (str):
            Text of the keyword idea.
            As in Keyword Plan historical metrics, this text
            may not be an actual keyword, but the canonical
            form of multiple keywords. See
            KeywordPlanKeywordHistoricalMetrics message in
            KeywordPlanService.

            This field is a member of `oneof`_ ``_text``.
        keyword_idea_metrics (google.ads.googleads.v23.common.types.KeywordPlanHistoricalMetrics):
            The historical metrics for the keyword.
        keyword_annotations (google.ads.googleads.v23.common.types.KeywordAnnotations):
            The annotations for the keyword.
            The annotation data is only provided if
            requested.
        close_variants (MutableSequence[str]):
            The list of close variants from the requested
            keywords that are combined into this
            GenerateKeywordIdeaResult. See
            https://support.google.com/google-ads/answer/9342105
            for the definition of "close variants".
    """

    text: str = proto.Field(
        proto.STRING,
        number=5,
        optional=True,
    )
    keyword_idea_metrics: keyword_plan_common.KeywordPlanHistoricalMetrics = (
        proto.Field(
            proto.MESSAGE,
            number=3,
            message=keyword_plan_common.KeywordPlanHistoricalMetrics,
        )
    )
    keyword_annotations: keyword_plan_common.KeywordAnnotations = proto.Field(
        proto.MESSAGE,
        number=6,
        message=keyword_plan_common.KeywordAnnotations,
    )
    close_variants: MutableSequence[str] = proto.RepeatedField(
        proto.STRING,
        number=7,
    )


class GenerateKeywordHistoricalMetricsRequest(proto.Message):
    r"""Request message for
    [KeywordPlanIdeaService.GenerateKeywordHistoricalMetrics][google.ads.googleads.v23.services.KeywordPlanIdeaService.GenerateKeywordHistoricalMetrics].


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        customer_id (str):
            The ID of the customer with the
            recommendation.
        keywords (MutableSequence[str]):
            A list of keywords to get historical metrics.
            Not all inputs will be returned as a result of
            near-exact deduplication. For example, if stats
            for "car" and "cars" are requested, only "car"
            will be returned.
            A maximum of 10,000 keywords can be used.
        language (str):
            The resource name of the language to target.
            Each keyword belongs to some set of languages; a
            keyword is included if language is one of its
            languages.
            If not set, all keywords will be included.

            This field is a member of `oneof`_ ``_language``.
        include_adult_keywords (bool):
            If true, adult keywords will be included in
            response. The default value is false.
        geo_target_constants (MutableSequence[str]):
            The resource names of the location to target.
            Maximum is 10. An empty list MAY be used to
            specify all targeting geos.
        keyword_plan_network (google.ads.googleads.v23.enums.types.KeywordPlanNetworkEnum.KeywordPlanNetwork):
            Targeting network.
            If not set, Google Search And Partners Network
            will be used.
        aggregate_metrics (google.ads.googleads.v23.common.types.KeywordPlanAggregateMetrics):
            The aggregate fields to include in response.
        historical_metrics_options (google.ads.googleads.v23.common.types.HistoricalMetricsOptions):
            The options for historical metrics data.
    """

    customer_id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    keywords: MutableSequence[str] = proto.RepeatedField(
        proto.STRING,
        number=2,
    )
    language: str = proto.Field(
        proto.STRING,
        number=4,
        optional=True,
    )
    include_adult_keywords: bool = proto.Field(
        proto.BOOL,
        number=5,
    )
    geo_target_constants: MutableSequence[str] = proto.RepeatedField(
        proto.STRING,
        number=6,
    )
    keyword_plan_network: (
        gage_keyword_plan_network.KeywordPlanNetworkEnum.KeywordPlanNetwork
    ) = proto.Field(
        proto.ENUM,
        number=7,
        enum=gage_keyword_plan_network.KeywordPlanNetworkEnum.KeywordPlanNetwork,
    )
    aggregate_metrics: keyword_plan_common.KeywordPlanAggregateMetrics = (
        proto.Field(
            proto.MESSAGE,
            number=8,
            message=keyword_plan_common.KeywordPlanAggregateMetrics,
        )
    )
    historical_metrics_options: keyword_plan_common.HistoricalMetricsOptions = (
        proto.Field(
            proto.MESSAGE,
            number=3,
            message=keyword_plan_common.HistoricalMetricsOptions,
        )
    )


class GenerateKeywordHistoricalMetricsResponse(proto.Message):
    r"""Response message for
    [KeywordPlanIdeaService.GenerateKeywordHistoricalMetrics][google.ads.googleads.v23.services.KeywordPlanIdeaService.GenerateKeywordHistoricalMetrics].

    Attributes:
        results (MutableSequence[google.ads.googleads.v23.services.types.GenerateKeywordHistoricalMetricsResult]):
            List of keywords and their historical
            metrics.
        aggregate_metric_results (google.ads.googleads.v23.common.types.KeywordPlanAggregateMetricResults):
            The aggregate metrics for all keywords.
    """

    results: MutableSequence["GenerateKeywordHistoricalMetricsResult"] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=1,
            message="GenerateKeywordHistoricalMetricsResult",
        )
    )
    aggregate_metric_results: (
        keyword_plan_common.KeywordPlanAggregateMetricResults
    ) = proto.Field(
        proto.MESSAGE,
        number=2,
        message=keyword_plan_common.KeywordPlanAggregateMetricResults,
    )


class GenerateKeywordHistoricalMetricsResult(proto.Message):
    r"""The result of generating keyword historical metrics.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        text (str):
            The text of the query associated with one or more keywords.
            Note that we de-dupe your keywords list, eliminating close
            variants before returning the keywords as text. For example,
            if your request originally contained the keywords "car" and
            "cars", the returned search query will only contain "cars".
            The list of de-duped queries will be included in
            close_variants field.

            This field is a member of `oneof`_ ``_text``.
        close_variants (MutableSequence[str]):
            The list of close variants from the requested
            keywords whose stats are combined into this
            GenerateKeywordHistoricalMetricsResult.
        keyword_metrics (google.ads.googleads.v23.common.types.KeywordPlanHistoricalMetrics):
            The historical metrics for text and its close
            variants
    """

    text: str = proto.Field(
        proto.STRING,
        number=1,
        optional=True,
    )
    close_variants: MutableSequence[str] = proto.RepeatedField(
        proto.STRING,
        number=3,
    )
    keyword_metrics: keyword_plan_common.KeywordPlanHistoricalMetrics = (
        proto.Field(
            proto.MESSAGE,
            number=2,
            message=keyword_plan_common.KeywordPlanHistoricalMetrics,
        )
    )


class GenerateAdGroupThemesRequest(proto.Message):
    r"""Request message for
    [KeywordPlanIdeaService.GenerateAdGroupThemes][google.ads.googleads.v23.services.KeywordPlanIdeaService.GenerateAdGroupThemes].

    Attributes:
        customer_id (str):
            Required. The ID of the customer.
        keywords (MutableSequence[str]):
            Required. A list of keywords to group into
            the provided AdGroups.
        ad_groups (MutableSequence[str]):
            Required. A list of resource names of AdGroups to group
            keywords into. Resource name format:
            ``customers/{customer_id}/adGroups/{ad_group_id}``
    """

    customer_id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    keywords: MutableSequence[str] = proto.RepeatedField(
        proto.STRING,
        number=2,
    )
    ad_groups: MutableSequence[str] = proto.RepeatedField(
        proto.STRING,
        number=3,
    )


class GenerateAdGroupThemesResponse(proto.Message):
    r"""Response message for
    [KeywordPlanIdeaService.GenerateAdGroupThemes][google.ads.googleads.v23.services.KeywordPlanIdeaService.GenerateAdGroupThemes].

    Attributes:
        ad_group_keyword_suggestions (MutableSequence[google.ads.googleads.v23.services.types.AdGroupKeywordSuggestion]):
            A list of suggested AdGroup/keyword pairings.
        unusable_ad_groups (MutableSequence[google.ads.googleads.v23.services.types.UnusableAdGroup]):
            A list of provided AdGroups that could not be
            used as suggestions.
    """

    ad_group_keyword_suggestions: MutableSequence[
        "AdGroupKeywordSuggestion"
    ] = proto.RepeatedField(
        proto.MESSAGE,
        number=1,
        message="AdGroupKeywordSuggestion",
    )
    unusable_ad_groups: MutableSequence["UnusableAdGroup"] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=2,
            message="UnusableAdGroup",
        )
    )


class AdGroupKeywordSuggestion(proto.Message):
    r"""The suggested text and AdGroup/Campaign pairing for a given
    keyword.

    Attributes:
        keyword_text (str):
            The original keyword text.
        suggested_keyword_text (str):
            The normalized version of keyword_text for
            BROAD/EXACT/PHRASE suggestions.
        suggested_match_type (google.ads.googleads.v23.enums.types.KeywordMatchTypeEnum.KeywordMatchType):
            The suggested keyword match type.
        suggested_ad_group (str):
            The suggested AdGroup for the keyword. Resource name format:
            ``customers/{customer_id}/adGroups/{ad_group_id}``
        suggested_campaign (str):
            The suggested Campaign for the keyword. Resource name
            format: ``customers/{customer_id}/campaigns/{campaign_id}``
    """

    keyword_text: str = proto.Field(
        proto.STRING,
        number=1,
    )
    suggested_keyword_text: str = proto.Field(
        proto.STRING,
        number=2,
    )
    suggested_match_type: (
        keyword_match_type.KeywordMatchTypeEnum.KeywordMatchType
    ) = proto.Field(
        proto.ENUM,
        number=3,
        enum=keyword_match_type.KeywordMatchTypeEnum.KeywordMatchType,
    )
    suggested_ad_group: str = proto.Field(
        proto.STRING,
        number=4,
    )
    suggested_campaign: str = proto.Field(
        proto.STRING,
        number=5,
    )


class UnusableAdGroup(proto.Message):
    r"""An AdGroup/Campaign pair that could not be used as a suggestion for
    keywords.

    AdGroups may not be usable if the AdGroup

    - belongs to a Campaign that is not ENABLED or PAUSED
    - is itself not ENABLED

    Attributes:
        ad_group (str):
            The AdGroup resource name. Resource name format:
            ``customers/{customer_id}/adGroups/{ad_group_id}``
        campaign (str):
            The Campaign resource name. Resource name format:
            ``customers/{customer_id}/campaigns/{campaign_id}``
    """

    ad_group: str = proto.Field(
        proto.STRING,
        number=1,
    )
    campaign: str = proto.Field(
        proto.STRING,
        number=2,
    )


class GenerateKeywordForecastMetricsRequest(proto.Message):
    r"""Request message for
    [KeywordPlanIdeaService.GenerateKeywordForecastMetrics][google.ads.googleads.v23.services.KeywordPlanIdeaService.GenerateKeywordForecastMetrics].


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        customer_id (str):
            The ID of the customer.
        currency_code (str):
            The currency used for exchange rate
            conversion. By default, the account currency of
            the customer is used. Set this field only if the
            currency is different from the account currency.
            The list of valid currency codes can be found at
            https://developers.google.com/google-ads/api/data/codes-formats#currency-codes.

            This field is a member of `oneof`_ ``_currency_code``.
        forecast_period (google.ads.googleads.v23.common.types.DateRange):
            The date range for the forecast. The start
            date must be in the future and end date must be
            within 1 year from today. The reference timezone
            used is the one of the Google Ads account
            belonging to the customer. If not set, a default
            date range from next Sunday to the following
            Saturday will be used.
        campaign (google.ads.googleads.v23.services.types.CampaignToForecast):
            Required. The campaign used in the forecast.
    """

    customer_id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    currency_code: str = proto.Field(
        proto.STRING,
        number=2,
        optional=True,
    )
    forecast_period: dates.DateRange = proto.Field(
        proto.MESSAGE,
        number=3,
        message=dates.DateRange,
    )
    campaign: "CampaignToForecast" = proto.Field(
        proto.MESSAGE,
        number=4,
        message="CampaignToForecast",
    )


class CampaignToForecast(proto.Message):
    r"""A campaign to do a keyword campaign forecast.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        language_constants (MutableSequence[str]):
            The list of resource names of languages to be targeted. The
            resource name is of the format
            "languageConstants/{criterion_id}". See
            https://developers.google.com/google-ads/api/data/codes-formats#languages
            for the list of language criterion codes.
        geo_modifiers (MutableSequence[google.ads.googleads.v23.services.types.CriterionBidModifier]):
            Locations to be targeted. Locations must be
            unique.
        keyword_plan_network (google.ads.googleads.v23.enums.types.KeywordPlanNetworkEnum.KeywordPlanNetwork):
            Required. The network used for targeting.
        negative_keywords (MutableSequence[google.ads.googleads.v23.common.types.KeywordInfo]):
            The list of negative keywords to be used in
            the campaign when doing the forecast.
        bidding_strategy (google.ads.googleads.v23.services.types.CampaignToForecast.CampaignBiddingStrategy):
            Required. The bidding strategy for the
            campaign.
        conversion_rate (float):
            The expected conversion rate (number of
            conversions divided by number of total clicks)
            as defined by the user. This value is expressed
            as a decimal value, so an expected conversion
            rate of 2% should be entered as 0.02. If left
            empty, an estimated conversion rate will be
            used.

            This field is a member of `oneof`_ ``_conversion_rate``.
        ad_groups (MutableSequence[google.ads.googleads.v23.services.types.ForecastAdGroup]):
            The ad groups in the new campaign to
            forecast.
    """

    class CampaignBiddingStrategy(proto.Message):
        r"""Supported bidding strategies for new campaign forecasts.

        This message has `oneof`_ fields (mutually exclusive fields).
        For each oneof, at most one member field can be set at the same time.
        Setting any member of the oneof automatically clears all other
        members.

        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            manual_cpc_bidding_strategy (google.ads.googleads.v23.services.types.ManualCpcBiddingStrategy):
                Use manual CPC bidding strategy for
                forecasting.

                This field is a member of `oneof`_ ``bidding_strategy``.
            maximize_clicks_bidding_strategy (google.ads.googleads.v23.services.types.MaximizeClicksBiddingStrategy):
                Use maximize clicks bidding strategy for
                forecasting.

                This field is a member of `oneof`_ ``bidding_strategy``.
            maximize_conversions_bidding_strategy (google.ads.googleads.v23.services.types.MaximizeConversionsBiddingStrategy):
                Use maximize conversions bidding strategy for
                forecasting.

                This field is a member of `oneof`_ ``bidding_strategy``.
        """

        manual_cpc_bidding_strategy: "ManualCpcBiddingStrategy" = proto.Field(
            proto.MESSAGE,
            number=1,
            oneof="bidding_strategy",
            message="ManualCpcBiddingStrategy",
        )
        maximize_clicks_bidding_strategy: "MaximizeClicksBiddingStrategy" = (
            proto.Field(
                proto.MESSAGE,
                number=2,
                oneof="bidding_strategy",
                message="MaximizeClicksBiddingStrategy",
            )
        )
        maximize_conversions_bidding_strategy: (
            "MaximizeConversionsBiddingStrategy"
        ) = proto.Field(
            proto.MESSAGE,
            number=3,
            oneof="bidding_strategy",
            message="MaximizeConversionsBiddingStrategy",
        )

    language_constants: MutableSequence[str] = proto.RepeatedField(
        proto.STRING,
        number=1,
    )
    geo_modifiers: MutableSequence["CriterionBidModifier"] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=2,
            message="CriterionBidModifier",
        )
    )
    keyword_plan_network: (
        gage_keyword_plan_network.KeywordPlanNetworkEnum.KeywordPlanNetwork
    ) = proto.Field(
        proto.ENUM,
        number=3,
        enum=gage_keyword_plan_network.KeywordPlanNetworkEnum.KeywordPlanNetwork,
    )
    negative_keywords: MutableSequence[criteria.KeywordInfo] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=4,
            message=criteria.KeywordInfo,
        )
    )
    bidding_strategy: CampaignBiddingStrategy = proto.Field(
        proto.MESSAGE,
        number=5,
        message=CampaignBiddingStrategy,
    )
    conversion_rate: float = proto.Field(
        proto.DOUBLE,
        number=6,
        optional=True,
    )
    ad_groups: MutableSequence["ForecastAdGroup"] = proto.RepeatedField(
        proto.MESSAGE,
        number=7,
        message="ForecastAdGroup",
    )


class ForecastAdGroup(proto.Message):
    r"""An ad group that is part of a campaign to be forecasted.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        max_cpc_bid_micros (int):
            The max cpc to use for the ad group when
            generating forecasted traffic. This value will
            override the max cpc value set in the bidding
            strategy. Only specify this field for bidding
            strategies that max cpc values.

            This field is a member of `oneof`_ ``_max_cpc_bid_micros``.
        biddable_keywords (MutableSequence[google.ads.googleads.v23.services.types.BiddableKeyword]):
            Required. The list of biddable keywords to be
            used in the ad group when doing the forecast.
            Requires at least one keyword.
        negative_keywords (MutableSequence[google.ads.googleads.v23.common.types.KeywordInfo]):
            The details of the keyword. You should
            specify both the keyword text and match type.
    """

    max_cpc_bid_micros: int = proto.Field(
        proto.INT64,
        number=1,
        optional=True,
    )
    biddable_keywords: MutableSequence["BiddableKeyword"] = proto.RepeatedField(
        proto.MESSAGE,
        number=2,
        message="BiddableKeyword",
    )
    negative_keywords: MutableSequence[criteria.KeywordInfo] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=3,
            message=criteria.KeywordInfo,
        )
    )


class BiddableKeyword(proto.Message):
    r"""A biddable keyword part of an ad group.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        keyword (google.ads.googleads.v23.common.types.KeywordInfo):
            Required. Keyword. Must have text and match
            type.
        max_cpc_bid_micros (int):
            A max cpc bid in micros that overrides the ad
            group level max cpc bid in forecast simulation.
            This value will override the max cpc value set
            in the bidding strategy and ad group. Only
            specify this field for bidding strategies that
            support max cpc values.

            This field is a member of `oneof`_ ``_max_cpc_bid_micros``.
    """

    keyword: criteria.KeywordInfo = proto.Field(
        proto.MESSAGE,
        number=1,
        message=criteria.KeywordInfo,
    )
    max_cpc_bid_micros: int = proto.Field(
        proto.INT64,
        number=2,
        optional=True,
    )


class CriterionBidModifier(proto.Message):
    r"""Location Criterion bid modifier.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        geo_target_constant (str):
            The resource name of the geo location to target. The
            resource name is of the format
            "geoTargetConstants/{criterion_id}".
        bid_modifier (float):
            The associated multiplier for the criterion_id. If set, this
            value cannot be 0.

            This field is a member of `oneof`_ ``_bid_modifier``.
    """

    geo_target_constant: str = proto.Field(
        proto.STRING,
        number=1,
    )
    bid_modifier: float = proto.Field(
        proto.DOUBLE,
        number=2,
        optional=True,
    )


class ManualCpcBiddingStrategy(proto.Message):
    r"""Manual CPC Bidding Strategy.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        daily_budget_micros (int):
            Campaign level budget in micros. If set, a
            minimum value is enforced for the local currency
            used in the campaign. An error will occur
            showing the minimum value if this field is set
            too low.

            This field is a member of `oneof`_ ``_daily_budget_micros``.
        max_cpc_bid_micros (int):
            Required. A bid in micros to be applied to ad
            groups within the campaign for a manual CPC
            bidding strategy.
    """

    daily_budget_micros: int = proto.Field(
        proto.INT64,
        number=1,
        optional=True,
    )
    max_cpc_bid_micros: int = proto.Field(
        proto.INT64,
        number=2,
    )


class MaximizeClicksBiddingStrategy(proto.Message):
    r"""Maximize Clicks Bidding Strategy.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        daily_target_spend_micros (int):
            Required. The daily target spend in micros to
            be used for estimation. A minimum value is
            enforced for the local currency used in the
            campaign. An error will occur showing the
            minimum value if this field is set too low.
        max_cpc_bid_ceiling_micros (int):
            Ceiling on max CPC bids in micros.

            This field is a member of `oneof`_ ``_max_cpc_bid_ceiling_micros``.
    """

    daily_target_spend_micros: int = proto.Field(
        proto.INT64,
        number=1,
    )
    max_cpc_bid_ceiling_micros: int = proto.Field(
        proto.INT64,
        number=2,
        optional=True,
    )


class MaximizeConversionsBiddingStrategy(proto.Message):
    r"""Maximize Conversions Bidding Strategy.

    Attributes:
        daily_target_spend_micros (int):
            Required. The daily target spend in micros to
            be used for estimation. This value must be
            greater than zero.
    """

    daily_target_spend_micros: int = proto.Field(
        proto.INT64,
        number=1,
    )


class GenerateKeywordForecastMetricsResponse(proto.Message):
    r"""Response message for
    [KeywordPlanIdeaService.GenerateKeywordForecastMetrics][google.ads.googleads.v23.services.KeywordPlanIdeaService.GenerateKeywordForecastMetrics].


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        campaign_forecast_metrics (google.ads.googleads.v23.services.types.KeywordForecastMetrics):
            Results of the campaign forecast.

            This field is a member of `oneof`_ ``_campaign_forecast_metrics``.
    """

    campaign_forecast_metrics: "KeywordForecastMetrics" = proto.Field(
        proto.MESSAGE,
        number=1,
        optional=True,
        message="KeywordForecastMetrics",
    )


class KeywordForecastMetrics(proto.Message):
    r"""The forecast metrics for the planless keyword campaign.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        impressions (float):
            The total number of impressions.

            This field is a member of `oneof`_ ``_impressions``.
        click_through_rate (float):
            The average click through rate. Available
            only if impressions > 0.

            This field is a member of `oneof`_ ``_click_through_rate``.
        average_cpc_micros (int):
            The average cpc. Available only if clicks >
            0.

            This field is a member of `oneof`_ ``_average_cpc_micros``.
        clicks (float):
            The total number of clicks.

            This field is a member of `oneof`_ ``_clicks``.
        cost_micros (int):
            The total cost.

            This field is a member of `oneof`_ ``_cost_micros``.
        conversions (float):
            Forecasted number of conversions: clicks \* conversion_rate.

            This field is a member of `oneof`_ ``_conversions``.
        conversion_rate (float):
            Forecasted conversion rate.

            This field is a member of `oneof`_ ``_conversion_rate``.
        average_cpa_micros (int):
            Average cost per acquisition calculated as cost_micros /
            conversions.

            This field is a member of `oneof`_ ``_average_cpa_micros``.
    """

    impressions: float = proto.Field(
        proto.DOUBLE,
        number=1,
        optional=True,
    )
    click_through_rate: float = proto.Field(
        proto.DOUBLE,
        number=2,
        optional=True,
    )
    average_cpc_micros: int = proto.Field(
        proto.INT64,
        number=3,
        optional=True,
    )
    clicks: float = proto.Field(
        proto.DOUBLE,
        number=4,
        optional=True,
    )
    cost_micros: int = proto.Field(
        proto.INT64,
        number=5,
        optional=True,
    )
    conversions: float = proto.Field(
        proto.DOUBLE,
        number=6,
        optional=True,
    )
    conversion_rate: float = proto.Field(
        proto.DOUBLE,
        number=7,
        optional=True,
    )
    average_cpa_micros: int = proto.Field(
        proto.INT64,
        number=8,
        optional=True,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
