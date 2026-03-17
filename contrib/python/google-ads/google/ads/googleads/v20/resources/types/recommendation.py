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

from google.ads.googleads.v20.common.types import criteria
from google.ads.googleads.v20.enums.types import ad_strength as gage_ad_strength
from google.ads.googleads.v20.enums.types import (
    app_bidding_goal as gage_app_bidding_goal,
)
from google.ads.googleads.v20.enums.types import keyword_match_type
from google.ads.googleads.v20.enums.types import recommendation_type
from google.ads.googleads.v20.enums.types import (
    shopping_add_products_to_campaign_recommendation_enum,
)
from google.ads.googleads.v20.enums.types import (
    target_cpa_opt_in_recommendation_goal,
)
from google.ads.googleads.v20.resources.types import ad as gagr_ad
from google.ads.googleads.v20.resources.types import asset


__protobuf__ = proto.module(
    package="google.ads.googleads.v20.resources",
    marshal="google.ads.googleads.v20",
    manifest={
        "Recommendation",
    },
)


class Recommendation(proto.Message):
    r"""A recommendation.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Immutable. The resource name of the recommendation.

            ``customers/{customer_id}/recommendations/{recommendation_id}``
        type_ (google.ads.googleads.v20.enums.types.RecommendationTypeEnum.RecommendationType):
            Output only. The type of recommendation.
        impact (google.ads.googleads.v20.resources.types.Recommendation.RecommendationImpact):
            Output only. The impact on account
            performance as a result of applying the
            recommendation.
        campaign_budget (str):
            Output only. The budget targeted by this recommendation.
            This will be set only when the recommendation affects a
            single campaign budget.

            This field will be set for the following recommendation
            types: CAMPAIGN_BUDGET, FORECASTING_CAMPAIGN_BUDGET,
            MARGINAL_ROI_CAMPAIGN_BUDGET, MOVE_UNUSED_BUDGET

            This field is a member of `oneof`_ ``_campaign_budget``.
        campaign (str):
            Output only. The campaign targeted by this recommendation.

            This field will be set for the following recommendation
            types: CALL_EXTENSION, CALLOUT_EXTENSION,
            ENHANCED_CPC_OPT_IN, USE_BROAD_MATCH_KEYWORD, KEYWORD,
            KEYWORD_MATCH_TYPE,
            UPGRADE_LOCAL_CAMPAIGN_TO_PERFORMANCE_MAX,
            MAXIMIZE_CLICKS_OPT_IN, MAXIMIZE_CONVERSIONS_OPT_IN,
            OPTIMIZE_AD_ROTATION, RESPONSIVE_SEARCH_AD,
            RESPONSIVE_SEARCH_AD_ASSET, SEARCH_PARTNERS_OPT_IN,
            DISPLAY_EXPANSION_OPT_IN, SITELINK_EXTENSION,
            TARGET_CPA_OPT_IN, TARGET_ROAS_OPT_IN, TEXT_AD,
            UPGRADE_SMART_SHOPPING_CAMPAIGN_TO_PERFORMANCE_MAX,
            RAISE_TARGET_CPA_BID_TOO_LOW, FORECASTING_SET_TARGET_ROAS,
            SHOPPING_ADD_AGE_GROUP, SHOPPING_ADD_COLOR,
            SHOPPING_ADD_GENDER, SHOPPING_ADD_SIZE, SHOPPING_ADD_GTIN,
            SHOPPING_ADD_MORE_IDENTIFIERS,
            SHOPPING_ADD_PRODUCTS_TO_CAMPAIGN,
            SHOPPING_FIX_DISAPPROVED_PRODUCTS,
            SHOPPING_MIGRATE_REGULAR_SHOPPING_CAMPAIGN_OFFERS_TO_PERFORMANCE_MAX,
            DYNAMIC_IMAGE_EXTENSION_OPT_IN, RAISE_TARGET_CPA,
            LOWER_TARGET_ROAS, FORECASTING_SET_TARGET_CPA,
            SET_TARGET_CPA, SET_TARGET_ROAS,
            MAXIMIZE_CONVERSION_VALUE_OPT_IN,
            IMPROVE_GOOGLE_TAG_COVERAGE,
            PERFORMANCE_MAX_FINAL_URL_OPT_IN

            This field is a member of `oneof`_ ``_campaign``.
        ad_group (str):
            Output only. The ad group targeted by this recommendation.
            This will be set only when the recommendation affects a
            single ad group.

            This field will be set for the following recommendation
            types: KEYWORD, OPTIMIZE_AD_ROTATION, RESPONSIVE_SEARCH_AD,
            RESPONSIVE_SEARCH_AD_ASSET, TEXT_AD

            This field is a member of `oneof`_ ``_ad_group``.
        dismissed (bool):
            Output only. Whether the recommendation is
            dismissed or not.

            This field is a member of `oneof`_ ``_dismissed``.
        campaigns (MutableSequence[str]):
            Output only. The campaigns targeted by this recommendation.

            This field will be set for the following recommendation
            types: CAMPAIGN_BUDGET, FORECASTING_CAMPAIGN_BUDGET,
            MARGINAL_ROI_CAMPAIGN_BUDGET and MOVE_UNUSED_BUDGET
        campaign_budget_recommendation (google.ads.googleads.v20.resources.types.Recommendation.CampaignBudgetRecommendation):
            Output only. The campaign budget
            recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        forecasting_campaign_budget_recommendation (google.ads.googleads.v20.resources.types.Recommendation.CampaignBudgetRecommendation):
            Output only. The forecasting campaign budget
            recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        keyword_recommendation (google.ads.googleads.v20.resources.types.Recommendation.KeywordRecommendation):
            Output only. The keyword recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        text_ad_recommendation (google.ads.googleads.v20.resources.types.Recommendation.TextAdRecommendation):
            Output only. Add expanded text ad
            recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        target_cpa_opt_in_recommendation (google.ads.googleads.v20.resources.types.Recommendation.TargetCpaOptInRecommendation):
            Output only. The TargetCPA opt-in
            recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        maximize_conversions_opt_in_recommendation (google.ads.googleads.v20.resources.types.Recommendation.MaximizeConversionsOptInRecommendation):
            Output only. The MaximizeConversions Opt-In
            recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        enhanced_cpc_opt_in_recommendation (google.ads.googleads.v20.resources.types.Recommendation.EnhancedCpcOptInRecommendation):
            Output only. The Enhanced Cost-Per-Click
            Opt-In recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        search_partners_opt_in_recommendation (google.ads.googleads.v20.resources.types.Recommendation.SearchPartnersOptInRecommendation):
            Output only. The Search Partners Opt-In
            recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        maximize_clicks_opt_in_recommendation (google.ads.googleads.v20.resources.types.Recommendation.MaximizeClicksOptInRecommendation):
            Output only. The MaximizeClicks Opt-In
            recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        optimize_ad_rotation_recommendation (google.ads.googleads.v20.resources.types.Recommendation.OptimizeAdRotationRecommendation):
            Output only. The Optimize Ad Rotation
            recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        keyword_match_type_recommendation (google.ads.googleads.v20.resources.types.Recommendation.KeywordMatchTypeRecommendation):
            Output only. The keyword match type
            recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        move_unused_budget_recommendation (google.ads.googleads.v20.resources.types.Recommendation.MoveUnusedBudgetRecommendation):
            Output only. The move unused budget
            recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        target_roas_opt_in_recommendation (google.ads.googleads.v20.resources.types.Recommendation.TargetRoasOptInRecommendation):
            Output only. The Target ROAS opt-in
            recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        responsive_search_ad_recommendation (google.ads.googleads.v20.resources.types.Recommendation.ResponsiveSearchAdRecommendation):
            Output only. The add responsive search ad
            recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        marginal_roi_campaign_budget_recommendation (google.ads.googleads.v20.resources.types.Recommendation.CampaignBudgetRecommendation):
            Output only. The marginal ROI campaign budget
            recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        use_broad_match_keyword_recommendation (google.ads.googleads.v20.resources.types.Recommendation.UseBroadMatchKeywordRecommendation):
            Output only. The use broad match keyword
            recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        responsive_search_ad_asset_recommendation (google.ads.googleads.v20.resources.types.Recommendation.ResponsiveSearchAdAssetRecommendation):
            Output only. The responsive search ad asset
            recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        upgrade_smart_shopping_campaign_to_performance_max_recommendation (google.ads.googleads.v20.resources.types.Recommendation.UpgradeSmartShoppingCampaignToPerformanceMaxRecommendation):
            Output only. The upgrade a Smart Shopping
            campaign to a Performance Max campaign
            recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        responsive_search_ad_improve_ad_strength_recommendation (google.ads.googleads.v20.resources.types.Recommendation.ResponsiveSearchAdImproveAdStrengthRecommendation):
            Output only. The responsive search ad improve
            ad strength recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        display_expansion_opt_in_recommendation (google.ads.googleads.v20.resources.types.Recommendation.DisplayExpansionOptInRecommendation):
            Output only. The Display Expansion opt-in
            recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        upgrade_local_campaign_to_performance_max_recommendation (google.ads.googleads.v20.resources.types.Recommendation.UpgradeLocalCampaignToPerformanceMaxRecommendation):
            Output only. The upgrade a Local campaign to
            a Performance Max campaign recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        raise_target_cpa_bid_too_low_recommendation (google.ads.googleads.v20.resources.types.Recommendation.RaiseTargetCpaBidTooLowRecommendation):
            Output only. The raise target CPA bid too low
            recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        forecasting_set_target_roas_recommendation (google.ads.googleads.v20.resources.types.Recommendation.ForecastingSetTargetRoasRecommendation):
            Output only. The forecasting set target ROAS
            recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        callout_asset_recommendation (google.ads.googleads.v20.resources.types.Recommendation.CalloutAssetRecommendation):
            Output only. The callout asset
            recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        sitelink_asset_recommendation (google.ads.googleads.v20.resources.types.Recommendation.SitelinkAssetRecommendation):
            Output only. The sitelink asset
            recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        call_asset_recommendation (google.ads.googleads.v20.resources.types.Recommendation.CallAssetRecommendation):
            Output only. The call asset recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        shopping_add_age_group_recommendation (google.ads.googleads.v20.resources.types.Recommendation.ShoppingOfferAttributeRecommendation):
            Output only. The shopping add age group
            recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        shopping_add_color_recommendation (google.ads.googleads.v20.resources.types.Recommendation.ShoppingOfferAttributeRecommendation):
            Output only. The shopping add color
            recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        shopping_add_gender_recommendation (google.ads.googleads.v20.resources.types.Recommendation.ShoppingOfferAttributeRecommendation):
            Output only. The shopping add gender
            recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        shopping_add_gtin_recommendation (google.ads.googleads.v20.resources.types.Recommendation.ShoppingOfferAttributeRecommendation):
            Output only. The shopping add GTIN
            recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        shopping_add_more_identifiers_recommendation (google.ads.googleads.v20.resources.types.Recommendation.ShoppingOfferAttributeRecommendation):
            Output only. The shopping add more
            identifiers recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        shopping_add_size_recommendation (google.ads.googleads.v20.resources.types.Recommendation.ShoppingOfferAttributeRecommendation):
            Output only. The shopping add size
            recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        shopping_add_products_to_campaign_recommendation (google.ads.googleads.v20.resources.types.Recommendation.ShoppingAddProductsToCampaignRecommendation):
            Output only. The shopping add products to
            campaign recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        shopping_fix_disapproved_products_recommendation (google.ads.googleads.v20.resources.types.Recommendation.ShoppingFixDisapprovedProductsRecommendation):
            Output only. The shopping fix disapproved
            products recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        shopping_target_all_offers_recommendation (google.ads.googleads.v20.resources.types.Recommendation.ShoppingTargetAllOffersRecommendation):
            Output only. The shopping target all offers
            recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        shopping_fix_suspended_merchant_center_account_recommendation (google.ads.googleads.v20.resources.types.Recommendation.ShoppingMerchantCenterAccountSuspensionRecommendation):
            Output only. The shopping fix suspended
            Merchant Center account recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        shopping_fix_merchant_center_account_suspension_warning_recommendation (google.ads.googleads.v20.resources.types.Recommendation.ShoppingMerchantCenterAccountSuspensionRecommendation):
            Output only. The shopping fix Merchant Center
            account suspension warning recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        shopping_migrate_regular_shopping_campaign_offers_to_performance_max_recommendation (google.ads.googleads.v20.resources.types.Recommendation.ShoppingMigrateRegularShoppingCampaignOffersToPerformanceMaxRecommendation):
            Output only. The shopping migrate Regular
            Shopping Campaign offers to Performance Max
            recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        dynamic_image_extension_opt_in_recommendation (google.ads.googleads.v20.resources.types.Recommendation.DynamicImageExtensionOptInRecommendation):
            Output only. Recommendation to enable dynamic
            image extensions on the account, allowing Google
            to find the best images from ad landing pages
            and complement text ads.

            This field is a member of `oneof`_ ``recommendation``.
        raise_target_cpa_recommendation (google.ads.googleads.v20.resources.types.Recommendation.RaiseTargetCpaRecommendation):
            Output only. Recommendation to raise Target
            CPA.

            This field is a member of `oneof`_ ``recommendation``.
        lower_target_roas_recommendation (google.ads.googleads.v20.resources.types.Recommendation.LowerTargetRoasRecommendation):
            Output only. Recommendation to lower Target
            ROAS.

            This field is a member of `oneof`_ ``recommendation``.
        performance_max_opt_in_recommendation (google.ads.googleads.v20.resources.types.Recommendation.PerformanceMaxOptInRecommendation):
            Output only. The Performance Max Opt In
            recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        improve_performance_max_ad_strength_recommendation (google.ads.googleads.v20.resources.types.Recommendation.ImprovePerformanceMaxAdStrengthRecommendation):
            Output only. The improve Performance Max ad
            strength recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        migrate_dynamic_search_ads_campaign_to_performance_max_recommendation (google.ads.googleads.v20.resources.types.Recommendation.MigrateDynamicSearchAdsCampaignToPerformanceMaxRecommendation):
            Output only. The Dynamic Search Ads to
            Performance Max migration recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        forecasting_set_target_cpa_recommendation (google.ads.googleads.v20.resources.types.Recommendation.ForecastingSetTargetCpaRecommendation):
            Output only. The forecasting set target CPA
            recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        set_target_cpa_recommendation (google.ads.googleads.v20.resources.types.Recommendation.ForecastingSetTargetCpaRecommendation):
            Output only. The set target CPA
            recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        set_target_roas_recommendation (google.ads.googleads.v20.resources.types.Recommendation.ForecastingSetTargetRoasRecommendation):
            Output only. The set target ROAS
            recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        maximize_conversion_value_opt_in_recommendation (google.ads.googleads.v20.resources.types.Recommendation.MaximizeConversionValueOptInRecommendation):
            Output only. The Maximize Conversion Value
            opt-in recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        improve_google_tag_coverage_recommendation (google.ads.googleads.v20.resources.types.Recommendation.ImproveGoogleTagCoverageRecommendation):
            Output only. Recommendation to deploy Google
            Tag on more pages.

            This field is a member of `oneof`_ ``recommendation``.
        performance_max_final_url_opt_in_recommendation (google.ads.googleads.v20.resources.types.Recommendation.PerformanceMaxFinalUrlOptInRecommendation):
            Output only. Recommendation to turn on Final
            URL expansion for your Performance Max
            campaigns.

            This field is a member of `oneof`_ ``recommendation``.
        refresh_customer_match_list_recommendation (google.ads.googleads.v20.resources.types.Recommendation.RefreshCustomerMatchListRecommendation):
            Output only. The refresh customer list
            recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        custom_audience_opt_in_recommendation (google.ads.googleads.v20.resources.types.Recommendation.CustomAudienceOptInRecommendation):
            Output only. The custom audience opt in
            recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        lead_form_asset_recommendation (google.ads.googleads.v20.resources.types.Recommendation.LeadFormAssetRecommendation):
            Output only. The lead form asset
            recommendation.

            This field is a member of `oneof`_ ``recommendation``.
        improve_demand_gen_ad_strength_recommendation (google.ads.googleads.v20.resources.types.Recommendation.ImproveDemandGenAdStrengthRecommendation):
            Output only. The improve Demand Gen ad
            strength recommendation.

            This field is a member of `oneof`_ ``recommendation``.
    """

    class MerchantInfo(proto.Message):
        r"""The Merchant Center account details.

        Attributes:
            id (int):
                Output only. The Merchant Center account ID.
            name (str):
                Output only. The name of the Merchant Center
                account.
            multi_client (bool):
                Output only. Whether the Merchant Center
                account is a Multi-Client account (MCA).
        """

        id: int = proto.Field(
            proto.INT64,
            number=1,
        )
        name: str = proto.Field(
            proto.STRING,
            number=2,
        )
        multi_client: bool = proto.Field(
            proto.BOOL,
            number=3,
        )

    class RecommendationImpact(proto.Message):
        r"""The impact of making the change as described in the
        recommendation. Some types of recommendations may not have
        impact information.

        Attributes:
            base_metrics (google.ads.googleads.v20.resources.types.Recommendation.RecommendationMetrics):
                Output only. Base metrics at the time the
                recommendation was generated.
            potential_metrics (google.ads.googleads.v20.resources.types.Recommendation.RecommendationMetrics):
                Output only. Estimated metrics if the
                recommendation is applied.
        """

        base_metrics: "Recommendation.RecommendationMetrics" = proto.Field(
            proto.MESSAGE,
            number=1,
            message="Recommendation.RecommendationMetrics",
        )
        potential_metrics: "Recommendation.RecommendationMetrics" = proto.Field(
            proto.MESSAGE,
            number=2,
            message="Recommendation.RecommendationMetrics",
        )

    class RecommendationMetrics(proto.Message):
        r"""Weekly account performance metrics. For some recommendation
        types, these are averaged over the past 90-day period and hence
        can be fractional.


        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            impressions (float):
                Output only. Number of ad impressions.

                This field is a member of `oneof`_ ``_impressions``.
            clicks (float):
                Output only. Number of ad clicks.

                This field is a member of `oneof`_ ``_clicks``.
            cost_micros (int):
                Output only. Cost (in micros) for
                advertising, in the local currency for the
                account.

                This field is a member of `oneof`_ ``_cost_micros``.
            conversions (float):
                Output only. Number of conversions.

                This field is a member of `oneof`_ ``_conversions``.
            conversions_value (float):
                Output only. Sum of the conversion value of
                the conversions.

                This field is a member of `oneof`_ ``_conversions_value``.
            video_views (float):
                Output only. Number of video views for a
                video ad campaign.

                This field is a member of `oneof`_ ``_video_views``.
        """

        impressions: float = proto.Field(
            proto.DOUBLE,
            number=6,
            optional=True,
        )
        clicks: float = proto.Field(
            proto.DOUBLE,
            number=7,
            optional=True,
        )
        cost_micros: int = proto.Field(
            proto.INT64,
            number=8,
            optional=True,
        )
        conversions: float = proto.Field(
            proto.DOUBLE,
            number=9,
            optional=True,
        )
        conversions_value: float = proto.Field(
            proto.DOUBLE,
            number=11,
            optional=True,
        )
        video_views: float = proto.Field(
            proto.DOUBLE,
            number=10,
            optional=True,
        )

    class CampaignBudgetRecommendation(proto.Message):
        r"""The budget recommendation for budget constrained campaigns.

        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            current_budget_amount_micros (int):
                Output only. The current budget amount in
                micros.

                This field is a member of `oneof`_ ``_current_budget_amount_micros``.
            recommended_budget_amount_micros (int):
                Output only. The recommended budget amount in
                micros.

                This field is a member of `oneof`_ ``_recommended_budget_amount_micros``.
            budget_options (MutableSequence[google.ads.googleads.v20.resources.types.Recommendation.CampaignBudgetRecommendation.CampaignBudgetRecommendationOption]):
                Output only. The budget amounts and
                associated impact estimates for some values of
                possible budget amounts.
        """

        class CampaignBudgetRecommendationOption(proto.Message):
            r"""The impact estimates for a given budget amount.

            .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

            Attributes:
                budget_amount_micros (int):
                    Output only. The budget amount for this
                    option.

                    This field is a member of `oneof`_ ``_budget_amount_micros``.
                impact (google.ads.googleads.v20.resources.types.Recommendation.RecommendationImpact):
                    Output only. The impact estimate if budget is
                    changed to amount specified in this option.
            """

            budget_amount_micros: int = proto.Field(
                proto.INT64,
                number=3,
                optional=True,
            )
            impact: "Recommendation.RecommendationImpact" = proto.Field(
                proto.MESSAGE,
                number=2,
                message="Recommendation.RecommendationImpact",
            )

        current_budget_amount_micros: int = proto.Field(
            proto.INT64,
            number=7,
            optional=True,
        )
        recommended_budget_amount_micros: int = proto.Field(
            proto.INT64,
            number=8,
            optional=True,
        )
        budget_options: MutableSequence[
            "Recommendation.CampaignBudgetRecommendation.CampaignBudgetRecommendationOption"
        ] = proto.RepeatedField(
            proto.MESSAGE,
            number=3,
            message="Recommendation.CampaignBudgetRecommendation.CampaignBudgetRecommendationOption",
        )

    class KeywordRecommendation(proto.Message):
        r"""The keyword recommendation.

        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            keyword (google.ads.googleads.v20.common.types.KeywordInfo):
                Output only. The recommended keyword.
            search_terms (MutableSequence[google.ads.googleads.v20.resources.types.Recommendation.KeywordRecommendation.SearchTerm]):
                Output only. A list of search terms this
                keyword matches. The same search term may be
                repeated for multiple keywords.
            recommended_cpc_bid_micros (int):
                Output only. The recommended CPC
                (cost-per-click) bid.

                This field is a member of `oneof`_ ``_recommended_cpc_bid_micros``.
        """

        class SearchTerm(proto.Message):
            r"""Information about a search term as related to a keyword
            recommendation.

            Attributes:
                text (str):
                    Output only. The text of the search term.
                estimated_weekly_search_count (int):
                    Output only. Estimated number of historical
                    weekly searches for this search term.
            """

            text: str = proto.Field(
                proto.STRING,
                number=1,
            )
            estimated_weekly_search_count: int = proto.Field(
                proto.INT64,
                number=2,
            )

        keyword: criteria.KeywordInfo = proto.Field(
            proto.MESSAGE,
            number=1,
            message=criteria.KeywordInfo,
        )
        search_terms: MutableSequence[
            "Recommendation.KeywordRecommendation.SearchTerm"
        ] = proto.RepeatedField(
            proto.MESSAGE,
            number=4,
            message="Recommendation.KeywordRecommendation.SearchTerm",
        )
        recommended_cpc_bid_micros: int = proto.Field(
            proto.INT64,
            number=3,
            optional=True,
        )

    class TextAdRecommendation(proto.Message):
        r"""The text ad recommendation.

        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            ad (google.ads.googleads.v20.resources.types.Ad):
                Output only. Recommended ad.
            creation_date (str):
                Output only. Creation date of the recommended
                ad. YYYY-MM-DD format, for example, 2018-04-17.

                This field is a member of `oneof`_ ``_creation_date``.
            auto_apply_date (str):
                Output only. Date, if present, is the
                earliest when the recommendation will be auto
                applied. YYYY-MM-DD format, for example,
                2018-04-17.

                This field is a member of `oneof`_ ``_auto_apply_date``.
        """

        ad: gagr_ad.Ad = proto.Field(
            proto.MESSAGE,
            number=1,
            message=gagr_ad.Ad,
        )
        creation_date: str = proto.Field(
            proto.STRING,
            number=4,
            optional=True,
        )
        auto_apply_date: str = proto.Field(
            proto.STRING,
            number=5,
            optional=True,
        )

    class TargetCpaOptInRecommendation(proto.Message):
        r"""The Target CPA opt-in recommendation.

        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            options (MutableSequence[google.ads.googleads.v20.resources.types.Recommendation.TargetCpaOptInRecommendation.TargetCpaOptInRecommendationOption]):
                Output only. The available goals and
                corresponding options for Target CPA strategy.
            recommended_target_cpa_micros (int):
                Output only. The recommended average CPA
                target. See required budget amount and impact of
                using this recommendation in options list.

                This field is a member of `oneof`_ ``_recommended_target_cpa_micros``.
        """

        class TargetCpaOptInRecommendationOption(proto.Message):
            r"""The Target CPA opt-in option with impact estimate.

            .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

            Attributes:
                goal (google.ads.googleads.v20.enums.types.TargetCpaOptInRecommendationGoalEnum.TargetCpaOptInRecommendationGoal):
                    Output only. The goal achieved by this
                    option.
                target_cpa_micros (int):
                    Output only. Average CPA target.

                    This field is a member of `oneof`_ ``_target_cpa_micros``.
                required_campaign_budget_amount_micros (int):
                    Output only. The minimum campaign budget, in
                    local currency for the account, required to
                    achieve the target CPA. Amount is specified in
                    micros, where one million is equivalent to one
                    currency unit.

                    This field is a member of `oneof`_ ``_required_campaign_budget_amount_micros``.
                impact (google.ads.googleads.v20.resources.types.Recommendation.RecommendationImpact):
                    Output only. The impact estimate if this
                    option is selected.
            """

            goal: (
                target_cpa_opt_in_recommendation_goal.TargetCpaOptInRecommendationGoalEnum.TargetCpaOptInRecommendationGoal
            ) = proto.Field(
                proto.ENUM,
                number=1,
                enum=target_cpa_opt_in_recommendation_goal.TargetCpaOptInRecommendationGoalEnum.TargetCpaOptInRecommendationGoal,
            )
            target_cpa_micros: int = proto.Field(
                proto.INT64,
                number=5,
                optional=True,
            )
            required_campaign_budget_amount_micros: int = proto.Field(
                proto.INT64,
                number=6,
                optional=True,
            )
            impact: "Recommendation.RecommendationImpact" = proto.Field(
                proto.MESSAGE,
                number=4,
                message="Recommendation.RecommendationImpact",
            )

        options: MutableSequence[
            "Recommendation.TargetCpaOptInRecommendation.TargetCpaOptInRecommendationOption"
        ] = proto.RepeatedField(
            proto.MESSAGE,
            number=1,
            message="Recommendation.TargetCpaOptInRecommendation.TargetCpaOptInRecommendationOption",
        )
        recommended_target_cpa_micros: int = proto.Field(
            proto.INT64,
            number=3,
            optional=True,
        )

    class MaximizeConversionsOptInRecommendation(proto.Message):
        r"""The Maximize Conversions Opt-In recommendation.

        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            recommended_budget_amount_micros (int):
                Output only. The recommended new budget
                amount.

                This field is a member of `oneof`_ ``_recommended_budget_amount_micros``.
        """

        recommended_budget_amount_micros: int = proto.Field(
            proto.INT64,
            number=2,
            optional=True,
        )

    class EnhancedCpcOptInRecommendation(proto.Message):
        r"""The Enhanced Cost-Per-Click Opt-In recommendation."""

    class SearchPartnersOptInRecommendation(proto.Message):
        r"""The Search Partners Opt-In recommendation."""

    class MaximizeClicksOptInRecommendation(proto.Message):
        r"""The Maximize Clicks opt-in recommendation.

        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            recommended_budget_amount_micros (int):
                Output only. The recommended new budget
                amount. Only set if the current budget is too
                high.

                This field is a member of `oneof`_ ``_recommended_budget_amount_micros``.
        """

        recommended_budget_amount_micros: int = proto.Field(
            proto.INT64,
            number=2,
            optional=True,
        )

    class OptimizeAdRotationRecommendation(proto.Message):
        r"""The Optimize Ad Rotation recommendation."""

    class CalloutAssetRecommendation(proto.Message):
        r"""The callout asset recommendation.

        Attributes:
            recommended_campaign_callout_assets (MutableSequence[google.ads.googleads.v20.resources.types.Asset]):
                Output only. New callout extension assets
                recommended at the campaign level.
            recommended_customer_callout_assets (MutableSequence[google.ads.googleads.v20.resources.types.Asset]):
                Output only. New callout extension assets
                recommended at the customer level.
        """

        recommended_campaign_callout_assets: MutableSequence[asset.Asset] = (
            proto.RepeatedField(
                proto.MESSAGE,
                number=1,
                message=asset.Asset,
            )
        )
        recommended_customer_callout_assets: MutableSequence[asset.Asset] = (
            proto.RepeatedField(
                proto.MESSAGE,
                number=2,
                message=asset.Asset,
            )
        )

    class SitelinkAssetRecommendation(proto.Message):
        r"""The sitelink asset recommendation.

        Attributes:
            recommended_campaign_sitelink_assets (MutableSequence[google.ads.googleads.v20.resources.types.Asset]):
                Output only. New sitelink assets recommended
                at the campaign level.
            recommended_customer_sitelink_assets (MutableSequence[google.ads.googleads.v20.resources.types.Asset]):
                Output only. New sitelink assets recommended
                at the customer level.
        """

        recommended_campaign_sitelink_assets: MutableSequence[asset.Asset] = (
            proto.RepeatedField(
                proto.MESSAGE,
                number=1,
                message=asset.Asset,
            )
        )
        recommended_customer_sitelink_assets: MutableSequence[asset.Asset] = (
            proto.RepeatedField(
                proto.MESSAGE,
                number=2,
                message=asset.Asset,
            )
        )

    class CallAssetRecommendation(proto.Message):
        r"""The call asset recommendation."""

    class KeywordMatchTypeRecommendation(proto.Message):
        r"""The keyword match type recommendation.

        Attributes:
            keyword (google.ads.googleads.v20.common.types.KeywordInfo):
                Output only. The existing keyword where the
                match type should be more broad.
            recommended_match_type (google.ads.googleads.v20.enums.types.KeywordMatchTypeEnum.KeywordMatchType):
                Output only. The recommended new match type.
        """

        keyword: criteria.KeywordInfo = proto.Field(
            proto.MESSAGE,
            number=1,
            message=criteria.KeywordInfo,
        )
        recommended_match_type: (
            keyword_match_type.KeywordMatchTypeEnum.KeywordMatchType
        ) = proto.Field(
            proto.ENUM,
            number=2,
            enum=keyword_match_type.KeywordMatchTypeEnum.KeywordMatchType,
        )

    class MoveUnusedBudgetRecommendation(proto.Message):
        r"""The move unused budget recommendation.

        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            excess_campaign_budget (str):
                Output only. The excess budget's resource_name.

                This field is a member of `oneof`_ ``_excess_campaign_budget``.
            budget_recommendation (google.ads.googleads.v20.resources.types.Recommendation.CampaignBudgetRecommendation):
                Output only. The recommendation for the
                constrained budget to increase.
        """

        excess_campaign_budget: str = proto.Field(
            proto.STRING,
            number=3,
            optional=True,
        )
        budget_recommendation: "Recommendation.CampaignBudgetRecommendation" = (
            proto.Field(
                proto.MESSAGE,
                number=2,
                message="Recommendation.CampaignBudgetRecommendation",
            )
        )

    class TargetRoasOptInRecommendation(proto.Message):
        r"""The Target ROAS opt-in recommendation.

        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            recommended_target_roas (float):
                Output only. The recommended target ROAS
                (revenue per unit of spend). The value is
                between 0.01 and 1000.0, inclusive.

                This field is a member of `oneof`_ ``_recommended_target_roas``.
            required_campaign_budget_amount_micros (int):
                Output only. The minimum campaign budget, in
                local currency for the account, required to
                achieve the target ROAS. Amount is specified in
                micros, where one million is equivalent to one
                currency unit.

                This field is a member of `oneof`_ ``_required_campaign_budget_amount_micros``.
        """

        recommended_target_roas: float = proto.Field(
            proto.DOUBLE,
            number=1,
            optional=True,
        )
        required_campaign_budget_amount_micros: int = proto.Field(
            proto.INT64,
            number=2,
            optional=True,
        )

    class ResponsiveSearchAdAssetRecommendation(proto.Message):
        r"""The add responsive search ad asset recommendation.

        Attributes:
            current_ad (google.ads.googleads.v20.resources.types.Ad):
                Output only. The current ad to be updated.
            recommended_assets (google.ads.googleads.v20.resources.types.Ad):
                Output only. The recommended assets. This is
                populated only with the new headlines and/or
                descriptions, and is otherwise empty.
        """

        current_ad: gagr_ad.Ad = proto.Field(
            proto.MESSAGE,
            number=3,
            message=gagr_ad.Ad,
        )
        recommended_assets: gagr_ad.Ad = proto.Field(
            proto.MESSAGE,
            number=2,
            message=gagr_ad.Ad,
        )

    class ResponsiveSearchAdImproveAdStrengthRecommendation(proto.Message):
        r"""The responsive search ad improve ad strength recommendation.

        Attributes:
            current_ad (google.ads.googleads.v20.resources.types.Ad):
                Output only. The current ad to be updated.
            recommended_ad (google.ads.googleads.v20.resources.types.Ad):
                Output only. The updated ad.
        """

        current_ad: gagr_ad.Ad = proto.Field(
            proto.MESSAGE,
            number=1,
            message=gagr_ad.Ad,
        )
        recommended_ad: gagr_ad.Ad = proto.Field(
            proto.MESSAGE,
            number=2,
            message=gagr_ad.Ad,
        )

    class ResponsiveSearchAdRecommendation(proto.Message):
        r"""The add responsive search ad recommendation.

        Attributes:
            ad (google.ads.googleads.v20.resources.types.Ad):
                Output only. Recommended ad.
        """

        ad: gagr_ad.Ad = proto.Field(
            proto.MESSAGE,
            number=1,
            message=gagr_ad.Ad,
        )

    class UseBroadMatchKeywordRecommendation(proto.Message):
        r"""The use broad match keyword recommendation.

        Attributes:
            keyword (MutableSequence[google.ads.googleads.v20.common.types.KeywordInfo]):
                Output only. Sample of keywords to be
                expanded to Broad Match.
            suggested_keywords_count (int):
                Output only. Total number of keywords to be
                expanded to Broad Match in the campaign.
            campaign_keywords_count (int):
                Output only. Total number of keywords in the
                campaign.
            campaign_uses_shared_budget (bool):
                Output only. Whether the associated campaign
                uses a shared budget.
            required_campaign_budget_amount_micros (int):
                Output only. The budget recommended to avoid
                becoming budget constrained after applying the
                recommendation.
        """

        keyword: MutableSequence[criteria.KeywordInfo] = proto.RepeatedField(
            proto.MESSAGE,
            number=1,
            message=criteria.KeywordInfo,
        )
        suggested_keywords_count: int = proto.Field(
            proto.INT64,
            number=2,
        )
        campaign_keywords_count: int = proto.Field(
            proto.INT64,
            number=3,
        )
        campaign_uses_shared_budget: bool = proto.Field(
            proto.BOOL,
            number=4,
        )
        required_campaign_budget_amount_micros: int = proto.Field(
            proto.INT64,
            number=5,
        )

    class UpgradeSmartShoppingCampaignToPerformanceMaxRecommendation(
        proto.Message
    ):
        r"""The upgrade a Smart Shopping campaign to a Performance Max
        campaign recommendation.

        Attributes:
            merchant_id (int):
                Output only. ID of Merchant Center account.
            sales_country_code (str):
                Output only. Country whose products from
                merchant's inventory should be included.
        """

        merchant_id: int = proto.Field(
            proto.INT64,
            number=1,
        )
        sales_country_code: str = proto.Field(
            proto.STRING,
            number=2,
        )

    class RaiseTargetCpaBidTooLowRecommendation(proto.Message):
        r"""The raise target CPA bid too low recommendation.

        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            recommended_target_multiplier (float):
                Output only. A number greater than 1.0
                indicating the factor by which we recommend the
                target CPA should be increased.

                This field is a member of `oneof`_ ``_recommended_target_multiplier``.
            average_target_cpa_micros (int):
                Output only. The current average target CPA
                of the campaign, in micros of customer local
                currency.

                This field is a member of `oneof`_ ``_average_target_cpa_micros``.
        """

        recommended_target_multiplier: float = proto.Field(
            proto.DOUBLE,
            number=1,
            optional=True,
        )
        average_target_cpa_micros: int = proto.Field(
            proto.INT64,
            number=2,
            optional=True,
        )

    class DisplayExpansionOptInRecommendation(proto.Message):
        r"""The Display Expansion opt-in recommendation."""

    class UpgradeLocalCampaignToPerformanceMaxRecommendation(proto.Message):
        r"""The Upgrade Local campaign to Performance Max campaign
        recommendation.

        """

    class ForecastingSetTargetRoasRecommendation(proto.Message):
        r"""The forecasting set target ROAS recommendation.

        Attributes:
            recommended_target_roas (float):
                Output only. The recommended target ROAS
                (revenue per unit of spend). The value is
                between 0.01 and 1000.0, inclusive.
            campaign_budget (google.ads.googleads.v20.resources.types.Recommendation.CampaignBudget):
                Output only. The campaign budget.
        """

        recommended_target_roas: float = proto.Field(
            proto.DOUBLE,
            number=1,
        )
        campaign_budget: "Recommendation.CampaignBudget" = proto.Field(
            proto.MESSAGE,
            number=2,
            message="Recommendation.CampaignBudget",
        )

    class ShoppingOfferAttributeRecommendation(proto.Message):
        r"""The shopping recommendation to add an attribute to offers
        that are demoted because it is missing.

        Attributes:
            merchant (google.ads.googleads.v20.resources.types.Recommendation.MerchantInfo):
                Output only. The details of the Merchant
                Center account.
            feed_label (str):
                Output only. The campaign feed label.
            offers_count (int):
                Output only. The number of online, servable
                offers.
            demoted_offers_count (int):
                Output only. The number of online, servable
                offers that are demoted for missing attributes.
                Visit the Merchant Center for more details.
        """

        merchant: "Recommendation.MerchantInfo" = proto.Field(
            proto.MESSAGE,
            number=1,
            message="Recommendation.MerchantInfo",
        )
        feed_label: str = proto.Field(
            proto.STRING,
            number=2,
        )
        offers_count: int = proto.Field(
            proto.INT64,
            number=3,
        )
        demoted_offers_count: int = proto.Field(
            proto.INT64,
            number=4,
        )

    class ShoppingFixDisapprovedProductsRecommendation(proto.Message):
        r"""The shopping recommendation to fix disapproved products in a
        Shopping Campaign Inventory.

        Attributes:
            merchant (google.ads.googleads.v20.resources.types.Recommendation.MerchantInfo):
                Output only. The details of the Merchant
                Center account.
            feed_label (str):
                Output only. The feed label for the campaign.
            products_count (int):
                Output only. The number of products of the
                campaign.
            disapproved_products_count (int):
                Output only. The numbers of products of the
                campaign that are disapproved.
        """

        merchant: "Recommendation.MerchantInfo" = proto.Field(
            proto.MESSAGE,
            number=1,
            message="Recommendation.MerchantInfo",
        )
        feed_label: str = proto.Field(
            proto.STRING,
            number=2,
        )
        products_count: int = proto.Field(
            proto.INT64,
            number=3,
        )
        disapproved_products_count: int = proto.Field(
            proto.INT64,
            number=4,
        )

    class ShoppingTargetAllOffersRecommendation(proto.Message):
        r"""The shopping recommendation to create a catch-all campaign
        that targets all offers.

        Attributes:
            merchant (google.ads.googleads.v20.resources.types.Recommendation.MerchantInfo):
                Output only. The details of the Merchant
                Center account.
            untargeted_offers_count (int):
                Output only. The number of untargeted offers.
            feed_label (str):
                Output only. The offer feed label.
        """

        merchant: "Recommendation.MerchantInfo" = proto.Field(
            proto.MESSAGE,
            number=1,
            message="Recommendation.MerchantInfo",
        )
        untargeted_offers_count: int = proto.Field(
            proto.INT64,
            number=2,
        )
        feed_label: str = proto.Field(
            proto.STRING,
            number=3,
        )

    class ShoppingAddProductsToCampaignRecommendation(proto.Message):
        r"""The shopping recommendation to add products to a Shopping
        Campaign Inventory.

        Attributes:
            merchant (google.ads.googleads.v20.resources.types.Recommendation.MerchantInfo):
                Output only. The details of the Merchant
                Center account.
            feed_label (str):
                Output only. The feed label for the campaign.
            reason (google.ads.googleads.v20.enums.types.ShoppingAddProductsToCampaignRecommendationEnum.Reason):
                Output only. The reason why no products are
                attached to the campaign.
        """

        merchant: "Recommendation.MerchantInfo" = proto.Field(
            proto.MESSAGE,
            number=1,
            message="Recommendation.MerchantInfo",
        )
        feed_label: str = proto.Field(
            proto.STRING,
            number=2,
        )
        reason: (
            shopping_add_products_to_campaign_recommendation_enum.ShoppingAddProductsToCampaignRecommendationEnum.Reason
        ) = proto.Field(
            proto.ENUM,
            number=3,
            enum=shopping_add_products_to_campaign_recommendation_enum.ShoppingAddProductsToCampaignRecommendationEnum.Reason,
        )

    class ShoppingMerchantCenterAccountSuspensionRecommendation(proto.Message):
        r"""The shopping recommendation to fix Merchant Center account
        suspension issues.

        Attributes:
            merchant (google.ads.googleads.v20.resources.types.Recommendation.MerchantInfo):
                Output only. The details of the Merchant
                Center account.
            feed_label (str):
                Output only. The feed label of the campaign
                for which the suspension happened.
        """

        merchant: "Recommendation.MerchantInfo" = proto.Field(
            proto.MESSAGE,
            number=1,
            message="Recommendation.MerchantInfo",
        )
        feed_label: str = proto.Field(
            proto.STRING,
            number=2,
        )

    class ShoppingMigrateRegularShoppingCampaignOffersToPerformanceMaxRecommendation(
        proto.Message
    ):
        r"""The shopping recommendation to migrate Regular Shopping
        Campaign targeted offers to Performance Max campaigns.

        Attributes:
            merchant (google.ads.googleads.v20.resources.types.Recommendation.MerchantInfo):
                Output only. The details of the Merchant
                Center account.
            feed_label (str):
                Output only. The feed label of the offers
                targeted by the campaigns sharing this
                suggestion.
        """

        merchant: "Recommendation.MerchantInfo" = proto.Field(
            proto.MESSAGE,
            number=1,
            message="Recommendation.MerchantInfo",
        )
        feed_label: str = proto.Field(
            proto.STRING,
            number=2,
        )

    class TargetAdjustmentInfo(proto.Message):
        r"""Information of a target adjustment recommendation.

        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            shared_set (str):
                Output only. The shared set resource name of
                the portfolio bidding strategy where the target
                is defined. Only populated if the recommendation
                is portfolio level.

                This field is a member of `oneof`_ ``_shared_set``.
            recommended_target_multiplier (float):
                Output only. The factor by which we recommend
                the target to be adjusted by.
            current_average_target_micros (int):
                Output only. The current average target of
                the campaign or portfolio targeted by this
                recommendation.
        """

        shared_set: str = proto.Field(
            proto.STRING,
            number=1,
            optional=True,
        )
        recommended_target_multiplier: float = proto.Field(
            proto.DOUBLE,
            number=2,
        )
        current_average_target_micros: int = proto.Field(
            proto.INT64,
            number=3,
        )

    class RaiseTargetCpaRecommendation(proto.Message):
        r"""Recommendation to raise Target CPA.

        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            target_adjustment (google.ads.googleads.v20.resources.types.Recommendation.TargetAdjustmentInfo):
                Output only. The relevant information
                describing the recommended target adjustment.
            app_bidding_goal (google.ads.googleads.v20.enums.types.AppBiddingGoalEnum.AppBiddingGoal):
                Output only. Represents the goal towards
                which the bidding strategy should optimize. Only
                populated for App Campaigns.

                This field is a member of `oneof`_ ``_app_bidding_goal``.
        """

        target_adjustment: "Recommendation.TargetAdjustmentInfo" = proto.Field(
            proto.MESSAGE,
            number=1,
            message="Recommendation.TargetAdjustmentInfo",
        )
        app_bidding_goal: (
            gage_app_bidding_goal.AppBiddingGoalEnum.AppBiddingGoal
        ) = proto.Field(
            proto.ENUM,
            number=2,
            optional=True,
            enum=gage_app_bidding_goal.AppBiddingGoalEnum.AppBiddingGoal,
        )

    class LowerTargetRoasRecommendation(proto.Message):
        r"""Recommendation to lower Target ROAS.

        Attributes:
            target_adjustment (google.ads.googleads.v20.resources.types.Recommendation.TargetAdjustmentInfo):
                Output only. The relevant information
                describing the recommended target adjustment.
        """

        target_adjustment: "Recommendation.TargetAdjustmentInfo" = proto.Field(
            proto.MESSAGE,
            number=1,
            message="Recommendation.TargetAdjustmentInfo",
        )

    class DynamicImageExtensionOptInRecommendation(proto.Message):
        r"""Recommendation to enable dynamic image extensions on the
        account, allowing Google to find the best images from ad landing
        pages and complement text ads.

        """

    class CampaignBudget(proto.Message):
        r"""A campaign budget shared amongst various budget
        recommendation types.

        Attributes:
            current_amount_micros (int):
                Output only. Current budget amount.
            recommended_new_amount_micros (int):
                Output only. Recommended budget amount.
            new_start_date (str):
                Output only. The date when the new budget would start being
                used. This field will be set for the following
                recommendation types: FORECASTING_SET_TARGET_ROAS ,
                FORECASTING_SET_TARGET_CPA YYYY-MM-DD format, for example,
                2018-04-17.
        """

        current_amount_micros: int = proto.Field(
            proto.INT64,
            number=1,
        )
        recommended_new_amount_micros: int = proto.Field(
            proto.INT64,
            number=2,
        )
        new_start_date: str = proto.Field(
            proto.STRING,
            number=3,
        )

    class PerformanceMaxOptInRecommendation(proto.Message):
        r"""The Performance Max Opt In recommendation."""

    class ImprovePerformanceMaxAdStrengthRecommendation(proto.Message):
        r"""Recommendation to improve the asset group strength of a
        Performance Max campaign to an "Excellent" rating.

        Attributes:
            asset_group (str):
                Output only. The asset group resource name.
            ad_strength (google.ads.googleads.v20.enums.types.AdStrengthEnum.AdStrength):
                Output only. The current ad strength score of
                the asset group.
        """

        asset_group: str = proto.Field(
            proto.STRING,
            number=1,
        )
        ad_strength: gage_ad_strength.AdStrengthEnum.AdStrength = proto.Field(
            proto.ENUM,
            number=2,
            enum=gage_ad_strength.AdStrengthEnum.AdStrength,
        )

    class MigrateDynamicSearchAdsCampaignToPerformanceMaxRecommendation(
        proto.Message
    ):
        r"""The Dynamic Search Ads to Performance Max migration
        recommendation.

        Attributes:
            apply_link (str):
                Output only. A link to the Google Ads UI
                where the customer can manually apply the
                recommendation.
        """

        apply_link: str = proto.Field(
            proto.STRING,
            number=1,
        )

    class ForecastingSetTargetCpaRecommendation(proto.Message):
        r"""The set target CPA recommendations.

        Attributes:
            recommended_target_cpa_micros (int):
                Output only. The recommended target CPA.
            campaign_budget (google.ads.googleads.v20.resources.types.Recommendation.CampaignBudget):
                Output only. The campaign budget.
        """

        recommended_target_cpa_micros: int = proto.Field(
            proto.INT64,
            number=1,
        )
        campaign_budget: "Recommendation.CampaignBudget" = proto.Field(
            proto.MESSAGE,
            number=2,
            message="Recommendation.CampaignBudget",
        )

    class MaximizeConversionValueOptInRecommendation(proto.Message):
        r"""Recommendation to opt into Maximize Conversion Value bidding
        strategy.

        """

    class ImproveGoogleTagCoverageRecommendation(proto.Message):
        r"""Recommendation to deploy Google Tag on more pages."""

    class PerformanceMaxFinalUrlOptInRecommendation(proto.Message):
        r"""Recommendation to turn on Final URL expansion for your
        Performance Max campaigns.

        """

    class RefreshCustomerMatchListRecommendation(proto.Message):
        r"""The recommendation to update a customer list that hasn't been
        updated in the last 90 days. The customer receiving the
        recommendation is not necessarily the owner account. The owner
        account should update the customer list.

        Attributes:
            user_list_id (int):
                Output only. The user list ID.
            user_list_name (str):
                Output only. The name of the list.
            days_since_last_refresh (int):
                Output only. Days since last refresh.
            top_spending_account (MutableSequence[google.ads.googleads.v20.resources.types.Recommendation.AccountInfo]):
                Output only. The top spending account.
            targeting_accounts_count (int):
                Output only. User lists can be shared with other accounts by
                the owner. targeting_accounts_count is the number of those
                accounts that can use it for targeting.
            owner_account (google.ads.googleads.v20.resources.types.Recommendation.AccountInfo):
                Output only. The owner account. This is the
                account that should update the customer list.
        """

        user_list_id: int = proto.Field(
            proto.INT64,
            number=1,
        )
        user_list_name: str = proto.Field(
            proto.STRING,
            number=2,
        )
        days_since_last_refresh: int = proto.Field(
            proto.INT64,
            number=3,
        )
        top_spending_account: MutableSequence["Recommendation.AccountInfo"] = (
            proto.RepeatedField(
                proto.MESSAGE,
                number=4,
                message="Recommendation.AccountInfo",
            )
        )
        targeting_accounts_count: int = proto.Field(
            proto.INT64,
            number=5,
        )
        owner_account: "Recommendation.AccountInfo" = proto.Field(
            proto.MESSAGE,
            number=6,
            message="Recommendation.AccountInfo",
        )

    class AccountInfo(proto.Message):
        r"""Wrapper for information about a Google Ads account.

        Attributes:
            customer_id (int):
                Output only. The customer ID of the account.
            descriptive_name (str):
                Output only. The descriptive name of the
                account.
        """

        customer_id: int = proto.Field(
            proto.INT64,
            number=1,
        )
        descriptive_name: str = proto.Field(
            proto.STRING,
            number=2,
        )

    class CustomAudienceOptInRecommendation(proto.Message):
        r"""The Custom Audience Opt In recommendation.

        Attributes:
            keywords (MutableSequence[google.ads.googleads.v20.common.types.KeywordInfo]):
                Output only. The list of keywords to use for
                custom audience creation.
        """

        keywords: MutableSequence[criteria.KeywordInfo] = proto.RepeatedField(
            proto.MESSAGE,
            number=1,
            message=criteria.KeywordInfo,
        )

    class LeadFormAssetRecommendation(proto.Message):
        r"""The lead form asset recommendation."""

    class ImproveDemandGenAdStrengthRecommendation(proto.Message):
        r"""The improve Demand Gen ad strength recommendation.

        Attributes:
            ad (str):
                Output only. The resource name of the ad that
                can be improved.
            ad_strength (google.ads.googleads.v20.enums.types.AdStrengthEnum.AdStrength):
                Output only. The current ad strength.
            demand_gen_asset_action_items (MutableSequence[str]):
                Output only. A list of recommendations to
                improve the ad strength.
        """

        ad: str = proto.Field(
            proto.STRING,
            number=1,
        )
        ad_strength: gage_ad_strength.AdStrengthEnum.AdStrength = proto.Field(
            proto.ENUM,
            number=2,
            enum=gage_ad_strength.AdStrengthEnum.AdStrength,
        )
        demand_gen_asset_action_items: MutableSequence[str] = (
            proto.RepeatedField(
                proto.STRING,
                number=3,
            )
        )

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    type_: recommendation_type.RecommendationTypeEnum.RecommendationType = (
        proto.Field(
            proto.ENUM,
            number=2,
            enum=recommendation_type.RecommendationTypeEnum.RecommendationType,
        )
    )
    impact: RecommendationImpact = proto.Field(
        proto.MESSAGE,
        number=3,
        message=RecommendationImpact,
    )
    campaign_budget: str = proto.Field(
        proto.STRING,
        number=24,
        optional=True,
    )
    campaign: str = proto.Field(
        proto.STRING,
        number=25,
        optional=True,
    )
    ad_group: str = proto.Field(
        proto.STRING,
        number=26,
        optional=True,
    )
    dismissed: bool = proto.Field(
        proto.BOOL,
        number=27,
        optional=True,
    )
    campaigns: MutableSequence[str] = proto.RepeatedField(
        proto.STRING,
        number=38,
    )
    campaign_budget_recommendation: CampaignBudgetRecommendation = proto.Field(
        proto.MESSAGE,
        number=4,
        oneof="recommendation",
        message=CampaignBudgetRecommendation,
    )
    forecasting_campaign_budget_recommendation: CampaignBudgetRecommendation = (
        proto.Field(
            proto.MESSAGE,
            number=22,
            oneof="recommendation",
            message=CampaignBudgetRecommendation,
        )
    )
    keyword_recommendation: KeywordRecommendation = proto.Field(
        proto.MESSAGE,
        number=8,
        oneof="recommendation",
        message=KeywordRecommendation,
    )
    text_ad_recommendation: TextAdRecommendation = proto.Field(
        proto.MESSAGE,
        number=9,
        oneof="recommendation",
        message=TextAdRecommendation,
    )
    target_cpa_opt_in_recommendation: TargetCpaOptInRecommendation = (
        proto.Field(
            proto.MESSAGE,
            number=10,
            oneof="recommendation",
            message=TargetCpaOptInRecommendation,
        )
    )
    maximize_conversions_opt_in_recommendation: (
        MaximizeConversionsOptInRecommendation
    ) = proto.Field(
        proto.MESSAGE,
        number=11,
        oneof="recommendation",
        message=MaximizeConversionsOptInRecommendation,
    )
    enhanced_cpc_opt_in_recommendation: EnhancedCpcOptInRecommendation = (
        proto.Field(
            proto.MESSAGE,
            number=12,
            oneof="recommendation",
            message=EnhancedCpcOptInRecommendation,
        )
    )
    search_partners_opt_in_recommendation: SearchPartnersOptInRecommendation = (
        proto.Field(
            proto.MESSAGE,
            number=14,
            oneof="recommendation",
            message=SearchPartnersOptInRecommendation,
        )
    )
    maximize_clicks_opt_in_recommendation: MaximizeClicksOptInRecommendation = (
        proto.Field(
            proto.MESSAGE,
            number=15,
            oneof="recommendation",
            message=MaximizeClicksOptInRecommendation,
        )
    )
    optimize_ad_rotation_recommendation: OptimizeAdRotationRecommendation = (
        proto.Field(
            proto.MESSAGE,
            number=16,
            oneof="recommendation",
            message=OptimizeAdRotationRecommendation,
        )
    )
    keyword_match_type_recommendation: KeywordMatchTypeRecommendation = (
        proto.Field(
            proto.MESSAGE,
            number=20,
            oneof="recommendation",
            message=KeywordMatchTypeRecommendation,
        )
    )
    move_unused_budget_recommendation: MoveUnusedBudgetRecommendation = (
        proto.Field(
            proto.MESSAGE,
            number=21,
            oneof="recommendation",
            message=MoveUnusedBudgetRecommendation,
        )
    )
    target_roas_opt_in_recommendation: TargetRoasOptInRecommendation = (
        proto.Field(
            proto.MESSAGE,
            number=23,
            oneof="recommendation",
            message=TargetRoasOptInRecommendation,
        )
    )
    responsive_search_ad_recommendation: ResponsiveSearchAdRecommendation = (
        proto.Field(
            proto.MESSAGE,
            number=28,
            oneof="recommendation",
            message=ResponsiveSearchAdRecommendation,
        )
    )
    marginal_roi_campaign_budget_recommendation: (
        CampaignBudgetRecommendation
    ) = proto.Field(
        proto.MESSAGE,
        number=29,
        oneof="recommendation",
        message=CampaignBudgetRecommendation,
    )
    use_broad_match_keyword_recommendation: (
        UseBroadMatchKeywordRecommendation
    ) = proto.Field(
        proto.MESSAGE,
        number=30,
        oneof="recommendation",
        message=UseBroadMatchKeywordRecommendation,
    )
    responsive_search_ad_asset_recommendation: (
        ResponsiveSearchAdAssetRecommendation
    ) = proto.Field(
        proto.MESSAGE,
        number=31,
        oneof="recommendation",
        message=ResponsiveSearchAdAssetRecommendation,
    )
    upgrade_smart_shopping_campaign_to_performance_max_recommendation: (
        UpgradeSmartShoppingCampaignToPerformanceMaxRecommendation
    ) = proto.Field(
        proto.MESSAGE,
        number=32,
        oneof="recommendation",
        message=UpgradeSmartShoppingCampaignToPerformanceMaxRecommendation,
    )
    responsive_search_ad_improve_ad_strength_recommendation: (
        ResponsiveSearchAdImproveAdStrengthRecommendation
    ) = proto.Field(
        proto.MESSAGE,
        number=33,
        oneof="recommendation",
        message=ResponsiveSearchAdImproveAdStrengthRecommendation,
    )
    display_expansion_opt_in_recommendation: (
        DisplayExpansionOptInRecommendation
    ) = proto.Field(
        proto.MESSAGE,
        number=34,
        oneof="recommendation",
        message=DisplayExpansionOptInRecommendation,
    )
    upgrade_local_campaign_to_performance_max_recommendation: (
        UpgradeLocalCampaignToPerformanceMaxRecommendation
    ) = proto.Field(
        proto.MESSAGE,
        number=35,
        oneof="recommendation",
        message=UpgradeLocalCampaignToPerformanceMaxRecommendation,
    )
    raise_target_cpa_bid_too_low_recommendation: (
        RaiseTargetCpaBidTooLowRecommendation
    ) = proto.Field(
        proto.MESSAGE,
        number=36,
        oneof="recommendation",
        message=RaiseTargetCpaBidTooLowRecommendation,
    )
    forecasting_set_target_roas_recommendation: (
        ForecastingSetTargetRoasRecommendation
    ) = proto.Field(
        proto.MESSAGE,
        number=37,
        oneof="recommendation",
        message=ForecastingSetTargetRoasRecommendation,
    )
    callout_asset_recommendation: CalloutAssetRecommendation = proto.Field(
        proto.MESSAGE,
        number=39,
        oneof="recommendation",
        message=CalloutAssetRecommendation,
    )
    sitelink_asset_recommendation: SitelinkAssetRecommendation = proto.Field(
        proto.MESSAGE,
        number=40,
        oneof="recommendation",
        message=SitelinkAssetRecommendation,
    )
    call_asset_recommendation: CallAssetRecommendation = proto.Field(
        proto.MESSAGE,
        number=41,
        oneof="recommendation",
        message=CallAssetRecommendation,
    )
    shopping_add_age_group_recommendation: (
        ShoppingOfferAttributeRecommendation
    ) = proto.Field(
        proto.MESSAGE,
        number=42,
        oneof="recommendation",
        message=ShoppingOfferAttributeRecommendation,
    )
    shopping_add_color_recommendation: ShoppingOfferAttributeRecommendation = (
        proto.Field(
            proto.MESSAGE,
            number=43,
            oneof="recommendation",
            message=ShoppingOfferAttributeRecommendation,
        )
    )
    shopping_add_gender_recommendation: ShoppingOfferAttributeRecommendation = (
        proto.Field(
            proto.MESSAGE,
            number=44,
            oneof="recommendation",
            message=ShoppingOfferAttributeRecommendation,
        )
    )
    shopping_add_gtin_recommendation: ShoppingOfferAttributeRecommendation = (
        proto.Field(
            proto.MESSAGE,
            number=45,
            oneof="recommendation",
            message=ShoppingOfferAttributeRecommendation,
        )
    )
    shopping_add_more_identifiers_recommendation: (
        ShoppingOfferAttributeRecommendation
    ) = proto.Field(
        proto.MESSAGE,
        number=46,
        oneof="recommendation",
        message=ShoppingOfferAttributeRecommendation,
    )
    shopping_add_size_recommendation: ShoppingOfferAttributeRecommendation = (
        proto.Field(
            proto.MESSAGE,
            number=47,
            oneof="recommendation",
            message=ShoppingOfferAttributeRecommendation,
        )
    )
    shopping_add_products_to_campaign_recommendation: (
        ShoppingAddProductsToCampaignRecommendation
    ) = proto.Field(
        proto.MESSAGE,
        number=48,
        oneof="recommendation",
        message=ShoppingAddProductsToCampaignRecommendation,
    )
    shopping_fix_disapproved_products_recommendation: (
        ShoppingFixDisapprovedProductsRecommendation
    ) = proto.Field(
        proto.MESSAGE,
        number=49,
        oneof="recommendation",
        message=ShoppingFixDisapprovedProductsRecommendation,
    )
    shopping_target_all_offers_recommendation: (
        ShoppingTargetAllOffersRecommendation
    ) = proto.Field(
        proto.MESSAGE,
        number=50,
        oneof="recommendation",
        message=ShoppingTargetAllOffersRecommendation,
    )
    shopping_fix_suspended_merchant_center_account_recommendation: (
        ShoppingMerchantCenterAccountSuspensionRecommendation
    ) = proto.Field(
        proto.MESSAGE,
        number=51,
        oneof="recommendation",
        message=ShoppingMerchantCenterAccountSuspensionRecommendation,
    )
    shopping_fix_merchant_center_account_suspension_warning_recommendation: (
        ShoppingMerchantCenterAccountSuspensionRecommendation
    ) = proto.Field(
        proto.MESSAGE,
        number=52,
        oneof="recommendation",
        message=ShoppingMerchantCenterAccountSuspensionRecommendation,
    )
    shopping_migrate_regular_shopping_campaign_offers_to_performance_max_recommendation: ShoppingMigrateRegularShoppingCampaignOffersToPerformanceMaxRecommendation = proto.Field(
        proto.MESSAGE,
        number=53,
        oneof="recommendation",
        message=ShoppingMigrateRegularShoppingCampaignOffersToPerformanceMaxRecommendation,
    )
    dynamic_image_extension_opt_in_recommendation: (
        DynamicImageExtensionOptInRecommendation
    ) = proto.Field(
        proto.MESSAGE,
        number=54,
        oneof="recommendation",
        message=DynamicImageExtensionOptInRecommendation,
    )
    raise_target_cpa_recommendation: RaiseTargetCpaRecommendation = proto.Field(
        proto.MESSAGE,
        number=55,
        oneof="recommendation",
        message=RaiseTargetCpaRecommendation,
    )
    lower_target_roas_recommendation: LowerTargetRoasRecommendation = (
        proto.Field(
            proto.MESSAGE,
            number=56,
            oneof="recommendation",
            message=LowerTargetRoasRecommendation,
        )
    )
    performance_max_opt_in_recommendation: PerformanceMaxOptInRecommendation = (
        proto.Field(
            proto.MESSAGE,
            number=57,
            oneof="recommendation",
            message=PerformanceMaxOptInRecommendation,
        )
    )
    improve_performance_max_ad_strength_recommendation: (
        ImprovePerformanceMaxAdStrengthRecommendation
    ) = proto.Field(
        proto.MESSAGE,
        number=58,
        oneof="recommendation",
        message=ImprovePerformanceMaxAdStrengthRecommendation,
    )
    migrate_dynamic_search_ads_campaign_to_performance_max_recommendation: (
        MigrateDynamicSearchAdsCampaignToPerformanceMaxRecommendation
    ) = proto.Field(
        proto.MESSAGE,
        number=59,
        oneof="recommendation",
        message=MigrateDynamicSearchAdsCampaignToPerformanceMaxRecommendation,
    )
    forecasting_set_target_cpa_recommendation: (
        ForecastingSetTargetCpaRecommendation
    ) = proto.Field(
        proto.MESSAGE,
        number=60,
        oneof="recommendation",
        message=ForecastingSetTargetCpaRecommendation,
    )
    set_target_cpa_recommendation: ForecastingSetTargetCpaRecommendation = (
        proto.Field(
            proto.MESSAGE,
            number=61,
            oneof="recommendation",
            message=ForecastingSetTargetCpaRecommendation,
        )
    )
    set_target_roas_recommendation: ForecastingSetTargetRoasRecommendation = (
        proto.Field(
            proto.MESSAGE,
            number=62,
            oneof="recommendation",
            message=ForecastingSetTargetRoasRecommendation,
        )
    )
    maximize_conversion_value_opt_in_recommendation: (
        MaximizeConversionValueOptInRecommendation
    ) = proto.Field(
        proto.MESSAGE,
        number=63,
        oneof="recommendation",
        message=MaximizeConversionValueOptInRecommendation,
    )
    improve_google_tag_coverage_recommendation: (
        ImproveGoogleTagCoverageRecommendation
    ) = proto.Field(
        proto.MESSAGE,
        number=64,
        oneof="recommendation",
        message=ImproveGoogleTagCoverageRecommendation,
    )
    performance_max_final_url_opt_in_recommendation: (
        PerformanceMaxFinalUrlOptInRecommendation
    ) = proto.Field(
        proto.MESSAGE,
        number=65,
        oneof="recommendation",
        message=PerformanceMaxFinalUrlOptInRecommendation,
    )
    refresh_customer_match_list_recommendation: (
        RefreshCustomerMatchListRecommendation
    ) = proto.Field(
        proto.MESSAGE,
        number=66,
        oneof="recommendation",
        message=RefreshCustomerMatchListRecommendation,
    )
    custom_audience_opt_in_recommendation: CustomAudienceOptInRecommendation = (
        proto.Field(
            proto.MESSAGE,
            number=67,
            oneof="recommendation",
            message=CustomAudienceOptInRecommendation,
        )
    )
    lead_form_asset_recommendation: LeadFormAssetRecommendation = proto.Field(
        proto.MESSAGE,
        number=68,
        oneof="recommendation",
        message=LeadFormAssetRecommendation,
    )
    improve_demand_gen_ad_strength_recommendation: (
        ImproveDemandGenAdStrengthRecommendation
    ) = proto.Field(
        proto.MESSAGE,
        number=69,
        oneof="recommendation",
        message=ImproveDemandGenAdStrengthRecommendation,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
