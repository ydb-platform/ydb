# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

from facebook_business.adobjects.abstractobject import AbstractObject
from facebook_business.adobjects.helpers.adsinsightsmixin import AdsInsightsMixin

"""
This class is auto-generated.

For any issues or feature requests related to this class, please let us know on
github and we'll fix in our codegen framework. We'll not be able to accept
pull request for this class.
"""

class AdsInsights(
    AdsInsightsMixin,
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdsInsights, self).__init__()
        self._isAdsInsights = True
        self._api = api

    class Field(AbstractObject.Field):
        account_currency = 'account_currency'
        account_id = 'account_id'
        account_name = 'account_name'
        action_values = 'action_values'
        actions = 'actions'
        ad_click_actions = 'ad_click_actions'
        ad_id = 'ad_id'
        ad_impression_actions = 'ad_impression_actions'
        ad_name = 'ad_name'
        adset_end = 'adset_end'
        adset_id = 'adset_id'
        adset_name = 'adset_name'
        adset_start = 'adset_start'
        age_targeting = 'age_targeting'
        anchor_events_performance_indicator = 'anchor_events_performance_indicator'
        attribution_setting = 'attribution_setting'
        auction_bid = 'auction_bid'
        auction_competitiveness = 'auction_competitiveness'
        auction_max_competitor_bid = 'auction_max_competitor_bid'
        average_purchases_conversion_value = 'average_purchases_conversion_value'
        buying_type = 'buying_type'
        campaign_id = 'campaign_id'
        campaign_name = 'campaign_name'
        canvas_avg_view_percent = 'canvas_avg_view_percent'
        canvas_avg_view_time = 'canvas_avg_view_time'
        catalog_segment_actions = 'catalog_segment_actions'
        catalog_segment_value = 'catalog_segment_value'
        catalog_segment_value_mobile_purchase_roas = 'catalog_segment_value_mobile_purchase_roas'
        catalog_segment_value_omni_purchase_roas = 'catalog_segment_value_omni_purchase_roas'
        catalog_segment_value_website_purchase_roas = 'catalog_segment_value_website_purchase_roas'
        clicks = 'clicks'
        conversion_lead_rate = 'conversion_lead_rate'
        conversion_leads = 'conversion_leads'
        conversion_rate_ranking = 'conversion_rate_ranking'
        conversion_values = 'conversion_values'
        conversions = 'conversions'
        converted_product_app_custom_event_fb_mobile_purchase = 'converted_product_app_custom_event_fb_mobile_purchase'
        converted_product_app_custom_event_fb_mobile_purchase_value = 'converted_product_app_custom_event_fb_mobile_purchase_value'
        converted_product_offline_purchase = 'converted_product_offline_purchase'
        converted_product_offline_purchase_value = 'converted_product_offline_purchase_value'
        converted_product_omni_purchase = 'converted_product_omni_purchase'
        converted_product_omni_purchase_values = 'converted_product_omni_purchase_values'
        converted_product_quantity = 'converted_product_quantity'
        converted_product_value = 'converted_product_value'
        converted_product_website_pixel_purchase = 'converted_product_website_pixel_purchase'
        converted_product_website_pixel_purchase_value = 'converted_product_website_pixel_purchase_value'
        converted_promoted_product_app_custom_event_fb_mobile_purchase = 'converted_promoted_product_app_custom_event_fb_mobile_purchase'
        converted_promoted_product_app_custom_event_fb_mobile_purchase_value = 'converted_promoted_product_app_custom_event_fb_mobile_purchase_value'
        converted_promoted_product_offline_purchase = 'converted_promoted_product_offline_purchase'
        converted_promoted_product_offline_purchase_value = 'converted_promoted_product_offline_purchase_value'
        converted_promoted_product_omni_purchase = 'converted_promoted_product_omni_purchase'
        converted_promoted_product_omni_purchase_values = 'converted_promoted_product_omni_purchase_values'
        converted_promoted_product_quantity = 'converted_promoted_product_quantity'
        converted_promoted_product_value = 'converted_promoted_product_value'
        converted_promoted_product_website_pixel_purchase = 'converted_promoted_product_website_pixel_purchase'
        converted_promoted_product_website_pixel_purchase_value = 'converted_promoted_product_website_pixel_purchase_value'
        cost_per_15_sec_video_view = 'cost_per_15_sec_video_view'
        cost_per_2_sec_continuous_video_view = 'cost_per_2_sec_continuous_video_view'
        cost_per_action_type = 'cost_per_action_type'
        cost_per_ad_click = 'cost_per_ad_click'
        cost_per_conversion = 'cost_per_conversion'
        cost_per_conversion_lead = 'cost_per_conversion_lead'
        cost_per_dda_countby_convs = 'cost_per_dda_countby_convs'
        cost_per_estimated_ad_recallers = 'cost_per_estimated_ad_recallers'
        cost_per_inline_link_click = 'cost_per_inline_link_click'
        cost_per_inline_post_engagement = 'cost_per_inline_post_engagement'
        cost_per_objective_result = 'cost_per_objective_result'
        cost_per_one_thousand_ad_impression = 'cost_per_one_thousand_ad_impression'
        cost_per_outbound_click = 'cost_per_outbound_click'
        cost_per_result = 'cost_per_result'
        cost_per_thruplay = 'cost_per_thruplay'
        cost_per_unique_action_type = 'cost_per_unique_action_type'
        cost_per_unique_click = 'cost_per_unique_click'
        cost_per_unique_conversion = 'cost_per_unique_conversion'
        cost_per_unique_inline_link_click = 'cost_per_unique_inline_link_click'
        cost_per_unique_outbound_click = 'cost_per_unique_outbound_click'
        cpc = 'cpc'
        cpm = 'cpm'
        cpp = 'cpp'
        created_time = 'created_time'
        creative_media_type = 'creative_media_type'
        ctr = 'ctr'
        date_start = 'date_start'
        date_stop = 'date_stop'
        dda_countby_convs = 'dda_countby_convs'
        dda_results = 'dda_results'
        engagement_rate_ranking = 'engagement_rate_ranking'
        estimated_ad_recall_rate = 'estimated_ad_recall_rate'
        estimated_ad_recall_rate_lower_bound = 'estimated_ad_recall_rate_lower_bound'
        estimated_ad_recall_rate_upper_bound = 'estimated_ad_recall_rate_upper_bound'
        estimated_ad_recallers = 'estimated_ad_recallers'
        estimated_ad_recallers_lower_bound = 'estimated_ad_recallers_lower_bound'
        estimated_ad_recallers_upper_bound = 'estimated_ad_recallers_upper_bound'
        frequency = 'frequency'
        full_view_impressions = 'full_view_impressions'
        full_view_reach = 'full_view_reach'
        gender_targeting = 'gender_targeting'
        impressions = 'impressions'
        inline_link_click_ctr = 'inline_link_click_ctr'
        inline_link_clicks = 'inline_link_clicks'
        inline_post_engagement = 'inline_post_engagement'
        instagram_upcoming_event_reminders_set = 'instagram_upcoming_event_reminders_set'
        instant_experience_clicks_to_open = 'instant_experience_clicks_to_open'
        instant_experience_clicks_to_start = 'instant_experience_clicks_to_start'
        instant_experience_outbound_clicks = 'instant_experience_outbound_clicks'
        interactive_component_tap = 'interactive_component_tap'
        labels = 'labels'
        landing_page_view_actions_per_link_click = 'landing_page_view_actions_per_link_click'
        landing_page_view_per_link_click = 'landing_page_view_per_link_click'
        landing_page_view_per_purchase_rate = 'landing_page_view_per_purchase_rate'
        link_clicks_per_results = 'link_clicks_per_results'
        location = 'location'
        marketing_messages_click_rate_benchmark = 'marketing_messages_click_rate_benchmark'
        marketing_messages_cost_per_delivered = 'marketing_messages_cost_per_delivered'
        marketing_messages_cost_per_link_btn_click = 'marketing_messages_cost_per_link_btn_click'
        marketing_messages_delivered = 'marketing_messages_delivered'
        marketing_messages_delivery_rate = 'marketing_messages_delivery_rate'
        marketing_messages_link_btn_click = 'marketing_messages_link_btn_click'
        marketing_messages_link_btn_click_rate = 'marketing_messages_link_btn_click_rate'
        marketing_messages_media_view_rate = 'marketing_messages_media_view_rate'
        marketing_messages_phone_call_btn_click_rate = 'marketing_messages_phone_call_btn_click_rate'
        marketing_messages_quick_reply_btn_click = 'marketing_messages_quick_reply_btn_click'
        marketing_messages_quick_reply_btn_click_rate = 'marketing_messages_quick_reply_btn_click_rate'
        marketing_messages_read = 'marketing_messages_read'
        marketing_messages_read_rate = 'marketing_messages_read_rate'
        marketing_messages_read_rate_benchmark = 'marketing_messages_read_rate_benchmark'
        marketing_messages_sent = 'marketing_messages_sent'
        marketing_messages_spend = 'marketing_messages_spend'
        marketing_messages_spend_currency = 'marketing_messages_spend_currency'
        marketing_messages_website_add_to_cart = 'marketing_messages_website_add_to_cart'
        marketing_messages_website_initiate_checkout = 'marketing_messages_website_initiate_checkout'
        marketing_messages_website_purchase = 'marketing_messages_website_purchase'
        marketing_messages_website_purchase_values = 'marketing_messages_website_purchase_values'
        mobile_app_purchase_roas = 'mobile_app_purchase_roas'
        objective = 'objective'
        objective_result_rate = 'objective_result_rate'
        objective_results = 'objective_results'
        onsite_conversion_messaging_detected_purchase_deduped = 'onsite_conversion_messaging_detected_purchase_deduped'
        optimization_goal = 'optimization_goal'
        outbound_clicks = 'outbound_clicks'
        outbound_clicks_ctr = 'outbound_clicks_ctr'
        place_page_name = 'place_page_name'
        product_group_retailer_id = 'product_group_retailer_id'
        product_retailer_id = 'product_retailer_id'
        product_views = 'product_views'
        purchase_per_landing_page_view = 'purchase_per_landing_page_view'
        purchase_roas = 'purchase_roas'
        purchases_per_link_click = 'purchases_per_link_click'
        qualifying_question_qualify_answer_rate = 'qualifying_question_qualify_answer_rate'
        quality_ranking = 'quality_ranking'
        reach = 'reach'
        result_rate = 'result_rate'
        result_values_performance_indicator = 'result_values_performance_indicator'
        results = 'results'
        shops_assisted_purchases = 'shops_assisted_purchases'
        social_spend = 'social_spend'
        spend = 'spend'
        total_card_view = 'total_card_view'
        total_postbacks = 'total_postbacks'
        total_postbacks_detailed = 'total_postbacks_detailed'
        total_postbacks_detailed_v4 = 'total_postbacks_detailed_v4'
        unique_actions = 'unique_actions'
        unique_clicks = 'unique_clicks'
        unique_conversions = 'unique_conversions'
        unique_ctr = 'unique_ctr'
        unique_inline_link_click_ctr = 'unique_inline_link_click_ctr'
        unique_inline_link_clicks = 'unique_inline_link_clicks'
        unique_link_clicks_ctr = 'unique_link_clicks_ctr'
        unique_outbound_clicks = 'unique_outbound_clicks'
        unique_outbound_clicks_ctr = 'unique_outbound_clicks_ctr'
        unique_video_continuous_2_sec_watched_actions = 'unique_video_continuous_2_sec_watched_actions'
        unique_video_view_15_sec = 'unique_video_view_15_sec'
        updated_time = 'updated_time'
        video_15_sec_watched_actions = 'video_15_sec_watched_actions'
        video_30_sec_watched_actions = 'video_30_sec_watched_actions'
        video_avg_time_watched_actions = 'video_avg_time_watched_actions'
        video_continuous_2_sec_watched_actions = 'video_continuous_2_sec_watched_actions'
        video_p100_watched_actions = 'video_p100_watched_actions'
        video_p25_watched_actions = 'video_p25_watched_actions'
        video_p50_watched_actions = 'video_p50_watched_actions'
        video_p75_watched_actions = 'video_p75_watched_actions'
        video_p95_watched_actions = 'video_p95_watched_actions'
        video_play_actions = 'video_play_actions'
        video_play_curve_actions = 'video_play_curve_actions'
        video_play_retention_0_to_15s_actions = 'video_play_retention_0_to_15s_actions'
        video_play_retention_20_to_60s_actions = 'video_play_retention_20_to_60s_actions'
        video_play_retention_graph_actions = 'video_play_retention_graph_actions'
        video_thruplay_watched_actions = 'video_thruplay_watched_actions'
        video_time_watched_actions = 'video_time_watched_actions'
        video_view_per_impression = 'video_view_per_impression'
        website_ctr = 'website_ctr'
        website_purchase_roas = 'website_purchase_roas'
        wish_bid = 'wish_bid'

    class ActionAttributionWindows:
        value_1d_click = '1d_click'
        value_1d_ev = '1d_ev'
        value_1d_view = '1d_view'
        value_28d_click = '28d_click'
        value_28d_view = '28d_view'
        value_28d_view_all_conversions = '28d_view_all_conversions'
        value_28d_view_first_conversion = '28d_view_first_conversion'
        value_7d_click = '7d_click'
        value_7d_view = '7d_view'
        value_7d_view_all_conversions = '7d_view_all_conversions'
        value_7d_view_first_conversion = '7d_view_first_conversion'
        dda = 'dda'
        value_default = 'default'
        skan_click = 'skan_click'
        skan_click_second_postback = 'skan_click_second_postback'
        skan_click_third_postback = 'skan_click_third_postback'
        skan_view = 'skan_view'
        skan_view_second_postback = 'skan_view_second_postback'
        skan_view_third_postback = 'skan_view_third_postback'

    class ActionBreakdowns:
        action_canvas_component_name = 'action_canvas_component_name'
        action_carousel_card_id = 'action_carousel_card_id'
        action_carousel_card_name = 'action_carousel_card_name'
        action_destination = 'action_destination'
        action_device = 'action_device'
        action_reaction = 'action_reaction'
        action_target_id = 'action_target_id'
        action_type = 'action_type'
        action_video_sound = 'action_video_sound'
        action_video_type = 'action_video_type'
        conversion_destination = 'conversion_destination'
        matched_persona_id = 'matched_persona_id'
        matched_persona_name = 'matched_persona_name'
        signal_source_bucket = 'signal_source_bucket'
        standard_event_content_type = 'standard_event_content_type'

    class ActionReportTime:
        conversion = 'conversion'
        impression = 'impression'
        lifetime = 'lifetime'
        mixed = 'mixed'

    class Breakdowns:
        ad_extension_domain = 'ad_extension_domain'
        ad_extension_url = 'ad_extension_url'
        ad_format_asset = 'ad_format_asset'
        age = 'age'
        app_id = 'app_id'
        body_asset = 'body_asset'
        breakdown_ad_objective = 'breakdown_ad_objective'
        breakdown_reporting_ad_id = 'breakdown_reporting_ad_id'
        call_to_action_asset = 'call_to_action_asset'
        coarse_conversion_value = 'coarse_conversion_value'
        comscore_market = 'comscore_market'
        conversion_destination = 'conversion_destination'
        country = 'country'
        creative_automation_asset_id = 'creative_automation_asset_id'
        creative_relaxation_asset_type = 'creative_relaxation_asset_type'
        crm_advertiser_l12_territory_ids = 'crm_advertiser_l12_territory_ids'
        crm_advertiser_subvertical_id = 'crm_advertiser_subvertical_id'
        crm_advertiser_vertical_id = 'crm_advertiser_vertical_id'
        crm_ult_advertiser_id = 'crm_ult_advertiser_id'
        description_asset = 'description_asset'
        device_platform = 'device_platform'
        dma = 'dma'
        fidelity_type = 'fidelity_type'
        flexible_format_asset_type = 'flexible_format_asset_type'
        frequency_value = 'frequency_value'
        gen_ai_asset_type = 'gen_ai_asset_type'
        gender = 'gender'
        hourly_stats_aggregated_by_advertiser_time_zone = 'hourly_stats_aggregated_by_advertiser_time_zone'
        hourly_stats_aggregated_by_audience_time_zone = 'hourly_stats_aggregated_by_audience_time_zone'
        hsid = 'hsid'
        image_asset = 'image_asset'
        impression_device = 'impression_device'
        impression_view_time_advertiser_hour_v2 = 'impression_view_time_advertiser_hour_v2'
        is_auto_advance = 'is_auto_advance'
        is_conversion_id_modeled = 'is_conversion_id_modeled'
        is_rendered_as_delayed_skip_ad = 'is_rendered_as_delayed_skip_ad'
        landing_destination = 'landing_destination'
        link_url_asset = 'link_url_asset'
        marketing_messages_btn_name = 'marketing_messages_btn_name'
        mdsa_landing_destination = 'mdsa_landing_destination'
        media_asset_url = 'media_asset_url'
        media_creator = 'media_creator'
        media_destination_url = 'media_destination_url'
        media_format = 'media_format'
        media_origin_url = 'media_origin_url'
        media_text_content = 'media_text_content'
        media_type = 'media_type'
        mmm = 'mmm'
        place_page_id = 'place_page_id'
        platform_position = 'platform_position'
        postback_sequence_index = 'postback_sequence_index'
        product_brand_breakdown = 'product_brand_breakdown'
        product_category_breakdown = 'product_category_breakdown'
        product_custom_label_0_breakdown = 'product_custom_label_0_breakdown'
        product_custom_label_1_breakdown = 'product_custom_label_1_breakdown'
        product_custom_label_2_breakdown = 'product_custom_label_2_breakdown'
        product_custom_label_3_breakdown = 'product_custom_label_3_breakdown'
        product_custom_label_4_breakdown = 'product_custom_label_4_breakdown'
        product_group_content_id_breakdown = 'product_group_content_id_breakdown'
        product_group_id = 'product_group_id'
        product_id = 'product_id'
        product_set_id_breakdown = 'product_set_id_breakdown'
        publisher_platform = 'publisher_platform'
        redownload = 'redownload'
        region = 'region'
        rta_ugc_topic = 'rta_ugc_topic'
        rule_set_id = 'rule_set_id'
        rule_set_name = 'rule_set_name'
        signal_source_bucket = 'signal_source_bucket'
        skan_campaign_id = 'skan_campaign_id'
        skan_conversion_id = 'skan_conversion_id'
        skan_version = 'skan_version'
        sot_attribution_model_type = 'sot_attribution_model_type'
        sot_attribution_window = 'sot_attribution_window'
        sot_channel = 'sot_channel'
        sot_event_type = 'sot_event_type'
        sot_source = 'sot_source'
        standard_event_content_type = 'standard_event_content_type'
        title_asset = 'title_asset'
        user_persona_id = 'user_persona_id'
        user_persona_name = 'user_persona_name'
        video_asset = 'video_asset'

    class DatePreset:
        data_maximum = 'data_maximum'
        last_14d = 'last_14d'
        last_28d = 'last_28d'
        last_30d = 'last_30d'
        last_3d = 'last_3d'
        last_7d = 'last_7d'
        last_90d = 'last_90d'
        last_month = 'last_month'
        last_quarter = 'last_quarter'
        last_week_mon_sun = 'last_week_mon_sun'
        last_week_sun_sat = 'last_week_sun_sat'
        last_year = 'last_year'
        maximum = 'maximum'
        this_month = 'this_month'
        this_quarter = 'this_quarter'
        this_week_mon_today = 'this_week_mon_today'
        this_week_sun_today = 'this_week_sun_today'
        this_year = 'this_year'
        today = 'today'
        yesterday = 'yesterday'

    class Level:
        account = 'account'
        ad = 'ad'
        adset = 'adset'
        campaign = 'campaign'

    class SummaryActionBreakdowns:
        action_canvas_component_name = 'action_canvas_component_name'
        action_carousel_card_id = 'action_carousel_card_id'
        action_carousel_card_name = 'action_carousel_card_name'
        action_destination = 'action_destination'
        action_device = 'action_device'
        action_reaction = 'action_reaction'
        action_target_id = 'action_target_id'
        action_type = 'action_type'
        action_video_sound = 'action_video_sound'
        action_video_type = 'action_video_type'
        conversion_destination = 'conversion_destination'
        matched_persona_id = 'matched_persona_id'
        matched_persona_name = 'matched_persona_name'
        signal_source_bucket = 'signal_source_bucket'
        standard_event_content_type = 'standard_event_content_type'

    # @deprecated get_endpoint function is deprecated
    @classmethod
    def get_endpoint(cls):
        return 'insights'

    _field_types = {
        'account_currency': 'string',
        'account_id': 'string',
        'account_name': 'string',
        'action_values': 'list<AdsActionStats>',
        'actions': 'list<AdsActionStats>',
        'ad_click_actions': 'list<AdsActionStats>',
        'ad_id': 'string',
        'ad_impression_actions': 'list<AdsActionStats>',
        'ad_name': 'string',
        'adset_end': 'string',
        'adset_id': 'string',
        'adset_name': 'string',
        'adset_start': 'string',
        'age_targeting': 'string',
        'anchor_events_performance_indicator': 'string',
        'attribution_setting': 'string',
        'auction_bid': 'string',
        'auction_competitiveness': 'string',
        'auction_max_competitor_bid': 'string',
        'average_purchases_conversion_value': 'list<AdsActionStats>',
        'buying_type': 'string',
        'campaign_id': 'string',
        'campaign_name': 'string',
        'canvas_avg_view_percent': 'string',
        'canvas_avg_view_time': 'string',
        'catalog_segment_actions': 'list<AdsActionStats>',
        'catalog_segment_value': 'list<AdsActionStats>',
        'catalog_segment_value_mobile_purchase_roas': 'list<AdsActionStats>',
        'catalog_segment_value_omni_purchase_roas': 'list<AdsActionStats>',
        'catalog_segment_value_website_purchase_roas': 'list<AdsActionStats>',
        'clicks': 'string',
        'conversion_lead_rate': 'list<AdsActionStats>',
        'conversion_leads': 'list<AdsActionStats>',
        'conversion_rate_ranking': 'string',
        'conversion_values': 'list<AdsActionStats>',
        'conversions': 'list<AdsActionStats>',
        'converted_product_app_custom_event_fb_mobile_purchase': 'list<AdsActionStats>',
        'converted_product_app_custom_event_fb_mobile_purchase_value': 'list<AdsActionStats>',
        'converted_product_offline_purchase': 'list<AdsActionStats>',
        'converted_product_offline_purchase_value': 'list<AdsActionStats>',
        'converted_product_omni_purchase': 'list<AdsActionStats>',
        'converted_product_omni_purchase_values': 'list<AdsActionStats>',
        'converted_product_quantity': 'list<AdsActionStats>',
        'converted_product_value': 'list<AdsActionStats>',
        'converted_product_website_pixel_purchase': 'list<AdsActionStats>',
        'converted_product_website_pixel_purchase_value': 'list<AdsActionStats>',
        'converted_promoted_product_app_custom_event_fb_mobile_purchase': 'list<AdsActionStats>',
        'converted_promoted_product_app_custom_event_fb_mobile_purchase_value': 'list<AdsActionStats>',
        'converted_promoted_product_offline_purchase': 'list<AdsActionStats>',
        'converted_promoted_product_offline_purchase_value': 'list<AdsActionStats>',
        'converted_promoted_product_omni_purchase': 'list<AdsActionStats>',
        'converted_promoted_product_omni_purchase_values': 'list<AdsActionStats>',
        'converted_promoted_product_quantity': 'list<AdsActionStats>',
        'converted_promoted_product_value': 'list<AdsActionStats>',
        'converted_promoted_product_website_pixel_purchase': 'list<AdsActionStats>',
        'converted_promoted_product_website_pixel_purchase_value': 'list<AdsActionStats>',
        'cost_per_15_sec_video_view': 'list<AdsActionStats>',
        'cost_per_2_sec_continuous_video_view': 'list<AdsActionStats>',
        'cost_per_action_type': 'list<AdsActionStats>',
        'cost_per_ad_click': 'list<AdsActionStats>',
        'cost_per_conversion': 'list<AdsActionStats>',
        'cost_per_conversion_lead': 'list<AdsActionStats>',
        'cost_per_dda_countby_convs': 'string',
        'cost_per_estimated_ad_recallers': 'string',
        'cost_per_inline_link_click': 'string',
        'cost_per_inline_post_engagement': 'string',
        'cost_per_objective_result': 'list<Object>',
        'cost_per_one_thousand_ad_impression': 'list<AdsActionStats>',
        'cost_per_outbound_click': 'list<AdsActionStats>',
        'cost_per_result': 'list<Object>',
        'cost_per_thruplay': 'list<AdsActionStats>',
        'cost_per_unique_action_type': 'list<AdsActionStats>',
        'cost_per_unique_click': 'string',
        'cost_per_unique_conversion': 'list<AdsActionStats>',
        'cost_per_unique_inline_link_click': 'string',
        'cost_per_unique_outbound_click': 'list<AdsActionStats>',
        'cpc': 'string',
        'cpm': 'string',
        'cpp': 'string',
        'created_time': 'string',
        'creative_media_type': 'string',
        'ctr': 'string',
        'date_start': 'string',
        'date_stop': 'string',
        'dda_countby_convs': 'string',
        'dda_results': 'list<Object>',
        'engagement_rate_ranking': 'string',
        'estimated_ad_recall_rate': 'string',
        'estimated_ad_recall_rate_lower_bound': 'string',
        'estimated_ad_recall_rate_upper_bound': 'string',
        'estimated_ad_recallers': 'string',
        'estimated_ad_recallers_lower_bound': 'string',
        'estimated_ad_recallers_upper_bound': 'string',
        'frequency': 'string',
        'full_view_impressions': 'string',
        'full_view_reach': 'string',
        'gender_targeting': 'string',
        'impressions': 'string',
        'inline_link_click_ctr': 'string',
        'inline_link_clicks': 'string',
        'inline_post_engagement': 'string',
        'instagram_upcoming_event_reminders_set': 'string',
        'instant_experience_clicks_to_open': 'string',
        'instant_experience_clicks_to_start': 'string',
        'instant_experience_outbound_clicks': 'list<AdsActionStats>',
        'interactive_component_tap': 'list<AdsActionStats>',
        'labels': 'string',
        'landing_page_view_actions_per_link_click': 'string',
        'landing_page_view_per_link_click': 'string',
        'landing_page_view_per_purchase_rate': 'string',
        'link_clicks_per_results': 'list<Object>',
        'location': 'string',
        'marketing_messages_click_rate_benchmark': 'string',
        'marketing_messages_cost_per_delivered': 'string',
        'marketing_messages_cost_per_link_btn_click': 'string',
        'marketing_messages_delivered': 'string',
        'marketing_messages_delivery_rate': 'string',
        'marketing_messages_link_btn_click': 'string',
        'marketing_messages_link_btn_click_rate': 'string',
        'marketing_messages_media_view_rate': 'string',
        'marketing_messages_phone_call_btn_click_rate': 'string',
        'marketing_messages_quick_reply_btn_click': 'string',
        'marketing_messages_quick_reply_btn_click_rate': 'string',
        'marketing_messages_read': 'string',
        'marketing_messages_read_rate': 'string',
        'marketing_messages_read_rate_benchmark': 'string',
        'marketing_messages_sent': 'string',
        'marketing_messages_spend': 'string',
        'marketing_messages_spend_currency': 'string',
        'marketing_messages_website_add_to_cart': 'string',
        'marketing_messages_website_initiate_checkout': 'string',
        'marketing_messages_website_purchase': 'string',
        'marketing_messages_website_purchase_values': 'string',
        'mobile_app_purchase_roas': 'list<AdsActionStats>',
        'objective': 'string',
        'objective_result_rate': 'list<Object>',
        'objective_results': 'list<Object>',
        'onsite_conversion_messaging_detected_purchase_deduped': 'list<AdsActionStats>',
        'optimization_goal': 'string',
        'outbound_clicks': 'list<AdsActionStats>',
        'outbound_clicks_ctr': 'list<AdsActionStats>',
        'place_page_name': 'string',
        'product_group_retailer_id': 'string',
        'product_retailer_id': 'string',
        'product_views': 'string',
        'purchase_per_landing_page_view': 'string',
        'purchase_roas': 'list<AdsActionStats>',
        'purchases_per_link_click': 'string',
        'qualifying_question_qualify_answer_rate': 'string',
        'quality_ranking': 'string',
        'reach': 'string',
        'result_rate': 'list<Object>',
        'result_values_performance_indicator': 'string',
        'results': 'list<Object>',
        'shops_assisted_purchases': 'string',
        'social_spend': 'string',
        'spend': 'string',
        'total_card_view': 'string',
        'total_postbacks': 'string',
        'total_postbacks_detailed': 'list<AdsActionStats>',
        'total_postbacks_detailed_v4': 'list<AdsActionStats>',
        'unique_actions': 'list<AdsActionStats>',
        'unique_clicks': 'string',
        'unique_conversions': 'list<AdsActionStats>',
        'unique_ctr': 'string',
        'unique_inline_link_click_ctr': 'string',
        'unique_inline_link_clicks': 'string',
        'unique_link_clicks_ctr': 'string',
        'unique_outbound_clicks': 'list<AdsActionStats>',
        'unique_outbound_clicks_ctr': 'list<AdsActionStats>',
        'unique_video_continuous_2_sec_watched_actions': 'list<AdsActionStats>',
        'unique_video_view_15_sec': 'list<AdsActionStats>',
        'updated_time': 'string',
        'video_15_sec_watched_actions': 'list<AdsActionStats>',
        'video_30_sec_watched_actions': 'list<AdsActionStats>',
        'video_avg_time_watched_actions': 'list<AdsActionStats>',
        'video_continuous_2_sec_watched_actions': 'list<AdsActionStats>',
        'video_p100_watched_actions': 'list<AdsActionStats>',
        'video_p25_watched_actions': 'list<AdsActionStats>',
        'video_p50_watched_actions': 'list<AdsActionStats>',
        'video_p75_watched_actions': 'list<AdsActionStats>',
        'video_p95_watched_actions': 'list<AdsActionStats>',
        'video_play_actions': 'list<AdsActionStats>',
        'video_play_curve_actions': 'list<AdsHistogramStats>',
        'video_play_retention_0_to_15s_actions': 'list<AdsHistogramStats>',
        'video_play_retention_20_to_60s_actions': 'list<AdsHistogramStats>',
        'video_play_retention_graph_actions': 'list<AdsHistogramStats>',
        'video_thruplay_watched_actions': 'list<AdsActionStats>',
        'video_time_watched_actions': 'list<AdsActionStats>',
        'video_view_per_impression': 'list<AdsActionStats>',
        'website_ctr': 'list<AdsActionStats>',
        'website_purchase_roas': 'list<AdsActionStats>',
        'wish_bid': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['ActionAttributionWindows'] = AdsInsights.ActionAttributionWindows.__dict__.values()
        field_enum_info['ActionBreakdowns'] = AdsInsights.ActionBreakdowns.__dict__.values()
        field_enum_info['ActionReportTime'] = AdsInsights.ActionReportTime.__dict__.values()
        field_enum_info['Breakdowns'] = AdsInsights.Breakdowns.__dict__.values()
        field_enum_info['DatePreset'] = AdsInsights.DatePreset.__dict__.values()
        field_enum_info['Level'] = AdsInsights.Level.__dict__.values()
        field_enum_info['SummaryActionBreakdowns'] = AdsInsights.SummaryActionBreakdowns.__dict__.values()
        return field_enum_info


