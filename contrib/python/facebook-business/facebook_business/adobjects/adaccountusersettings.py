# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

from facebook_business.adobjects.abstractobject import AbstractObject
from facebook_business.adobjects.abstractcrudobject import AbstractCrudObject
from facebook_business.adobjects.objectparser import ObjectParser
from facebook_business.api import FacebookRequest
from facebook_business.typechecker import TypeChecker

"""
This class is auto-generated.

For any issues or feature requests related to this class, please let us know on
github and we'll fix in our codegen framework. We'll not be able to accept
pull request for this class.
"""

class AdAccountUserSettings(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isAdAccountUserSettings = True
        super(AdAccountUserSettings, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        acf_should_opt_out_video_adjustments = 'acf_should_opt_out_video_adjustments'
        aco_sticky_settings = 'aco_sticky_settings'
        actions_quick_view_created = 'actions_quick_view_created'
        active_ads_quick_view_created = 'active_ads_quick_view_created'
        ad_account = 'ad_account'
        ad_object_export_format = 'ad_object_export_format'
        ads_manager_footer_row_toast_impressions = 'ads_manager_footer_row_toast_impressions'
        auto_review_video_caption = 'auto_review_video_caption'
        budget_optimization_quick_view_created = 'budget_optimization_quick_view_created'
        campaign_overview_columns = 'campaign_overview_columns'
        column_suggestion_status = 'column_suggestion_status'
        conditional_formatting_rules = 'conditional_formatting_rules'
        default_account_overview_agegender_metrics = 'default_account_overview_agegender_metrics'
        default_account_overview_location_metrics = 'default_account_overview_location_metrics'
        default_account_overview_metrics = 'default_account_overview_metrics'
        default_account_overview_time_metrics = 'default_account_overview_time_metrics'
        default_builtin_column_preset = 'default_builtin_column_preset'
        default_nam_time_range = 'default_nam_time_range'
        draft_mode_enabled = 'draft_mode_enabled'
        export_deleted_items_with_delivery = 'export_deleted_items_with_delivery'
        export_summary_row = 'export_summary_row'
        had_delivery_quick_view_created = 'had_delivery_quick_view_created'
        has_seen_groups_column_flexing_experience = 'has_seen_groups_column_flexing_experience'
        has_seen_instagram_column_flexing_experience = 'has_seen_instagram_column_flexing_experience'
        has_seen_leads_column_flexing_experience = 'has_seen_leads_column_flexing_experience'
        has_seen_shops_ads_metrics_onboarding_tour = 'has_seen_shops_ads_metrics_onboarding_tour'
        has_seen_shops_column_flexing_experience = 'has_seen_shops_column_flexing_experience'
        has_used_quick_views_panel = 'has_used_quick_views_panel'
        hidden_optimization_tips = 'hidden_optimization_tips'
        high_performing_quick_view_created = 'high_performing_quick_view_created'
        id = 'id'
        is_3p_auth_setting_set = 'is_3p_auth_setting_set'
        is_ads_manager_footer_row_preference_set = 'is_ads_manager_footer_row_preference_set'
        is_ads_manager_footer_row_shown = 'is_ads_manager_footer_row_shown'
        is_text_variation_nux_close = 'is_text_variation_nux_close'
        last_used_columns = 'last_used_columns'
        last_used_pe_filters = 'last_used_pe_filters'
        last_used_website_urls = 'last_used_website_urls'
        outlier_preferences = 'outlier_preferences'
        pinned_ad_object_ids = 'pinned_ad_object_ids'
        rb_export_format = 'rb_export_format'
        rb_export_raw_data = 'rb_export_raw_data'
        rb_export_summary_row = 'rb_export_summary_row'
        recently_used_quick_views = 'recently_used_quick_views'
        saip_advertiser_setup_optimisation_guidance_overall_state = 'saip_advertiser_setup_optimisation_guidance_overall_state'
        saip_advertiser_setup_optimisation_guidance_state = 'saip_advertiser_setup_optimisation_guidance_state'
        shops_ads_metrics_onboarding_tour_close_count = 'shops_ads_metrics_onboarding_tour_close_count'
        shops_ads_metrics_onboarding_tour_last_action_time = 'shops_ads_metrics_onboarding_tour_last_action_time'
        should_default_image_auto_crop = 'should_default_image_auto_crop'
        should_default_image_auto_crop_for_tail = 'should_default_image_auto_crop_for_tail'
        should_default_image_auto_crop_optimization = 'should_default_image_auto_crop_optimization'
        should_default_image_dof_toggle = 'should_default_image_dof_toggle'
        should_default_image_lpp_ads_to_square = 'should_default_image_lpp_ads_to_square'
        should_default_instagram_profile_card_optimization = 'should_default_instagram_profile_card_optimization'
        should_default_text_swapping_optimization = 'should_default_text_swapping_optimization'
        should_logout_of_3p_sourcing = 'should_logout_of_3p_sourcing'
        should_show_shops_ads_metrics_onboarding_tour = 'should_show_shops_ads_metrics_onboarding_tour'
        show_archived_data = 'show_archived_data'
        show_text_variation_nux_tooltip = 'show_text_variation_nux_tooltip'
        syd_campaign_trends_activemetric = 'syd_campaign_trends_activemetric'
        syd_campaign_trends_attribution = 'syd_campaign_trends_attribution'
        syd_campaign_trends_metrics = 'syd_campaign_trends_metrics'
        syd_campaign_trends_objective = 'syd_campaign_trends_objective'
        syd_campaign_trends_time_range = 'syd_campaign_trends_time_range'
        syd_landing_page_opt_in_status = 'syd_landing_page_opt_in_status'
        text_gen_persona_opt_in_type = 'text_gen_persona_opt_in_type'
        text_variations_hl_opt_in_out_ts = 'text_variations_hl_opt_in_out_ts'
        text_variations_hl_opt_in_type = 'text_variations_hl_opt_in_type'
        text_variations_opt_in_out_ts = 'text_variations_opt_in_out_ts'
        text_variations_opt_in_type = 'text_variations_opt_in_type'
        user = 'user'
        value_optimized_qv_created = 'value_optimized_qv_created'
        value_qv_nux_impressions = 'value_qv_nux_impressions'
        value_suggested_column_status = 'value_suggested_column_status'

    class SydCampaignTrendsObjective:
        app_installs = 'APP_INSTALLS'
        brand_awareness = 'BRAND_AWARENESS'
        event_responses = 'EVENT_RESPONSES'
        lead_generation = 'LEAD_GENERATION'
        link_clicks = 'LINK_CLICKS'
        local_awareness = 'LOCAL_AWARENESS'
        messages = 'MESSAGES'
        offer_claims = 'OFFER_CLAIMS'
        outcome_app_promotion = 'OUTCOME_APP_PROMOTION'
        outcome_awareness = 'OUTCOME_AWARENESS'
        outcome_engagement = 'OUTCOME_ENGAGEMENT'
        outcome_leads = 'OUTCOME_LEADS'
        outcome_sales = 'OUTCOME_SALES'
        outcome_traffic = 'OUTCOME_TRAFFIC'
        page_likes = 'PAGE_LIKES'
        post_engagement = 'POST_ENGAGEMENT'
        product_catalog_sales = 'PRODUCT_CATALOG_SALES'
        reach = 'REACH'
        store_visits = 'STORE_VISITS'
        video_views = 'VIDEO_VIEWS'
        website_conversions = 'WEBSITE_CONVERSIONS'

    def api_get(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdAccountUserSettings,
            api_type='NODE',
            response_parser=ObjectParser(reuse_object=self),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    _field_types = {
        'acf_should_opt_out_video_adjustments': 'bool',
        'aco_sticky_settings': 'list<map<string, string>>',
        'actions_quick_view_created': 'bool',
        'active_ads_quick_view_created': 'bool',
        'ad_account': 'AdAccount',
        'ad_object_export_format': 'string',
        'ads_manager_footer_row_toast_impressions': 'int',
        'auto_review_video_caption': 'bool',
        'budget_optimization_quick_view_created': 'bool',
        'campaign_overview_columns': 'list<string>',
        'column_suggestion_status': 'string',
        'conditional_formatting_rules': 'list<string>',
        'default_account_overview_agegender_metrics': 'list<string>',
        'default_account_overview_location_metrics': 'list<string>',
        'default_account_overview_metrics': 'list<string>',
        'default_account_overview_time_metrics': 'list<string>',
        'default_builtin_column_preset': 'string',
        'default_nam_time_range': 'string',
        'draft_mode_enabled': 'bool',
        'export_deleted_items_with_delivery': 'bool',
        'export_summary_row': 'bool',
        'had_delivery_quick_view_created': 'bool',
        'has_seen_groups_column_flexing_experience': 'bool',
        'has_seen_instagram_column_flexing_experience': 'bool',
        'has_seen_leads_column_flexing_experience': 'bool',
        'has_seen_shops_ads_metrics_onboarding_tour': 'bool',
        'has_seen_shops_column_flexing_experience': 'bool',
        'has_used_quick_views_panel': 'bool',
        'hidden_optimization_tips': 'list<map<string, bool>>',
        'high_performing_quick_view_created': 'bool',
        'id': 'string',
        'is_3p_auth_setting_set': 'bool',
        'is_ads_manager_footer_row_preference_set': 'bool',
        'is_ads_manager_footer_row_shown': 'bool',
        'is_text_variation_nux_close': 'bool',
        'last_used_columns': 'Object',
        'last_used_pe_filters': 'list<Object>',
        'last_used_website_urls': 'list<string>',
        'outlier_preferences': 'Object',
        'pinned_ad_object_ids': 'list<string>',
        'rb_export_format': 'string',
        'rb_export_raw_data': 'bool',
        'rb_export_summary_row': 'bool',
        'recently_used_quick_views': 'list<string>',
        'saip_advertiser_setup_optimisation_guidance_overall_state': 'string',
        'saip_advertiser_setup_optimisation_guidance_state': 'list<map<string, string>>',
        'shops_ads_metrics_onboarding_tour_close_count': 'int',
        'shops_ads_metrics_onboarding_tour_last_action_time': 'datetime',
        'should_default_image_auto_crop': 'bool',
        'should_default_image_auto_crop_for_tail': 'bool',
        'should_default_image_auto_crop_optimization': 'bool',
        'should_default_image_dof_toggle': 'bool',
        'should_default_image_lpp_ads_to_square': 'bool',
        'should_default_instagram_profile_card_optimization': 'bool',
        'should_default_text_swapping_optimization': 'bool',
        'should_logout_of_3p_sourcing': 'bool',
        'should_show_shops_ads_metrics_onboarding_tour': 'bool',
        'show_archived_data': 'bool',
        'show_text_variation_nux_tooltip': 'bool',
        'syd_campaign_trends_activemetric': 'string',
        'syd_campaign_trends_attribution': 'string',
        'syd_campaign_trends_metrics': 'list<string>',
        'syd_campaign_trends_objective': 'SydCampaignTrendsObjective',
        'syd_campaign_trends_time_range': 'string',
        'syd_landing_page_opt_in_status': 'string',
        'text_gen_persona_opt_in_type': 'string',
        'text_variations_hl_opt_in_out_ts': 'datetime',
        'text_variations_hl_opt_in_type': 'string',
        'text_variations_opt_in_out_ts': 'datetime',
        'text_variations_opt_in_type': 'string',
        'user': 'User',
        'value_optimized_qv_created': 'bool',
        'value_qv_nux_impressions': 'int',
        'value_suggested_column_status': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['SydCampaignTrendsObjective'] = AdAccountUserSettings.SydCampaignTrendsObjective.__dict__.values()
        return field_enum_info


