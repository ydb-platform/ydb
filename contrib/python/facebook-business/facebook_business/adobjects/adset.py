# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

from facebook_business.adobjects.abstractobject import AbstractObject
from facebook_business.adobjects.abstractcrudobject import AbstractCrudObject
from facebook_business.adobjects.objectparser import ObjectParser
from facebook_business.api import FacebookRequest
from facebook_business.typechecker import TypeChecker
from facebook_business.mixins import HasAdLabels
from facebook_business.mixins import CanValidate

"""
This class is auto-generated.

For any issues or feature requests related to this class, please let us know on
github and we'll fix in our codegen framework. We'll not be able to accept
pull request for this class.
"""

class AdSet(
    AbstractCrudObject,
    HasAdLabels,
    CanValidate,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isAdSet = True
        super(AdSet, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        account_id = 'account_id'
        adlabels = 'adlabels'
        adset_schedule = 'adset_schedule'
        anchor_event_attribution_window_days = 'anchor_event_attribution_window_days'
        asset_feed_id = 'asset_feed_id'
        attribution_spec = 'attribution_spec'
        automatic_manual_state = 'automatic_manual_state'
        bid_adjustments = 'bid_adjustments'
        bid_amount = 'bid_amount'
        bid_constraints = 'bid_constraints'
        bid_info = 'bid_info'
        bid_strategy = 'bid_strategy'
        billing_event = 'billing_event'
        brand_safety_config = 'brand_safety_config'
        budget_remaining = 'budget_remaining'
        campaign = 'campaign'
        campaign_active_time = 'campaign_active_time'
        campaign_attribution = 'campaign_attribution'
        campaign_id = 'campaign_id'
        configured_status = 'configured_status'
        created_time = 'created_time'
        creative_sequence = 'creative_sequence'
        creative_sequence_repetition_pattern = 'creative_sequence_repetition_pattern'
        daily_budget = 'daily_budget'
        daily_min_spend_target = 'daily_min_spend_target'
        daily_spend_cap = 'daily_spend_cap'
        destination_type = 'destination_type'
        dsa_beneficiary = 'dsa_beneficiary'
        dsa_payor = 'dsa_payor'
        effective_status = 'effective_status'
        end_time = 'end_time'
        existing_customer_budget_percentage = 'existing_customer_budget_percentage'
        frequency_control_specs = 'frequency_control_specs'
        full_funnel_exploration_mode = 'full_funnel_exploration_mode'
        id = 'id'
        instagram_user_id = 'instagram_user_id'
        is_ba_skip_delayed_eligible = 'is_ba_skip_delayed_eligible'
        is_budget_schedule_enabled = 'is_budget_schedule_enabled'
        is_dynamic_creative = 'is_dynamic_creative'
        is_incremental_attribution_enabled = 'is_incremental_attribution_enabled'
        issues_info = 'issues_info'
        learning_stage_info = 'learning_stage_info'
        lifetime_budget = 'lifetime_budget'
        lifetime_imps = 'lifetime_imps'
        lifetime_min_spend_target = 'lifetime_min_spend_target'
        lifetime_spend_cap = 'lifetime_spend_cap'
        max_budget_spend_percentage = 'max_budget_spend_percentage'
        min_budget_spend_percentage = 'min_budget_spend_percentage'
        multi_optimization_goal_weight = 'multi_optimization_goal_weight'
        name = 'name'
        optimization_goal = 'optimization_goal'
        optimization_sub_event = 'optimization_sub_event'
        pacing_type = 'pacing_type'
        placement_soft_opt_out = 'placement_soft_opt_out'
        promoted_object = 'promoted_object'
        recommendations = 'recommendations'
        recurring_budget_semantics = 'recurring_budget_semantics'
        regional_regulated_categories = 'regional_regulated_categories'
        regional_regulation_identities = 'regional_regulation_identities'
        review_feedback = 'review_feedback'
        rf_prediction_id = 'rf_prediction_id'
        source_adset = 'source_adset'
        source_adset_id = 'source_adset_id'
        start_time = 'start_time'
        status = 'status'
        targeting = 'targeting'
        targeting_optimization_types = 'targeting_optimization_types'
        time_based_ad_rotation_id_blocks = 'time_based_ad_rotation_id_blocks'
        time_based_ad_rotation_intervals = 'time_based_ad_rotation_intervals'
        trending_topics_spec = 'trending_topics_spec'
        updated_time = 'updated_time'
        use_new_app_click = 'use_new_app_click'
        value_rule_set_id = 'value_rule_set_id'
        value_rules_applied = 'value_rules_applied'
        budget_schedule_specs = 'budget_schedule_specs'
        budget_source = 'budget_source'
        budget_split_set_id = 'budget_split_set_id'
        campaign_spec = 'campaign_spec'
        daily_imps = 'daily_imps'
        date_format = 'date_format'
        execution_options = 'execution_options'
        is_sac_cfca_terms_certified = 'is_sac_cfca_terms_certified'
        line_number = 'line_number'
        rb_prediction_id = 'rb_prediction_id'
        time_start = 'time_start'
        time_stop = 'time_stop'
        topline_id = 'topline_id'
        tune_for_category = 'tune_for_category'

    class BidStrategy:
        cost_cap = 'COST_CAP'
        lowest_cost_without_cap = 'LOWEST_COST_WITHOUT_CAP'
        lowest_cost_with_bid_cap = 'LOWEST_COST_WITH_BID_CAP'
        lowest_cost_with_min_roas = 'LOWEST_COST_WITH_MIN_ROAS'

    class BillingEvent:
        app_installs = 'APP_INSTALLS'
        clicks = 'CLICKS'
        impressions = 'IMPRESSIONS'
        link_clicks = 'LINK_CLICKS'
        listing_interaction = 'LISTING_INTERACTION'
        none = 'NONE'
        offer_claims = 'OFFER_CLAIMS'
        page_likes = 'PAGE_LIKES'
        post_engagement = 'POST_ENGAGEMENT'
        purchase = 'PURCHASE'
        thruplay = 'THRUPLAY'

    class ConfiguredStatus:
        active = 'ACTIVE'
        archived = 'ARCHIVED'
        deleted = 'DELETED'
        paused = 'PAUSED'

    class EffectiveStatus:
        active = 'ACTIVE'
        archived = 'ARCHIVED'
        campaign_paused = 'CAMPAIGN_PAUSED'
        deleted = 'DELETED'
        in_process = 'IN_PROCESS'
        paused = 'PAUSED'
        with_issues = 'WITH_ISSUES'

    class OptimizationGoal:
        advertiser_siloed_value = 'ADVERTISER_SILOED_VALUE'
        ad_recall_lift = 'AD_RECALL_LIFT'
        app_installs = 'APP_INSTALLS'
        app_installs_and_offsite_conversions = 'APP_INSTALLS_AND_OFFSITE_CONVERSIONS'
        automatic_objective = 'AUTOMATIC_OBJECTIVE'
        conversations = 'CONVERSATIONS'
        derived_events = 'DERIVED_EVENTS'
        engaged_users = 'ENGAGED_USERS'
        event_responses = 'EVENT_RESPONSES'
        impressions = 'IMPRESSIONS'
        in_app_value = 'IN_APP_VALUE'
        landing_page_views = 'LANDING_PAGE_VIEWS'
        lead_generation = 'LEAD_GENERATION'
        link_clicks = 'LINK_CLICKS'
        meaningful_call_attempt = 'MEANINGFUL_CALL_ATTEMPT'
        messaging_appointment_conversion = 'MESSAGING_APPOINTMENT_CONVERSION'
        messaging_purchase_conversion = 'MESSAGING_PURCHASE_CONVERSION'
        none = 'NONE'
        offsite_conversions = 'OFFSITE_CONVERSIONS'
        page_likes = 'PAGE_LIKES'
        post_engagement = 'POST_ENGAGEMENT'
        profile_and_page_engagement = 'PROFILE_AND_PAGE_ENGAGEMENT'
        profile_visit = 'PROFILE_VISIT'
        quality_call = 'QUALITY_CALL'
        quality_lead = 'QUALITY_LEAD'
        reach = 'REACH'
        reminders_set = 'REMINDERS_SET'
        subscribers = 'SUBSCRIBERS'
        thruplay = 'THRUPLAY'
        value = 'VALUE'
        visit_instagram_profile = 'VISIT_INSTAGRAM_PROFILE'

    class Status:
        active = 'ACTIVE'
        archived = 'ARCHIVED'
        deleted = 'DELETED'
        paused = 'PAUSED'

    class AutomaticManualState:
        automatic = 'AUTOMATIC'
        manual = 'MANUAL'
        unset = 'UNSET'

    class BudgetSource:
        none = 'NONE'
        rmn = 'RMN'

    class CreativeSequenceRepetitionPattern:
        full_sequence = 'FULL_SEQUENCE'
        last_ad = 'LAST_AD'

    class DatePreset:
        data_maximum = 'DATA_MAXIMUM'
        last_14d = 'LAST_14D'
        last_28d = 'LAST_28D'
        last_30d = 'LAST_30D'
        last_3d = 'LAST_3D'
        last_7d = 'LAST_7D'
        last_90d = 'LAST_90D'
        last_month = 'LAST_MONTH'
        last_quarter = 'LAST_QUARTER'
        last_week_mon_sun = 'LAST_WEEK_MON_SUN'
        last_week_sun_sat = 'LAST_WEEK_SUN_SAT'
        last_year = 'LAST_YEAR'
        maximum = 'MAXIMUM'
        this_month = 'THIS_MONTH'
        this_quarter = 'THIS_QUARTER'
        this_week_mon_today = 'THIS_WEEK_MON_TODAY'
        this_week_sun_today = 'THIS_WEEK_SUN_TODAY'
        this_year = 'THIS_YEAR'
        today = 'TODAY'
        yesterday = 'YESTERDAY'

    class DestinationType:
        app = 'APP'
        applinks_automatic = 'APPLINKS_AUTOMATIC'
        facebook = 'FACEBOOK'
        facebook_live = 'FACEBOOK_LIVE'
        facebook_page = 'FACEBOOK_PAGE'
        imagine = 'IMAGINE'
        instagram_direct = 'INSTAGRAM_DIRECT'
        instagram_live = 'INSTAGRAM_LIVE'
        instagram_profile = 'INSTAGRAM_PROFILE'
        instagram_profile_and_facebook_page = 'INSTAGRAM_PROFILE_AND_FACEBOOK_PAGE'
        messaging_instagram_direct_messenger = 'MESSAGING_INSTAGRAM_DIRECT_MESSENGER'
        messaging_instagram_direct_messenger_whatsapp = 'MESSAGING_INSTAGRAM_DIRECT_MESSENGER_WHATSAPP'
        messaging_instagram_direct_whatsapp = 'MESSAGING_INSTAGRAM_DIRECT_WHATSAPP'
        messaging_messenger_whatsapp = 'MESSAGING_MESSENGER_WHATSAPP'
        messenger = 'MESSENGER'
        on_ad = 'ON_AD'
        on_event = 'ON_EVENT'
        on_page = 'ON_PAGE'
        on_post = 'ON_POST'
        on_video = 'ON_VIDEO'
        shop_automatic = 'SHOP_AUTOMATIC'
        website = 'WEBSITE'
        whatsapp = 'WHATSAPP'

    class ExecutionOptions:
        include_recommendations = 'include_recommendations'
        validate_only = 'validate_only'

    class FullFunnelExplorationMode:
        extended_exploration = 'EXTENDED_EXPLORATION'
        limited_exploration = 'LIMITED_EXPLORATION'
        none_exploration = 'NONE_EXPLORATION'

    class MultiOptimizationGoalWeight:
        balanced = 'BALANCED'
        prefer_event = 'PREFER_EVENT'
        prefer_install = 'PREFER_INSTALL'
        undefined = 'UNDEFINED'

    class OptimizationSubEvent:
        none = 'NONE'
        post_interaction = 'POST_INTERACTION'
        travel_intent = 'TRAVEL_INTENT'
        travel_intent_bucket_01 = 'TRAVEL_INTENT_BUCKET_01'
        travel_intent_bucket_02 = 'TRAVEL_INTENT_BUCKET_02'
        travel_intent_bucket_03 = 'TRAVEL_INTENT_BUCKET_03'
        travel_intent_bucket_04 = 'TRAVEL_INTENT_BUCKET_04'
        travel_intent_bucket_05 = 'TRAVEL_INTENT_BUCKET_05'
        travel_intent_no_destination_intent = 'TRAVEL_INTENT_NO_DESTINATION_INTENT'
        trip_consideration = 'TRIP_CONSIDERATION'
        video_sound_on = 'VIDEO_SOUND_ON'

    class RegionalRegulatedCategories:
        value_0 = '0'
        value_1 = '1'
        value_2 = '2'
        value_3 = '3'
        value_4 = '4'
        value_5 = '5'
        value_6 = '6'
        value_7 = '7'
        value_8 = '8'
        value_9 = '9'
        value_10 = '10'
        value_11 = '11'
        value_12 = '12'

    class TuneForCategory:
        credit = 'CREDIT'
        employment = 'EMPLOYMENT'
        financial_products_services = 'FINANCIAL_PRODUCTS_SERVICES'
        housing = 'HOUSING'
        issues_elections_politics = 'ISSUES_ELECTIONS_POLITICS'
        none = 'NONE'
        online_gambling_and_gaming = 'ONLINE_GAMBLING_AND_GAMING'

    class Operator:
        all = 'ALL'
        any = 'ANY'

    class StatusOption:
        active = 'ACTIVE'
        inherited_from_source = 'INHERITED_FROM_SOURCE'
        paused = 'PAUSED'

    # @deprecated get_endpoint function is deprecated
    @classmethod
    def get_endpoint(cls):
        return 'adsets'

    # @deprecated api_create is being deprecated
    def api_create(self, parent_id, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.adobjects.adaccount import AdAccount
        return AdAccount(api=self._api, fbid=parent_id).create_ad_set(fields, params, batch, success, failure, pending)

    def api_delete(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='DELETE',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AbstractCrudObject,
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

    def api_get(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'am_call_tags': 'map',
            'date_preset': 'date_preset_enum',
            'from_adtable': 'bool',
            'time_range': 'map',
        }
        enums = {
            'date_preset_enum': [
                'data_maximum',
                'last_14d',
                'last_28d',
                'last_30d',
                'last_3d',
                'last_7d',
                'last_90d',
                'last_month',
                'last_quarter',
                'last_week_mon_sun',
                'last_week_sun_sat',
                'last_year',
                'maximum',
                'this_month',
                'this_quarter',
                'this_week_mon_today',
                'this_week_sun_today',
                'this_year',
                'today',
                'yesterday',
            ],
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdSet,
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

    def api_update(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'account_id': 'string',
            'adlabels': 'list<Object>',
            'adset_schedule': 'list<Object>',
            'attribution_spec': 'list<map>',
            'automatic_manual_state': 'automatic_manual_state_enum',
            'bid_adjustments': 'Object',
            'bid_amount': 'int',
            'bid_constraints': 'map<string, Object>',
            'bid_strategy': 'bid_strategy_enum',
            'billing_event': 'billing_event_enum',
            'budget_schedule_specs': 'list<Object>',
            'campaign_attribution': 'Object',
            'campaign_spec': 'Object',
            'creative_sequence': 'list<string>',
            'creative_sequence_repetition_pattern': 'creative_sequence_repetition_pattern_enum',
            'daily_budget': 'unsigned int',
            'daily_imps': 'unsigned int',
            'daily_min_spend_target': 'unsigned int',
            'daily_spend_cap': 'unsigned int',
            'date_format': 'string',
            'destination_type': 'destination_type_enum',
            'dsa_beneficiary': 'string',
            'dsa_payor': 'string',
            'end_time': 'datetime',
            'execution_options': 'list<execution_options_enum>',
            'existing_customer_budget_percentage': 'unsigned int',
            'full_funnel_exploration_mode': 'full_funnel_exploration_mode_enum',
            'is_ba_skip_delayed_eligible': 'bool',
            'is_budget_schedule_enabled': 'bool',
            'is_incremental_attribution_enabled': 'bool',
            'is_sac_cfca_terms_certified': 'bool',
            'lifetime_budget': 'unsigned int',
            'lifetime_imps': 'unsigned int',
            'lifetime_min_spend_target': 'unsigned int',
            'lifetime_spend_cap': 'unsigned int',
            'max_budget_spend_percentage': 'unsigned int',
            'min_budget_spend_percentage': 'unsigned int',
            'multi_optimization_goal_weight': 'multi_optimization_goal_weight_enum',
            'name': 'string',
            'optimization_goal': 'optimization_goal_enum',
            'optimization_sub_event': 'optimization_sub_event_enum',
            'pacing_type': 'list<string>',
            'placement_soft_opt_out': 'Object',
            'promoted_object': 'Object',
            'rb_prediction_id': 'string',
            'regional_regulated_categories': 'list<regional_regulated_categories_enum>',
            'regional_regulation_identities': 'map',
            'rf_prediction_id': 'string',
            'start_time': 'datetime',
            'status': 'status_enum',
            'targeting': 'Targeting',
            'time_based_ad_rotation_id_blocks': 'list<list<unsigned int>>',
            'time_based_ad_rotation_intervals': 'list<unsigned int>',
            'time_start': 'datetime',
            'time_stop': 'datetime',
            'trending_topics_spec': 'map',
            'tune_for_category': 'tune_for_category_enum',
            'value_rule_set_id': 'string',
            'value_rules_applied': 'bool',
        }
        enums = {
            'automatic_manual_state_enum': AdSet.AutomaticManualState.__dict__.values(),
            'bid_strategy_enum': AdSet.BidStrategy.__dict__.values(),
            'billing_event_enum': AdSet.BillingEvent.__dict__.values(),
            'creative_sequence_repetition_pattern_enum': AdSet.CreativeSequenceRepetitionPattern.__dict__.values(),
            'destination_type_enum': AdSet.DestinationType.__dict__.values(),
            'execution_options_enum': AdSet.ExecutionOptions.__dict__.values(),
            'full_funnel_exploration_mode_enum': AdSet.FullFunnelExplorationMode.__dict__.values(),
            'multi_optimization_goal_weight_enum': AdSet.MultiOptimizationGoalWeight.__dict__.values(),
            'optimization_goal_enum': AdSet.OptimizationGoal.__dict__.values(),
            'optimization_sub_event_enum': AdSet.OptimizationSubEvent.__dict__.values(),
            'regional_regulated_categories_enum': AdSet.RegionalRegulatedCategories.__dict__.values(),
            'status_enum': AdSet.Status.__dict__.values(),
            'tune_for_category_enum': AdSet.TuneForCategory.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdSet,
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

    def get_activities(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adactivity import AdActivity
        param_types = {
            'after': 'string',
            'business_id': 'string',
            'category': 'category_enum',
            'limit': 'int',
            'since': 'datetime',
            'uid': 'int',
            'until': 'datetime',
        }
        enums = {
            'category_enum': AdActivity.Category.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/activities',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdActivity,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdActivity, api=self._api),
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

    def get_ad_studies(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adstudy import AdStudy
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/ad_studies',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdStudy,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdStudy, api=self._api),
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

    def get_ad_creatives(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adcreative import AdCreative
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/adcreatives',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdCreative,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdCreative, api=self._api),
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

    def delete_ad_labels(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'adlabels': 'list<Object>',
            'execution_options': 'list<execution_options_enum>',
        }
        enums = {
            'execution_options_enum': AdSet.ExecutionOptions.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='DELETE',
            endpoint='/adlabels',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AbstractCrudObject,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AbstractCrudObject, api=self._api),
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

    def create_ad_label(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'adlabels': 'list<Object>',
            'execution_options': 'list<execution_options_enum>',
        }
        enums = {
            'execution_options_enum': AdSet.ExecutionOptions.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/adlabels',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdSet,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdSet, api=self._api),
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

    def get_ad_rules_governed(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adrule import AdRule
        param_types = {
            'pass_evaluation': 'bool',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/adrules_governed',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdRule,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdRule, api=self._api),
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

    def get_ads(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.ad import Ad
        param_types = {
            'date_preset': 'date_preset_enum',
            'effective_status': 'list<string>',
            'time_range': 'map',
            'updated_since': 'int',
        }
        enums = {
            'date_preset_enum': Ad.DatePreset.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/ads',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Ad,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Ad, api=self._api),
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

    def get_async_ad_requests(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adasyncrequest import AdAsyncRequest
        param_types = {
            'statuses': 'list<statuses_enum>',
        }
        enums = {
            'statuses_enum': AdAsyncRequest.Statuses.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/asyncadrequests',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdAsyncRequest,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdAsyncRequest, api=self._api),
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

    def get_budget_schedules(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.highdemandperiod import HighDemandPeriod
        param_types = {
            'time_start': 'datetime',
            'time_stop': 'datetime',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/budget_schedules',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=HighDemandPeriod,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=HighDemandPeriod, api=self._api),
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

    def create_budget_schedule(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.highdemandperiod import HighDemandPeriod
        param_types = {
            'budget_value': 'unsigned int',
            'budget_value_type': 'budget_value_type_enum',
            'time_end': 'unsigned int',
            'time_start': 'unsigned int',
        }
        enums = {
            'budget_value_type_enum': HighDemandPeriod.BudgetValueType.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/budget_schedules',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=HighDemandPeriod,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=HighDemandPeriod, api=self._api),
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

    def get_copies(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'date_preset': 'date_preset_enum',
            'effective_status': 'list<effective_status_enum>',
            'is_completed': 'bool',
            'time_range': 'map',
        }
        enums = {
            'date_preset_enum': AdSet.DatePreset.__dict__.values(),
            'effective_status_enum': AdSet.EffectiveStatus.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/copies',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdSet,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdSet, api=self._api),
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

    def create_copy(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'campaign_id': 'string',
            'create_dco_adset': 'bool',
            'deep_copy': 'bool',
            'end_time': 'datetime',
            'rename_options': 'Object',
            'start_time': 'datetime',
            'status_option': 'status_option_enum',
        }
        enums = {
            'status_option_enum': AdSet.StatusOption.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/copies',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdSet,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdSet, api=self._api),
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

    def get_delivery_estimate(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adcampaigndeliveryestimate import AdCampaignDeliveryEstimate
        param_types = {
            'optimization_goal': 'optimization_goal_enum',
            'promoted_object': 'Object',
            'targeting_spec': 'Targeting',
        }
        enums = {
            'optimization_goal_enum': AdCampaignDeliveryEstimate.OptimizationGoal.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/delivery_estimate',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdCampaignDeliveryEstimate,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdCampaignDeliveryEstimate, api=self._api),
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

    def get_insights(self, fields=None, params=None, is_async=False, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adsinsights import AdsInsights
        if is_async:
          return self.get_insights_async(fields, params, batch, success, failure, pending)
        param_types = {
            'action_attribution_windows': 'list<action_attribution_windows_enum>',
            'action_breakdowns': 'list<action_breakdowns_enum>',
            'action_report_time': 'action_report_time_enum',
            'breakdowns': 'list<breakdowns_enum>',
            'date_preset': 'date_preset_enum',
            'default_summary': 'bool',
            'export_columns': 'list<string>',
            'export_format': 'string',
            'export_name': 'string',
            'fields': 'list<string>',
            'filtering': 'list<Object>',
            'graph_cache': 'bool',
            'level': 'level_enum',
            'limit': 'int',
            'product_id_limit': 'int',
            'sort': 'list<string>',
            'summary': 'list<string>',
            'summary_action_breakdowns': 'list<summary_action_breakdowns_enum>',
            'time_increment': 'string',
            'time_range': 'map',
            'time_ranges': 'list<map>',
            'use_account_attribution_setting': 'bool',
            'use_unified_attribution_setting': 'bool',
        }
        enums = {
            'action_attribution_windows_enum': AdsInsights.ActionAttributionWindows.__dict__.values(),
            'action_breakdowns_enum': AdsInsights.ActionBreakdowns.__dict__.values(),
            'action_report_time_enum': AdsInsights.ActionReportTime.__dict__.values(),
            'breakdowns_enum': AdsInsights.Breakdowns.__dict__.values(),
            'date_preset_enum': AdsInsights.DatePreset.__dict__.values(),
            'level_enum': AdsInsights.Level.__dict__.values(),
            'summary_action_breakdowns_enum': AdsInsights.SummaryActionBreakdowns.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/insights',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdsInsights,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdsInsights, api=self._api),
            include_summary=False,
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

    def get_insights_async(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adreportrun import AdReportRun
        from facebook_business.adobjects.adsinsights import AdsInsights
        param_types = {
            'action_attribution_windows': 'list<action_attribution_windows_enum>',
            'action_breakdowns': 'list<action_breakdowns_enum>',
            'action_report_time': 'action_report_time_enum',
            'breakdowns': 'list<breakdowns_enum>',
            'date_preset': 'date_preset_enum',
            'default_summary': 'bool',
            'export_columns': 'list<string>',
            'export_format': 'string',
            'export_name': 'string',
            'fields': 'list<string>',
            'filtering': 'list<Object>',
            'graph_cache': 'bool',
            'level': 'level_enum',
            'limit': 'int',
            'product_id_limit': 'int',
            'sort': 'list<string>',
            'summary': 'list<string>',
            'summary_action_breakdowns': 'list<summary_action_breakdowns_enum>',
            'time_increment': 'string',
            'time_range': 'map',
            'time_ranges': 'list<map>',
            'use_account_attribution_setting': 'bool',
            'use_unified_attribution_setting': 'bool',
        }
        enums = {
            'action_attribution_windows_enum': AdsInsights.ActionAttributionWindows.__dict__.values(),
            'action_breakdowns_enum': AdsInsights.ActionBreakdowns.__dict__.values(),
            'action_report_time_enum': AdsInsights.ActionReportTime.__dict__.values(),
            'breakdowns_enum': AdsInsights.Breakdowns.__dict__.values(),
            'date_preset_enum': AdsInsights.DatePreset.__dict__.values(),
            'level_enum': AdsInsights.Level.__dict__.values(),
            'summary_action_breakdowns_enum': AdsInsights.SummaryActionBreakdowns.__dict__.values(),
        }

        if fields is not None:
            params['fields'] = params.get('fields') if params.get('fields') is not None else list()
            params['fields'].extend(field for field in fields if field not in params['fields'])

        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/insights',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdReportRun,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdReportRun, api=self._api),
            include_summary=False,
        )
        request.add_params(params)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_message_delivery_estimate(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.messagedeliveryestimate import MessageDeliveryEstimate
        param_types = {
            'bid_amount': 'unsigned int',
            'daily_budget': 'unsigned int',
            'is_direct_send_campaign': 'bool',
            'lifetime_budget': 'unsigned int',
            'lifetime_in_days': 'unsigned int',
            'optimization_goal': 'optimization_goal_enum',
            'pacing_type': 'pacing_type_enum',
            'promoted_object': 'Object',
            'targeting_spec': 'Targeting',
        }
        enums = {
            'optimization_goal_enum': MessageDeliveryEstimate.OptimizationGoal.__dict__.values(),
            'pacing_type_enum': MessageDeliveryEstimate.PacingType.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/message_delivery_estimate',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=MessageDeliveryEstimate,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=MessageDeliveryEstimate, api=self._api),
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

    def get_targeting_sentence_lines(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.targetingsentenceline import TargetingSentenceLine
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/targetingsentencelines',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=TargetingSentenceLine,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=TargetingSentenceLine, api=self._api),
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
        'account_id': 'string',
        'adlabels': 'list<AdLabel>',
        'adset_schedule': 'list<DayPart>',
        'anchor_event_attribution_window_days': 'int',
        'asset_feed_id': 'string',
        'attribution_spec': 'list<AttributionSpec>',
        'automatic_manual_state': 'string',
        'bid_adjustments': 'AdBidAdjustments',
        'bid_amount': 'unsigned int',
        'bid_constraints': 'AdCampaignBidConstraint',
        'bid_info': 'map<string, unsigned int>',
        'bid_strategy': 'BidStrategy',
        'billing_event': 'BillingEvent',
        'brand_safety_config': 'BrandSafetyCampaignConfig',
        'budget_remaining': 'string',
        'campaign': 'Campaign',
        'campaign_active_time': 'string',
        'campaign_attribution': 'string',
        'campaign_id': 'string',
        'configured_status': 'ConfiguredStatus',
        'created_time': 'datetime',
        'creative_sequence': 'list<string>',
        'creative_sequence_repetition_pattern': 'string',
        'daily_budget': 'string',
        'daily_min_spend_target': 'string',
        'daily_spend_cap': 'string',
        'destination_type': 'string',
        'dsa_beneficiary': 'string',
        'dsa_payor': 'string',
        'effective_status': 'EffectiveStatus',
        'end_time': 'datetime',
        'existing_customer_budget_percentage': 'unsigned int',
        'frequency_control_specs': 'list<AdCampaignFrequencyControlSpecs>',
        'full_funnel_exploration_mode': 'string',
        'id': 'string',
        'instagram_user_id': 'string',
        'is_ba_skip_delayed_eligible': 'bool',
        'is_budget_schedule_enabled': 'bool',
        'is_dynamic_creative': 'bool',
        'is_incremental_attribution_enabled': 'bool',
        'issues_info': 'list<AdCampaignIssuesInfo>',
        'learning_stage_info': 'AdCampaignLearningStageInfo',
        'lifetime_budget': 'string',
        'lifetime_imps': 'int',
        'lifetime_min_spend_target': 'string',
        'lifetime_spend_cap': 'string',
        'max_budget_spend_percentage': 'string',
        'min_budget_spend_percentage': 'string',
        'multi_optimization_goal_weight': 'string',
        'name': 'string',
        'optimization_goal': 'OptimizationGoal',
        'optimization_sub_event': 'string',
        'pacing_type': 'list<string>',
        'placement_soft_opt_out': 'PlacementSoftOptOut',
        'promoted_object': 'AdPromotedObject',
        'recommendations': 'list<AdRecommendation>',
        'recurring_budget_semantics': 'bool',
        'regional_regulated_categories': 'list<string>',
        'regional_regulation_identities': 'RegionalRegulationIdentities',
        'review_feedback': 'string',
        'rf_prediction_id': 'string',
        'source_adset': 'AdSet',
        'source_adset_id': 'string',
        'start_time': 'datetime',
        'status': 'Status',
        'targeting': 'Targeting',
        'targeting_optimization_types': 'list<map<string, int>>',
        'time_based_ad_rotation_id_blocks': 'list<list<int>>',
        'time_based_ad_rotation_intervals': 'list<unsigned int>',
        'trending_topics_spec': 'TrendingTopicsSpec',
        'updated_time': 'datetime',
        'use_new_app_click': 'bool',
        'value_rule_set_id': 'string',
        'value_rules_applied': 'bool',
        'budget_schedule_specs': 'list<Object>',
        'budget_source': 'BudgetSource',
        'budget_split_set_id': 'string',
        'campaign_spec': 'Object',
        'daily_imps': 'unsigned int',
        'date_format': 'string',
        'execution_options': 'list<ExecutionOptions>',
        'is_sac_cfca_terms_certified': 'bool',
        'line_number': 'unsigned int',
        'rb_prediction_id': 'string',
        'time_start': 'datetime',
        'time_stop': 'datetime',
        'topline_id': 'string',
        'tune_for_category': 'TuneForCategory',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['BidStrategy'] = AdSet.BidStrategy.__dict__.values()
        field_enum_info['BillingEvent'] = AdSet.BillingEvent.__dict__.values()
        field_enum_info['ConfiguredStatus'] = AdSet.ConfiguredStatus.__dict__.values()
        field_enum_info['EffectiveStatus'] = AdSet.EffectiveStatus.__dict__.values()
        field_enum_info['OptimizationGoal'] = AdSet.OptimizationGoal.__dict__.values()
        field_enum_info['Status'] = AdSet.Status.__dict__.values()
        field_enum_info['AutomaticManualState'] = AdSet.AutomaticManualState.__dict__.values()
        field_enum_info['BudgetSource'] = AdSet.BudgetSource.__dict__.values()
        field_enum_info['CreativeSequenceRepetitionPattern'] = AdSet.CreativeSequenceRepetitionPattern.__dict__.values()
        field_enum_info['DatePreset'] = AdSet.DatePreset.__dict__.values()
        field_enum_info['DestinationType'] = AdSet.DestinationType.__dict__.values()
        field_enum_info['ExecutionOptions'] = AdSet.ExecutionOptions.__dict__.values()
        field_enum_info['FullFunnelExplorationMode'] = AdSet.FullFunnelExplorationMode.__dict__.values()
        field_enum_info['MultiOptimizationGoalWeight'] = AdSet.MultiOptimizationGoalWeight.__dict__.values()
        field_enum_info['OptimizationSubEvent'] = AdSet.OptimizationSubEvent.__dict__.values()
        field_enum_info['RegionalRegulatedCategories'] = AdSet.RegionalRegulatedCategories.__dict__.values()
        field_enum_info['TuneForCategory'] = AdSet.TuneForCategory.__dict__.values()
        field_enum_info['Operator'] = AdSet.Operator.__dict__.values()
        field_enum_info['StatusOption'] = AdSet.StatusOption.__dict__.values()
        return field_enum_info


