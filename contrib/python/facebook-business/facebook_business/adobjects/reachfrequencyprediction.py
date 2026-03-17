# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

from facebook_business.adobjects.abstractobject import AbstractObject
from facebook_business.adobjects.abstractcrudobject import AbstractCrudObject
from facebook_business.adobjects.objectparser import ObjectParser
from facebook_business.api import FacebookRequest
from facebook_business.typechecker import TypeChecker
from facebook_business.adobjects.helpers.reachfrequencypredictionmixin import ReachFrequencyPredictionMixin

"""
This class is auto-generated.

For any issues or feature requests related to this class, please let us know on
github and we'll fix in our codegen framework. We'll not be able to accept
pull request for this class.
"""

class ReachFrequencyPrediction(
    ReachFrequencyPredictionMixin,
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isReachFrequencyPrediction = True
        super(ReachFrequencyPrediction, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        account_id = 'account_id'
        activity_status = 'activity_status'
        ad_formats = 'ad_formats'
        auction_entry_option_index = 'auction_entry_option_index'
        audience_size_lower_bound = 'audience_size_lower_bound'
        audience_size_upper_bound = 'audience_size_upper_bound'
        business_id = 'business_id'
        buying_type = 'buying_type'
        campaign_group_id = 'campaign_group_id'
        campaign_id = 'campaign_id'
        campaign_time_start = 'campaign_time_start'
        campaign_time_stop = 'campaign_time_stop'
        currency = 'currency'
        curve_budget_reach = 'curve_budget_reach'
        curve_reach = 'curve_reach'
        daily_grp_curve = 'daily_grp_curve'
        daily_impression_curve = 'daily_impression_curve'
        daily_impression_curve_map = 'daily_impression_curve_map'
        day_parting_schedule = 'day_parting_schedule'
        destination_id = 'destination_id'
        end_time = 'end_time'
        expiration_time = 'expiration_time'
        external_budget = 'external_budget'
        external_impression = 'external_impression'
        external_maximum_budget = 'external_maximum_budget'
        external_maximum_impression = 'external_maximum_impression'
        external_maximum_reach = 'external_maximum_reach'
        external_minimum_budget = 'external_minimum_budget'
        external_minimum_impression = 'external_minimum_impression'
        external_minimum_reach = 'external_minimum_reach'
        external_reach = 'external_reach'
        feed_ratio_0000 = 'feed_ratio_0000'
        frequency_cap = 'frequency_cap'
        frequency_distribution_map = 'frequency_distribution_map'
        frequency_distribution_map_agg = 'frequency_distribution_map_agg'
        grp_audience_size = 'grp_audience_size'
        grp_avg_probability_map = 'grp_avg_probability_map'
        grp_country_audience_size = 'grp_country_audience_size'
        grp_curve = 'grp_curve'
        grp_dmas_audience_size = 'grp_dmas_audience_size'
        grp_filtering_threshold_00 = 'grp_filtering_threshold_00'
        grp_points = 'grp_points'
        grp_ratio = 'grp_ratio'
        grp_reach_ratio = 'grp_reach_ratio'
        grp_status = 'grp_status'
        holdout_percentage = 'holdout_percentage'
        id = 'id'
        impression_curve = 'impression_curve'
        instagram_destination_id = 'instagram_destination_id'
        instream_packages = 'instream_packages'
        interval_frequency_cap = 'interval_frequency_cap'
        interval_frequency_cap_reset_period = 'interval_frequency_cap_reset_period'
        is_balanced_frequency = 'is_balanced_frequency'
        is_bonus_media = 'is_bonus_media'
        is_conversion_goal = 'is_conversion_goal'
        is_higher_average_frequency = 'is_higher_average_frequency'
        is_io = 'is_io'
        is_reserved_buying = 'is_reserved_buying'
        is_trp = 'is_trp'
        name = 'name'
        objective = 'objective'
        objective_name = 'objective_name'
        odax_objective = 'odax_objective'
        odax_objective_name = 'odax_objective_name'
        optimization_goal = 'optimization_goal'
        optimization_goal_name = 'optimization_goal_name'
        pause_periods = 'pause_periods'
        percent_reach_at_target_frequency = 'percent_reach_at_target_frequency'
        placement_breakdown = 'placement_breakdown'
        placement_breakdown_map = 'placement_breakdown_map'
        plan_name = 'plan_name'
        plan_type = 'plan_type'
        prediction_mode = 'prediction_mode'
        prediction_progress = 'prediction_progress'
        reference_id = 'reference_id'
        reservation_status = 'reservation_status'
        start_time = 'start_time'
        status = 'status'
        story_event_type = 'story_event_type'
        target_cpm = 'target_cpm'
        target_frequency = 'target_frequency'
        target_frequency_reset_period = 'target_frequency_reset_period'
        target_spec = 'target_spec'
        time_created = 'time_created'
        time_updated = 'time_updated'
        timezone_id = 'timezone_id'
        timezone_name = 'timezone_name'
        topline_id = 'topline_id'
        trending_topics_spec = 'trending_topics_spec'
        video_view_length_constraint = 'video_view_length_constraint'
        viewtag = 'viewtag'
        action = 'action'
        budget = 'budget'
        deal_id = 'deal_id'
        destination_ids = 'destination_ids'
        exceptions = 'exceptions'
        existing_campaign_id = 'existing_campaign_id'
        grp_buying = 'grp_buying'
        impression = 'impression'
        is_full_view = 'is_full_view'
        is_reach_and_frequency_io_buying = 'is_reach_and_frequency_io_buying'
        num_curve_points = 'num_curve_points'
        reach = 'reach'
        rf_prediction_id = 'rf_prediction_id'
        rf_prediction_id_to_release = 'rf_prediction_id_to_release'
        rf_prediction_id_to_share = 'rf_prediction_id_to_share'
        stop_time = 'stop_time'

    class Action:
        cancel = 'cancel'
        quote = 'quote'
        reserve = 'reserve'

    class BuyingType:
        auction = 'AUCTION'
        deprecated_reach_block = 'DEPRECATED_REACH_BLOCK'
        fixed_cpm = 'FIXED_CPM'
        mixed = 'MIXED'
        reachblock = 'REACHBLOCK'
        research_poll = 'RESEARCH_POLL'
        reserved = 'RESERVED'

    class InstreamPackages:
        beauty = 'BEAUTY'
        entertainment = 'ENTERTAINMENT'
        food = 'FOOD'
        normal = 'NORMAL'
        premium = 'PREMIUM'
        regular_animals_pets = 'REGULAR_ANIMALS_PETS'
        regular_food = 'REGULAR_FOOD'
        regular_games = 'REGULAR_GAMES'
        regular_politics = 'REGULAR_POLITICS'
        regular_sports = 'REGULAR_SPORTS'
        regular_style = 'REGULAR_STYLE'
        regular_tv_movies = 'REGULAR_TV_MOVIES'
        spanish = 'SPANISH'
        sports = 'SPORTS'

    # @deprecated get_endpoint function is deprecated
    @classmethod
    def get_endpoint(cls):
        return 'reachfrequencypredictions'

    # @deprecated api_create is being deprecated
    def api_create(self, parent_id, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.adobjects.adaccount import AdAccount
        return AdAccount(api=self._api, fbid=parent_id).create_reach_frequency_prediction(fields, params, batch, success, failure, pending)

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
            target_class=ReachFrequencyPrediction,
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
        'account_id': 'int',
        'activity_status': 'ReachFrequencyActivity',
        'ad_formats': 'list<ReachFrequencyAdFormat>',
        'auction_entry_option_index': 'int',
        'audience_size_lower_bound': 'unsigned int',
        'audience_size_upper_bound': 'unsigned int',
        'business_id': 'int',
        'buying_type': 'string',
        'campaign_group_id': 'int',
        'campaign_id': 'string',
        'campaign_time_start': 'datetime',
        'campaign_time_stop': 'datetime',
        'currency': 'string',
        'curve_budget_reach': 'ReachFrequencyEstimatesCurve',
        'curve_reach': 'list<unsigned int>',
        'daily_grp_curve': 'list<float>',
        'daily_impression_curve': 'list<float>',
        'daily_impression_curve_map': 'list<map<unsigned int, list<float>>>',
        'day_parting_schedule': 'list<ReachFrequencyDayPart>',
        'destination_id': 'string',
        'end_time': 'datetime',
        'expiration_time': 'datetime',
        'external_budget': 'int',
        'external_impression': 'unsigned int',
        'external_maximum_budget': 'int',
        'external_maximum_impression': 'string',
        'external_maximum_reach': 'unsigned int',
        'external_minimum_budget': 'int',
        'external_minimum_impression': 'unsigned int',
        'external_minimum_reach': 'unsigned int',
        'external_reach': 'unsigned int',
        'feed_ratio_0000': 'unsigned int',
        'frequency_cap': 'unsigned int',
        'frequency_distribution_map': 'list<map<unsigned int, list<float>>>',
        'frequency_distribution_map_agg': 'list<map<unsigned int, list<unsigned int>>>',
        'grp_audience_size': 'float',
        'grp_avg_probability_map': 'string',
        'grp_country_audience_size': 'float',
        'grp_curve': 'list<float>',
        'grp_dmas_audience_size': 'float',
        'grp_filtering_threshold_00': 'unsigned int',
        'grp_points': 'float',
        'grp_ratio': 'float',
        'grp_reach_ratio': 'float',
        'grp_status': 'string',
        'holdout_percentage': 'unsigned int',
        'id': 'string',
        'impression_curve': 'list<unsigned int>',
        'instagram_destination_id': 'string',
        'instream_packages': 'list<string>',
        'interval_frequency_cap': 'unsigned int',
        'interval_frequency_cap_reset_period': 'unsigned int',
        'is_balanced_frequency': 'bool',
        'is_bonus_media': 'unsigned int',
        'is_conversion_goal': 'unsigned int',
        'is_higher_average_frequency': 'bool',
        'is_io': 'bool',
        'is_reserved_buying': 'unsigned int',
        'is_trp': 'bool',
        'name': 'string',
        'objective': 'unsigned int',
        'objective_name': 'string',
        'odax_objective': 'unsigned int',
        'odax_objective_name': 'string',
        'optimization_goal': 'unsigned int',
        'optimization_goal_name': 'string',
        'pause_periods': 'list<Object>',
        'percent_reach_at_target_frequency': 'int',
        'placement_breakdown': 'ReachFrequencyEstimatesPlacementBreakdown',
        'placement_breakdown_map': 'list<map<unsigned int, ReachFrequencyEstimatesPlacementBreakdown>>',
        'plan_name': 'string',
        'plan_type': 'string',
        'prediction_mode': 'unsigned int',
        'prediction_progress': 'unsigned int',
        'reference_id': 'string',
        'reservation_status': 'unsigned int',
        'start_time': 'datetime',
        'status': 'unsigned int',
        'story_event_type': 'unsigned int',
        'target_cpm': 'unsigned int',
        'target_frequency': 'unsigned int',
        'target_frequency_reset_period': 'unsigned int',
        'target_spec': 'Targeting',
        'time_created': 'datetime',
        'time_updated': 'datetime',
        'timezone_id': 'unsigned int',
        'timezone_name': 'string',
        'topline_id': 'unsigned int',
        'trending_topics_spec': 'TrendingTopicsSpec',
        'video_view_length_constraint': 'unsigned int',
        'viewtag': 'string',
        'action': 'Action',
        'budget': 'unsigned int',
        'deal_id': 'string',
        'destination_ids': 'list<string>',
        'exceptions': 'bool',
        'existing_campaign_id': 'string',
        'grp_buying': 'bool',
        'impression': 'unsigned int',
        'is_full_view': 'bool',
        'is_reach_and_frequency_io_buying': 'bool',
        'num_curve_points': 'unsigned int',
        'reach': 'unsigned int',
        'rf_prediction_id': 'string',
        'rf_prediction_id_to_release': 'string',
        'rf_prediction_id_to_share': 'string',
        'stop_time': 'unsigned int',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['Action'] = ReachFrequencyPrediction.Action.__dict__.values()
        field_enum_info['BuyingType'] = ReachFrequencyPrediction.BuyingType.__dict__.values()
        field_enum_info['InstreamPackages'] = ReachFrequencyPrediction.InstreamPackages.__dict__.values()
        return field_enum_info


