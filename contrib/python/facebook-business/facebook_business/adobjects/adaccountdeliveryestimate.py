# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

from facebook_business.adobjects.abstractobject import AbstractObject

"""
This class is auto-generated.

For any issues or feature requests related to this class, please let us know on
github and we'll fix in our codegen framework. We'll not be able to accept
pull request for this class.
"""

class AdAccountDeliveryEstimate(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdAccountDeliveryEstimate, self).__init__()
        self._isAdAccountDeliveryEstimate = True
        self._api = api

    class Field(AbstractObject.Field):
        daily_outcomes_curve = 'daily_outcomes_curve'
        estimate_dau = 'estimate_dau'
        estimate_mau_lower_bound = 'estimate_mau_lower_bound'
        estimate_mau_upper_bound = 'estimate_mau_upper_bound'
        estimate_ready = 'estimate_ready'
        targeting_optimization_types = 'targeting_optimization_types'

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

    _field_types = {
        'daily_outcomes_curve': 'list<OutcomePredictionPoint>',
        'estimate_dau': 'int',
        'estimate_mau_lower_bound': 'int',
        'estimate_mau_upper_bound': 'int',
        'estimate_ready': 'bool',
        'targeting_optimization_types': 'list<map<string, int>>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['OptimizationGoal'] = AdAccountDeliveryEstimate.OptimizationGoal.__dict__.values()
        return field_enum_info


