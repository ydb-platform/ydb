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

class MessageDeliveryEstimate(
    AbstractObject,
):

    def __init__(self, api=None):
        super(MessageDeliveryEstimate, self).__init__()
        self._isMessageDeliveryEstimate = True
        self._api = api

    class Field(AbstractObject.Field):
        estimate_cost = 'estimate_cost'
        estimate_cost_lower_bound = 'estimate_cost_lower_bound'
        estimate_cost_upper_bound = 'estimate_cost_upper_bound'
        estimate_coverage_lower_bound = 'estimate_coverage_lower_bound'
        estimate_coverage_upper_bound = 'estimate_coverage_upper_bound'
        estimate_delivery = 'estimate_delivery'
        estimate_delivery_lower_bound = 'estimate_delivery_lower_bound'
        estimate_delivery_upper_bound = 'estimate_delivery_upper_bound'
        estimate_status = 'estimate_status'

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

    class PacingType:
        day_parting = 'DAY_PARTING'
        disabled = 'DISABLED'
        no_pacing = 'NO_PACING'
        probabilistic_pacing = 'PROBABILISTIC_PACING'
        probabilistic_pacing_v2 = 'PROBABILISTIC_PACING_V2'
        standard = 'STANDARD'

    _field_types = {
        'estimate_cost': 'float',
        'estimate_cost_lower_bound': 'float',
        'estimate_cost_upper_bound': 'float',
        'estimate_coverage_lower_bound': 'int',
        'estimate_coverage_upper_bound': 'int',
        'estimate_delivery': 'int',
        'estimate_delivery_lower_bound': 'int',
        'estimate_delivery_upper_bound': 'int',
        'estimate_status': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['OptimizationGoal'] = MessageDeliveryEstimate.OptimizationGoal.__dict__.values()
        field_enum_info['PacingType'] = MessageDeliveryEstimate.PacingType.__dict__.values()
        return field_enum_info


