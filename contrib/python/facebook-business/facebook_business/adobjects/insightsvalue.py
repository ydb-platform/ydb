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

class InsightsValue(
    AbstractObject,
):

    def __init__(self, api=None):
        super(InsightsValue, self).__init__()
        self._isInsightsValue = True
        self._api = api

    class Field(AbstractObject.Field):
        campaign_id = 'campaign_id'
        earning_source = 'earning_source'
        end_time = 'end_time'
        engagement_source = 'engagement_source'
        is_from_ads = 'is_from_ads'
        is_from_followers = 'is_from_followers'
        message_type = 'message_type'
        messaging_channel = 'messaging_channel'
        monetization_tool = 'monetization_tool'
        recurring_notifications_entry_point = 'recurring_notifications_entry_point'
        recurring_notifications_frequency = 'recurring_notifications_frequency'
        recurring_notifications_topic = 'recurring_notifications_topic'
        start_time = 'start_time'
        value = 'value'

    _field_types = {
        'campaign_id': 'string',
        'earning_source': 'string',
        'end_time': 'datetime',
        'engagement_source': 'string',
        'is_from_ads': 'string',
        'is_from_followers': 'string',
        'message_type': 'string',
        'messaging_channel': 'string',
        'monetization_tool': 'string',
        'recurring_notifications_entry_point': 'string',
        'recurring_notifications_frequency': 'string',
        'recurring_notifications_topic': 'string',
        'start_time': 'datetime',
        'value': 'Object',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


