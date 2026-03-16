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

class AdCampaignGroupStats(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdCampaignGroupStats, self).__init__()
        self._isAdCampaignGroupStats = True
        self._api = api

    class Field(AbstractObject.Field):
        actions = 'actions'
        campaign_group_id = 'campaign_group_id'
        clicks = 'clicks'
        end_time = 'end_time'
        impressions = 'impressions'
        inline_actions = 'inline_actions'
        social_clicks = 'social_clicks'
        social_impressions = 'social_impressions'
        social_spent = 'social_spent'
        social_unique_clicks = 'social_unique_clicks'
        social_unique_impressions = 'social_unique_impressions'
        spent = 'spent'
        start_time = 'start_time'
        unique_clicks = 'unique_clicks'
        unique_impressions = 'unique_impressions'

    _field_types = {
        'actions': 'map<string, int>',
        'campaign_group_id': 'string',
        'clicks': 'unsigned int',
        'end_time': 'datetime',
        'impressions': 'unsigned int',
        'inline_actions': 'map<string, int>',
        'social_clicks': 'unsigned int',
        'social_impressions': 'unsigned int',
        'social_spent': 'unsigned int',
        'social_unique_clicks': 'unsigned int',
        'social_unique_impressions': 'unsigned int',
        'spent': 'unsigned int',
        'start_time': 'datetime',
        'unique_clicks': 'unsigned int',
        'unique_impressions': 'unsigned int',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


