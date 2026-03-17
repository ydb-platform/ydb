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

class AdCampaignStats(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isAdCampaignStats = True
        super(AdCampaignStats, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        account_id = 'account_id'
        actions = 'actions'
        adgroup_id = 'adgroup_id'
        campaign_id = 'campaign_id'
        campaign_ids = 'campaign_ids'
        clicks = 'clicks'
        end_time = 'end_time'
        id = 'id'
        impressions = 'impressions'
        inline_actions = 'inline_actions'
        io_number = 'io_number'
        is_completed = 'is_completed'
        line_number = 'line_number'
        newsfeed_position = 'newsfeed_position'
        social_clicks = 'social_clicks'
        social_impressions = 'social_impressions'
        social_spent = 'social_spent'
        social_unique_clicks = 'social_unique_clicks'
        social_unique_impressions = 'social_unique_impressions'
        spent = 'spent'
        start_time = 'start_time'
        topline_id = 'topline_id'
        unique_clicks = 'unique_clicks'
        unique_impressions = 'unique_impressions'

    _field_types = {
        'account_id': 'string',
        'actions': 'Object',
        'adgroup_id': 'string',
        'campaign_id': 'string',
        'campaign_ids': 'list<string>',
        'clicks': 'unsigned int',
        'end_time': 'Object',
        'id': 'string',
        'impressions': 'string',
        'inline_actions': 'map',
        'io_number': 'unsigned int',
        'is_completed': 'bool',
        'line_number': 'unsigned int',
        'newsfeed_position': 'Object',
        'social_clicks': 'unsigned int',
        'social_impressions': 'string',
        'social_spent': 'unsigned int',
        'social_unique_clicks': 'unsigned int',
        'social_unique_impressions': 'string',
        'spent': 'int',
        'start_time': 'Object',
        'topline_id': 'string',
        'unique_clicks': 'unsigned int',
        'unique_impressions': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


