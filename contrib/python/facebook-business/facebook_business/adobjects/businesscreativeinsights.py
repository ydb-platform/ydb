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

class BusinessCreativeInsights(
    AbstractObject,
):

    def __init__(self, api=None):
        super(BusinessCreativeInsights, self).__init__()
        self._isBusinessCreativeInsights = True
        self._api = api

    class Field(AbstractObject.Field):
        actions = 'actions'
        age = 'age'
        country = 'country'
        date_end = 'date_end'
        date_start = 'date_start'
        device_platform = 'device_platform'
        gender = 'gender'
        impressions = 'impressions'
        inline_link_clicks = 'inline_link_clicks'
        objective = 'objective'
        optimization_goal = 'optimization_goal'
        platform_position = 'platform_position'
        publisher_platform = 'publisher_platform'
        quality_ranking = 'quality_ranking'
        video_play_actions = 'video_play_actions'
        video_thruplay_watched_actions = 'video_thruplay_watched_actions'

    _field_types = {
        'actions': 'list<AdsActionStats>',
        'age': 'string',
        'country': 'string',
        'date_end': 'string',
        'date_start': 'string',
        'device_platform': 'string',
        'gender': 'string',
        'impressions': 'int',
        'inline_link_clicks': 'int',
        'objective': 'string',
        'optimization_goal': 'string',
        'platform_position': 'string',
        'publisher_platform': 'string',
        'quality_ranking': 'string',
        'video_play_actions': 'list<AdsActionStats>',
        'video_thruplay_watched_actions': 'list<AdsActionStats>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


