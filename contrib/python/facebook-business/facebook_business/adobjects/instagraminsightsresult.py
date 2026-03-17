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

class InstagramInsightsResult(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isInstagramInsightsResult = True
        super(InstagramInsightsResult, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        description = 'description'
        id = 'id'
        name = 'name'
        period = 'period'
        title = 'title'
        total_value = 'total_value'
        values = 'values'

    class Breakdown:
        action_type = 'action_type'
        follow_type = 'follow_type'
        story_navigation_action_type = 'story_navigation_action_type'
        surface_type = 'surface_type'

    class Metric:
        clips_replays_count = 'clips_replays_count'
        comments = 'comments'
        follows = 'follows'
        ig_reels_aggregated_all_plays_count = 'ig_reels_aggregated_all_plays_count'
        ig_reels_avg_watch_time = 'ig_reels_avg_watch_time'
        ig_reels_video_view_total_time = 'ig_reels_video_view_total_time'
        impressions = 'impressions'
        likes = 'likes'
        navigation = 'navigation'
        plays = 'plays'
        profile_activity = 'profile_activity'
        profile_visits = 'profile_visits'
        reach = 'reach'
        replies = 'replies'
        saved = 'saved'
        shares = 'shares'
        total_interactions = 'total_interactions'
        video_views = 'video_views'
        views = 'views'

    class Period:
        day = 'day'
        days_28 = 'days_28'
        lifetime = 'lifetime'
        month = 'month'
        total_over_range = 'total_over_range'
        week = 'week'

    class MetricType:
        value_default = 'default'
        time_series = 'time_series'
        total_value = 'total_value'

    class Timeframe:
        last_14_days = 'last_14_days'
        last_30_days = 'last_30_days'
        last_90_days = 'last_90_days'
        prev_month = 'prev_month'
        this_month = 'this_month'
        this_week = 'this_week'

    _field_types = {
        'description': 'string',
        'id': 'string',
        'name': 'string',
        'period': 'string',
        'title': 'string',
        'total_value': 'Object',
        'values': 'list<InstagramInsightsValue>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['Breakdown'] = InstagramInsightsResult.Breakdown.__dict__.values()
        field_enum_info['Metric'] = InstagramInsightsResult.Metric.__dict__.values()
        field_enum_info['Period'] = InstagramInsightsResult.Period.__dict__.values()
        field_enum_info['MetricType'] = InstagramInsightsResult.MetricType.__dict__.values()
        field_enum_info['Timeframe'] = InstagramInsightsResult.Timeframe.__dict__.values()
        return field_enum_info


