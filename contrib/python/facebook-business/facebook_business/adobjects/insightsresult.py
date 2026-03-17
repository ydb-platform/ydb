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

class InsightsResult(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isInsightsResult = True
        super(InsightsResult, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        description = 'description'
        description_from_api_doc = 'description_from_api_doc'
        id = 'id'
        name = 'name'
        period = 'period'
        title = 'title'
        values = 'values'

    class Breakdown:
        action_type = 'action_type'
        follow_type = 'follow_type'
        story_navigation_action_type = 'story_navigation_action_type'
        surface_type = 'surface_type'

    class Metric:
        clips_replays_count = 'clips_replays_count'
        comments = 'comments'
        content_views = 'content_views'
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
        quotes = 'quotes'
        reach = 'reach'
        replies = 'replies'
        reposts = 'reposts'
        saved = 'saved'
        shares = 'shares'
        thread_replies = 'thread_replies'
        thread_shares = 'thread_shares'
        threads_media_clicks = 'threads_media_clicks'
        threads_views = 'threads_views'
        total_interactions = 'total_interactions'
        views = 'views'

    class Period:
        day = 'day'
        days_28 = 'days_28'
        lifetime = 'lifetime'
        month = 'month'
        total_over_range = 'total_over_range'
        week = 'week'

    class DatePreset:
        data_maximum = 'data_maximum'
        last_14d = 'last_14d'
        last_28d = 'last_28d'
        last_30d = 'last_30d'
        last_3d = 'last_3d'
        last_7d = 'last_7d'
        last_90d = 'last_90d'
        last_month = 'last_month'
        last_quarter = 'last_quarter'
        last_week_mon_sun = 'last_week_mon_sun'
        last_week_sun_sat = 'last_week_sun_sat'
        last_year = 'last_year'
        maximum = 'maximum'
        this_month = 'this_month'
        this_quarter = 'this_quarter'
        this_week_mon_today = 'this_week_mon_today'
        this_week_sun_today = 'this_week_sun_today'
        this_year = 'this_year'
        today = 'today'
        yesterday = 'yesterday'

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
        'description_from_api_doc': 'string',
        'id': 'string',
        'name': 'string',
        'period': 'string',
        'title': 'string',
        'values': 'list<InsightsValue>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['Breakdown'] = InsightsResult.Breakdown.__dict__.values()
        field_enum_info['Metric'] = InsightsResult.Metric.__dict__.values()
        field_enum_info['Period'] = InsightsResult.Period.__dict__.values()
        field_enum_info['DatePreset'] = InsightsResult.DatePreset.__dict__.values()
        field_enum_info['MetricType'] = InsightsResult.MetricType.__dict__.values()
        field_enum_info['Timeframe'] = InsightsResult.Timeframe.__dict__.values()
        return field_enum_info


