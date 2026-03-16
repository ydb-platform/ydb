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

class AdsPixelStatsResult(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdsPixelStatsResult, self).__init__()
        self._isAdsPixelStatsResult = True
        self._api = api

    class Field(AbstractObject.Field):
        aggregation = 'aggregation'
        data = 'data'
        start_time = 'start_time'

    class Aggregation:
        browser_type = 'browser_type'
        custom_data_field = 'custom_data_field'
        device_os = 'device_os'
        device_type = 'device_type'
        event = 'event'
        event_detection_method = 'event_detection_method'
        event_processing_results = 'event_processing_results'
        event_source = 'event_source'
        event_total_counts = 'event_total_counts'
        event_value_count = 'event_value_count'
        had_pii = 'had_pii'
        host = 'host'
        match_keys = 'match_keys'
        pixel_fire = 'pixel_fire'
        url = 'url'
        url_by_rule = 'url_by_rule'

    # @deprecated get_endpoint function is deprecated
    @classmethod
    def get_endpoint(cls):
        return 'stats'

    _field_types = {
        'aggregation': 'string',
        'data': 'list<AdsPixelStats>',
        'start_time': 'datetime',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['Aggregation'] = AdsPixelStatsResult.Aggregation.__dict__.values()
        return field_enum_info


