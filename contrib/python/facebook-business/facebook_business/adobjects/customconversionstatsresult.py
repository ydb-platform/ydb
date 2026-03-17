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

class CustomConversionStatsResult(
    AbstractObject,
):

    def __init__(self, api=None):
        super(CustomConversionStatsResult, self).__init__()
        self._isCustomConversionStatsResult = True
        self._api = api

    class Field(AbstractObject.Field):
        aggregation = 'aggregation'
        data = 'data'
        timestamp = 'timestamp'

    class Aggregation:
        count = 'count'
        device_type = 'device_type'
        host = 'host'
        pixel_fire = 'pixel_fire'
        unmatched_count = 'unmatched_count'
        unmatched_usd_amount = 'unmatched_usd_amount'
        url = 'url'
        usd_amount = 'usd_amount'

    _field_types = {
        'aggregation': 'Aggregation',
        'data': 'list<Object>',
        'timestamp': 'datetime',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['Aggregation'] = CustomConversionStatsResult.Aggregation.__dict__.values()
        return field_enum_info


