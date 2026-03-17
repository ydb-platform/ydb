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

class AdsPixelEventSuggestionRule(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdsPixelEventSuggestionRule, self).__init__()
        self._isAdsPixelEventSuggestionRule = True
        self._api = api

    class Field(AbstractObject.Field):
        field_7d_volume = '7d_volume'
        dismissed = 'dismissed'
        end_time = 'end_time'
        event_type = 'event_type'
        rank = 'rank'
        rule = 'rule'
        sample_urls = 'sample_urls'
        start_time = 'start_time'

    _field_types = {
        '7d_volume': 'int',
        'dismissed': 'bool',
        'end_time': 'datetime',
        'event_type': 'string',
        'rank': 'int',
        'rule': 'string',
        'sample_urls': 'list<string>',
        'start_time': 'datetime',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


