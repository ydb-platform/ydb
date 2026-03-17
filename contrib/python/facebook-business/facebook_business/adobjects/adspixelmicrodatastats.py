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

class AdsPixelMicrodataStats(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdsPixelMicrodataStats, self).__init__()
        self._isAdsPixelMicrodataStats = True
        self._api = api

    class Field(AbstractObject.Field):
        allowed_domains = 'allowed_domains'
        errors_stats_for_time_ranges = 'errors_stats_for_time_ranges'
        has_valid_events = 'has_valid_events'
        suggested_allowed_domains_count_max = 'suggested_allowed_domains_count_max'
        suggested_trusted_domains = 'suggested_trusted_domains'

    _field_types = {
        'allowed_domains': 'list<string>',
        'errors_stats_for_time_ranges': 'list<Object>',
        'has_valid_events': 'bool',
        'suggested_allowed_domains_count_max': 'int',
        'suggested_trusted_domains': 'list<string>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


