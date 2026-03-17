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

class AdAccountCustomAudienceLimits(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdAccountCustomAudienceLimits, self).__init__()
        self._isAdAccountCustomAudienceLimits = True
        self._api = api

    class Field(AbstractObject.Field):
        audience_update_quota_in_total = 'audience_update_quota_in_total'
        audience_update_quota_left = 'audience_update_quota_left'
        has_hit_audience_update_limit = 'has_hit_audience_update_limit'
        next_audience_update_available_time = 'next_audience_update_available_time'
        rate_limit_reset_time = 'rate_limit_reset_time'

    _field_types = {
        'audience_update_quota_in_total': 'int',
        'audience_update_quota_left': 'float',
        'has_hit_audience_update_limit': 'bool',
        'next_audience_update_available_time': 'string',
        'rate_limit_reset_time': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


