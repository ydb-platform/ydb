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

class AdAssetTargetRuleTargeting(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdAssetTargetRuleTargeting, self).__init__()
        self._isAdAssetTargetRuleTargeting = True
        self._api = api

    class Field(AbstractObject.Field):
        age_max = 'age_max'
        age_min = 'age_min'
        audience_network_positions = 'audience_network_positions'
        device_platforms = 'device_platforms'
        facebook_positions = 'facebook_positions'
        geo_locations = 'geo_locations'
        instagram_positions = 'instagram_positions'
        publisher_platforms = 'publisher_platforms'
        threads_positions = 'threads_positions'

    class DevicePlatforms:
        desktop = 'desktop'
        mobile = 'mobile'

    _field_types = {
        'age_max': 'unsigned int',
        'age_min': 'unsigned int',
        'audience_network_positions': 'list<string>',
        'device_platforms': 'list<DevicePlatforms>',
        'facebook_positions': 'list<string>',
        'geo_locations': 'TargetingGeoLocation',
        'instagram_positions': 'list<string>',
        'publisher_platforms': 'list<string>',
        'threads_positions': 'list<string>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['DevicePlatforms'] = AdAssetTargetRuleTargeting.DevicePlatforms.__dict__.values()
        return field_enum_info


