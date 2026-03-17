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

class Placement(
    AbstractObject,
):

    def __init__(self, api=None):
        super(Placement, self).__init__()
        self._isPlacement = True
        self._api = api

    class Field(AbstractObject.Field):
        audience_network_positions = 'audience_network_positions'
        device_platforms = 'device_platforms'
        effective_audience_network_positions = 'effective_audience_network_positions'
        effective_device_platforms = 'effective_device_platforms'
        effective_facebook_positions = 'effective_facebook_positions'
        effective_instagram_positions = 'effective_instagram_positions'
        effective_messenger_positions = 'effective_messenger_positions'
        effective_oculus_positions = 'effective_oculus_positions'
        effective_publisher_platforms = 'effective_publisher_platforms'
        effective_threads_positions = 'effective_threads_positions'
        effective_whatsapp_positions = 'effective_whatsapp_positions'
        facebook_positions = 'facebook_positions'
        instagram_positions = 'instagram_positions'
        messenger_positions = 'messenger_positions'
        oculus_positions = 'oculus_positions'
        publisher_platforms = 'publisher_platforms'
        threads_positions = 'threads_positions'
        whatsapp_positions = 'whatsapp_positions'

    class DevicePlatforms:
        desktop = 'desktop'
        mobile = 'mobile'

    class EffectiveDevicePlatforms:
        desktop = 'desktop'
        mobile = 'mobile'

    _field_types = {
        'audience_network_positions': 'list<string>',
        'device_platforms': 'list<DevicePlatforms>',
        'effective_audience_network_positions': 'list<string>',
        'effective_device_platforms': 'list<EffectiveDevicePlatforms>',
        'effective_facebook_positions': 'list<string>',
        'effective_instagram_positions': 'list<string>',
        'effective_messenger_positions': 'list<string>',
        'effective_oculus_positions': 'list<string>',
        'effective_publisher_platforms': 'list<string>',
        'effective_threads_positions': 'list<string>',
        'effective_whatsapp_positions': 'list<string>',
        'facebook_positions': 'list<string>',
        'instagram_positions': 'list<string>',
        'messenger_positions': 'list<string>',
        'oculus_positions': 'list<string>',
        'publisher_platforms': 'list<string>',
        'threads_positions': 'list<string>',
        'whatsapp_positions': 'list<string>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['DevicePlatforms'] = Placement.DevicePlatforms.__dict__.values()
        field_enum_info['EffectiveDevicePlatforms'] = Placement.EffectiveDevicePlatforms.__dict__.values()
        return field_enum_info


