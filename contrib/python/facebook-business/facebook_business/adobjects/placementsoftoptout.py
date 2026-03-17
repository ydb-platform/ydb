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

class PlacementSoftOptOut(
    AbstractObject,
):

    def __init__(self, api=None):
        super(PlacementSoftOptOut, self).__init__()
        self._isPlacementSoftOptOut = True
        self._api = api

    class Field(AbstractObject.Field):
        audience_network_positions = 'audience_network_positions'
        facebook_positions = 'facebook_positions'
        instagram_positions = 'instagram_positions'
        messenger_positions = 'messenger_positions'
        oculus_positions = 'oculus_positions'
        threads_positions = 'threads_positions'
        whatsapp_positions = 'whatsapp_positions'

    _field_types = {
        'audience_network_positions': 'list<string>',
        'facebook_positions': 'list<string>',
        'instagram_positions': 'list<string>',
        'messenger_positions': 'list<string>',
        'oculus_positions': 'list<string>',
        'threads_positions': 'list<string>',
        'whatsapp_positions': 'list<string>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


