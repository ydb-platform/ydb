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

class AdCampaignOptimizationEvent(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdCampaignOptimizationEvent, self).__init__()
        self._isAdCampaignOptimizationEvent = True
        self._api = api

    class Field(AbstractObject.Field):
        custom_conversion_id = 'custom_conversion_id'
        event_sequence = 'event_sequence'
        event_type = 'event_type'

    _field_types = {
        'custom_conversion_id': 'string',
        'event_sequence': 'unsigned int',
        'event_type': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


