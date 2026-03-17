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

class SalesRightsInventoryManagement(
    AbstractObject,
):

    def __init__(self, api=None):
        super(SalesRightsInventoryManagement, self).__init__()
        self._isSalesRightsInventoryManagement = True
        self._api = api

    class Field(AbstractObject.Field):
        available_impressions = 'available_impressions'
        booked_impressions = 'booked_impressions'
        overbooked_impressions = 'overbooked_impressions'
        supported_countries = 'supported_countries'
        total_impressions = 'total_impressions'
        unavailable_impressions = 'unavailable_impressions'
        warning_messages = 'warning_messages'

    _field_types = {
        'available_impressions': 'int',
        'booked_impressions': 'int',
        'overbooked_impressions': 'int',
        'supported_countries': 'list<string>',
        'total_impressions': 'int',
        'unavailable_impressions': 'int',
        'warning_messages': 'list<string>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


