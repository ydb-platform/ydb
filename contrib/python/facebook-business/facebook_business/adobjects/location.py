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

class Location(
    AbstractObject,
):

    def __init__(self, api=None):
        super(Location, self).__init__()
        self._isLocation = True
        self._api = api

    class Field(AbstractObject.Field):
        city = 'city'
        city_id = 'city_id'
        country = 'country'
        country_code = 'country_code'
        latitude = 'latitude'
        located_in = 'located_in'
        longitude = 'longitude'
        name = 'name'
        region = 'region'
        region_id = 'region_id'
        state = 'state'
        street = 'street'
        zip = 'zip'

    _field_types = {
        'city': 'string',
        'city_id': 'unsigned int',
        'country': 'string',
        'country_code': 'string',
        'latitude': 'float',
        'located_in': 'string',
        'longitude': 'float',
        'name': 'string',
        'region': 'string',
        'region_id': 'unsigned int',
        'state': 'string',
        'street': 'string',
        'zip': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


