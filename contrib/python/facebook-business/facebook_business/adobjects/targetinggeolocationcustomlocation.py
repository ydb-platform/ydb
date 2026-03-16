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

class TargetingGeoLocationCustomLocation(
    AbstractObject,
):

    def __init__(self, api=None):
        super(TargetingGeoLocationCustomLocation, self).__init__()
        self._isTargetingGeoLocationCustomLocation = True
        self._api = api

    class Field(AbstractObject.Field):
        address_string = 'address_string'
        country = 'country'
        country_group = 'country_group'
        custom_type = 'custom_type'
        distance_unit = 'distance_unit'
        key = 'key'
        latitude = 'latitude'
        longitude = 'longitude'
        max_population = 'max_population'
        min_population = 'min_population'
        name = 'name'
        primary_city_id = 'primary_city_id'
        radius = 'radius'
        region_id = 'region_id'

    _field_types = {
        'address_string': 'string',
        'country': 'string',
        'country_group': 'string',
        'custom_type': 'string',
        'distance_unit': 'string',
        'key': 'string',
        'latitude': 'float',
        'longitude': 'float',
        'max_population': 'int',
        'min_population': 'int',
        'name': 'string',
        'primary_city_id': 'int',
        'radius': 'float',
        'region_id': 'int',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


