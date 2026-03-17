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

class TargetingGeoLocationPlace(
    AbstractObject,
):

    def __init__(self, api=None):
        super(TargetingGeoLocationPlace, self).__init__()
        self._isTargetingGeoLocationPlace = True
        self._api = api

    class Field(AbstractObject.Field):
        country = 'country'
        distance_unit = 'distance_unit'
        key = 'key'
        latitude = 'latitude'
        longitude = 'longitude'
        name = 'name'
        primary_city_id = 'primary_city_id'
        radius = 'radius'
        region_id = 'region_id'

    _field_types = {
        'country': 'string',
        'distance_unit': 'string',
        'key': 'string',
        'latitude': 'float',
        'longitude': 'float',
        'name': 'string',
        'primary_city_id': 'int',
        'radius': 'float',
        'region_id': 'int',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


