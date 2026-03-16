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

class TargetingGeoLocation(
    AbstractObject,
):

    def __init__(self, api=None):
        super(TargetingGeoLocation, self).__init__()
        self._isTargetingGeoLocation = True
        self._api = api

    class Field(AbstractObject.Field):
        cities = 'cities'
        countries = 'countries'
        country_groups = 'country_groups'
        custom_locations = 'custom_locations'
        electoral_districts = 'electoral_districts'
        geo_markets = 'geo_markets'
        large_geo_areas = 'large_geo_areas'
        location_cluster_ids = 'location_cluster_ids'
        location_types = 'location_types'
        medium_geo_areas = 'medium_geo_areas'
        metro_areas = 'metro_areas'
        neighborhoods = 'neighborhoods'
        places = 'places'
        political_districts = 'political_districts'
        regions = 'regions'
        small_geo_areas = 'small_geo_areas'
        subcities = 'subcities'
        subneighborhoods = 'subneighborhoods'
        zips = 'zips'

    _field_types = {
        'cities': 'list<TargetingGeoLocationCity>',
        'countries': 'list<string>',
        'country_groups': 'list<string>',
        'custom_locations': 'list<TargetingGeoLocationCustomLocation>',
        'electoral_districts': 'list<TargetingGeoLocationElectoralDistrict>',
        'geo_markets': 'list<TargetingGeoLocationMarket>',
        'large_geo_areas': 'list<TargetingGeoLocationGeoEntities>',
        'location_cluster_ids': 'list<TargetingGeoLocationLocationCluster>',
        'location_types': 'list<string>',
        'medium_geo_areas': 'list<TargetingGeoLocationGeoEntities>',
        'metro_areas': 'list<TargetingGeoLocationGeoEntities>',
        'neighborhoods': 'list<TargetingGeoLocationGeoEntities>',
        'places': 'list<TargetingGeoLocationPlace>',
        'political_districts': 'list<TargetingGeoLocationPoliticalDistrict>',
        'regions': 'list<TargetingGeoLocationRegion>',
        'small_geo_areas': 'list<TargetingGeoLocationGeoEntities>',
        'subcities': 'list<TargetingGeoLocationGeoEntities>',
        'subneighborhoods': 'list<TargetingGeoLocationGeoEntities>',
        'zips': 'list<TargetingGeoLocationZip>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


