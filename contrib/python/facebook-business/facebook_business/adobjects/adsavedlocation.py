# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

from facebook_business.adobjects.abstractobject import AbstractObject
from facebook_business.adobjects.abstractcrudobject import AbstractCrudObject
from facebook_business.adobjects.objectparser import ObjectParser
from facebook_business.api import FacebookRequest
from facebook_business.typechecker import TypeChecker

"""
This class is auto-generated.

For any issues or feature requests related to this class, please let us know on
github and we'll fix in our codegen framework. We'll not be able to accept
pull request for this class.
"""

class AdSavedLocation(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isAdSavedLocation = True
        super(AdSavedLocation, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        cities = 'cities'
        countries = 'countries'
        country_groups = 'country_groups'
        custom_locations = 'custom_locations'
        geo_markets = 'geo_markets'
        id = 'id'
        location_sentences = 'location_sentences'
        name = 'name'
        regions = 'regions'
        zips = 'zips'

    def api_get(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdSavedLocation,
            api_type='NODE',
            response_parser=ObjectParser(reuse_object=self),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    _field_types = {
        'cities': 'list<TargetingGeoLocationCity>',
        'countries': 'list<string>',
        'country_groups': 'list<string>',
        'custom_locations': 'list<TargetingGeoLocationCustomLocation>',
        'geo_markets': 'list<TargetingGeoLocationMarket>',
        'id': 'string',
        'location_sentences': 'list<string>',
        'name': 'string',
        'regions': 'list<TargetingGeoLocationRegion>',
        'zips': 'list<TargetingGeoLocationZip>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


