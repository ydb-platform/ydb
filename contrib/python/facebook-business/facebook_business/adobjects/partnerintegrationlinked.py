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

class PartnerIntegrationLinked(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isPartnerIntegrationLinked = True
        super(PartnerIntegrationLinked, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        ads_pixel = 'ads_pixel'
        application = 'application'
        completed_integration_types = 'completed_integration_types'
        external_business_connection_id = 'external_business_connection_id'
        external_id = 'external_id'
        has_oauth_token = 'has_oauth_token'
        id = 'id'
        mbe_app_id = 'mbe_app_id'
        mbe_asset_id = 'mbe_asset_id'
        mbe_external_business_id = 'mbe_external_business_id'
        name = 'name'
        offline_conversion_data_set = 'offline_conversion_data_set'
        page = 'page'
        partner = 'partner'
        product_catalog = 'product_catalog'
        setup_status = 'setup_status'

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
            target_class=PartnerIntegrationLinked,
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
        'ads_pixel': 'AdsPixel',
        'application': 'Application',
        'completed_integration_types': 'list<string>',
        'external_business_connection_id': 'string',
        'external_id': 'string',
        'has_oauth_token': 'bool',
        'id': 'string',
        'mbe_app_id': 'string',
        'mbe_asset_id': 'string',
        'mbe_external_business_id': 'string',
        'name': 'string',
        'offline_conversion_data_set': 'OfflineConversionDataSet',
        'page': 'Page',
        'partner': 'string',
        'product_catalog': 'ProductCatalog',
        'setup_status': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


