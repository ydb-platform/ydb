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

class CatalogSmartPixelSettings(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isCatalogSmartPixelSettings = True
        super(CatalogSmartPixelSettings, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        allowed_domains = 'allowed_domains'
        available_property_filters = 'available_property_filters'
        catalog = 'catalog'
        cbb_custom_override_filters = 'cbb_custom_override_filters'
        cbb_default_filter = 'cbb_default_filter'
        defaults = 'defaults'
        filters = 'filters'
        id = 'id'
        is_cbb_enabled = 'is_cbb_enabled'
        is_create_enabled = 'is_create_enabled'
        is_delete_enabled = 'is_delete_enabled'
        is_update_enabled = 'is_update_enabled'
        microdata_format_precedence = 'microdata_format_precedence'
        pixel = 'pixel'
        property_filter = 'property_filter'
        trusted_domains = 'trusted_domains'

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
            target_class=CatalogSmartPixelSettings,
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
        'allowed_domains': 'list<string>',
        'available_property_filters': 'list<string>',
        'catalog': 'ProductCatalog',
        'cbb_custom_override_filters': 'list<Object>',
        'cbb_default_filter': 'list<map<string, list<string>>>',
        'defaults': 'list<map<string, string>>',
        'filters': 'list<map<string, list<string>>>',
        'id': 'string',
        'is_cbb_enabled': 'bool',
        'is_create_enabled': 'bool',
        'is_delete_enabled': 'bool',
        'is_update_enabled': 'bool',
        'microdata_format_precedence': 'list<string>',
        'pixel': 'AdsPixel',
        'property_filter': 'list<string>',
        'trusted_domains': 'list<string>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


