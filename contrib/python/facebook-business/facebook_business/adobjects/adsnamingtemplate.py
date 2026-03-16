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

class AdsNamingTemplate(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isAdsNamingTemplate = True
        super(AdsNamingTemplate, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        api_fields = 'api_fields'
        api_version = 'api_version'
        field_order = 'field_order'
        id = 'id'
        level = 'level'
        separator = 'separator'
        template_version = 'template_version'
        user_defined_fields = 'user_defined_fields'
        value_separator = 'value_separator'

    class Level:
        adgroup = 'ADGROUP'
        ad_account = 'AD_ACCOUNT'
        campaign = 'CAMPAIGN'
        campaign_group = 'CAMPAIGN_GROUP'
        opportunities = 'OPPORTUNITIES'
        privacy_info_center = 'PRIVACY_INFO_CENTER'
        product = 'PRODUCT'
        topline = 'TOPLINE'
        unique_adcreative = 'UNIQUE_ADCREATIVE'

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
            target_class=AdsNamingTemplate,
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
        'api_fields': 'list<list<map<string, list<map<string, string>>>>>',
        'api_version': 'string',
        'field_order': 'list<string>',
        'id': 'string',
        'level': 'Level',
        'separator': 'string',
        'template_version': 'string',
        'user_defined_fields': 'list<list<map<string, list<string>>>>',
        'value_separator': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['Level'] = AdsNamingTemplate.Level.__dict__.values()
        return field_enum_info


