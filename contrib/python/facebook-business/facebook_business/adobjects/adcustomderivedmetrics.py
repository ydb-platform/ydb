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

class AdCustomDerivedMetrics(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isAdCustomDerivedMetrics = True
        super(AdCustomDerivedMetrics, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        ad_account_id = 'ad_account_id'
        business = 'business'
        creation_time = 'creation_time'
        creator = 'creator'
        custom_derived_metric_type = 'custom_derived_metric_type'
        deletion_time = 'deletion_time'
        deletor = 'deletor'
        description = 'description'
        format_type = 'format_type'
        formula = 'formula'
        has_attribution_windows = 'has_attribution_windows'
        has_inline_attribution_window = 'has_inline_attribution_window'
        id = 'id'
        name = 'name'
        permission = 'permission'
        saved_report_id = 'saved_report_id'
        scope = 'scope'

    class Scope:
        account = 'ACCOUNT'
        business = 'BUSINESS'
        business_asset_group = 'BUSINESS_ASSET_GROUP'

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
            target_class=AdCustomDerivedMetrics,
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
        'ad_account_id': 'string',
        'business': 'Business',
        'creation_time': 'datetime',
        'creator': 'Profile',
        'custom_derived_metric_type': 'string',
        'deletion_time': 'datetime',
        'deletor': 'Profile',
        'description': 'string',
        'format_type': 'string',
        'formula': 'string',
        'has_attribution_windows': 'bool',
        'has_inline_attribution_window': 'bool',
        'id': 'string',
        'name': 'string',
        'permission': 'string',
        'saved_report_id': 'string',
        'scope': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['Scope'] = AdCustomDerivedMetrics.Scope.__dict__.values()
        return field_enum_info


