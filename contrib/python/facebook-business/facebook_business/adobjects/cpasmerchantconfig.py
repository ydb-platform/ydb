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

class CPASMerchantConfig(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isCPASMerchantConfig = True
        super(CPASMerchantConfig, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        accepted_tos = 'accepted_tos'
        beta_features = 'beta_features'
        business_outcomes_status = 'business_outcomes_status'
        id = 'id'
        is_test_merchant = 'is_test_merchant'
        outcomes_compliance_status = 'outcomes_compliance_status'
        qualified_to_onboard = 'qualified_to_onboard'

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
            target_class=CPASMerchantConfig,
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
        'accepted_tos': 'bool',
        'beta_features': 'list<string>',
        'business_outcomes_status': 'list<map<string, string>>',
        'id': 'string',
        'is_test_merchant': 'bool',
        'outcomes_compliance_status': 'list<map<string, Object>>',
        'qualified_to_onboard': 'bool',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


