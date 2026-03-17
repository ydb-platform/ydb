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

class BrandRequest(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isBrandRequest = True
        super(BrandRequest, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        ad_countries = 'ad_countries'
        additional_contacts = 'additional_contacts'
        approval_level = 'approval_level'
        cells = 'cells'
        countries = 'countries'
        deny_reason = 'deny_reason'
        end_time = 'end_time'
        estimated_reach = 'estimated_reach'
        id = 'id'
        is_multicell = 'is_multicell'
        locale = 'locale'
        max_age = 'max_age'
        min_age = 'min_age'
        questions = 'questions'
        region = 'region'
        request_status = 'request_status'
        review_date = 'review_date'
        start_time = 'start_time'
        status = 'status'
        submit_date = 'submit_date'
        total_budget = 'total_budget'

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
            target_class=BrandRequest,
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
        'ad_countries': 'list<string>',
        'additional_contacts': 'list<string>',
        'approval_level': 'unsigned int',
        'cells': 'list<Object>',
        'countries': 'list<string>',
        'deny_reason': 'string',
        'end_time': 'datetime',
        'estimated_reach': 'unsigned int',
        'id': 'string',
        'is_multicell': 'bool',
        'locale': 'string',
        'max_age': 'unsigned int',
        'min_age': 'unsigned int',
        'questions': 'list<Object>',
        'region': 'string',
        'request_status': 'string',
        'review_date': 'datetime',
        'start_time': 'datetime',
        'status': 'string',
        'submit_date': 'datetime',
        'total_budget': 'unsigned int',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


