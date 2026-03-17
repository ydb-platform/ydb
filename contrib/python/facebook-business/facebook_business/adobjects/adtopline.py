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

class AdTopline(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isAdTopline = True
        super(AdTopline, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        account_id = 'account_id'
        client_approval_date = 'client_approval_date'
        created_by = 'created_by'
        created_date = 'created_date'
        description = 'description'
        flight_end_date = 'flight_end_date'
        flight_start_date = 'flight_start_date'
        func_cap_amount = 'func_cap_amount'
        func_cap_amount_with_offset = 'func_cap_amount_with_offset'
        func_line_amount = 'func_line_amount'
        func_line_amount_with_offset = 'func_line_amount_with_offset'
        func_price = 'func_price'
        func_price_with_offset = 'func_price_with_offset'
        gender = 'gender'
        id = 'id'
        impressions = 'impressions'
        io_number = 'io_number'
        is_bonus_line = 'is_bonus_line'
        keywords = 'keywords'
        last_updated_by = 'last_updated_by'
        last_updated_date = 'last_updated_date'
        line_number = 'line_number'
        line_position = 'line_position'
        line_type = 'line_type'
        location = 'location'
        max_age = 'max_age'
        max_budget = 'max_budget'
        min_age = 'min_age'
        price_per_trp = 'price_per_trp'
        product_type = 'product_type'
        rev_assurance_approval_date = 'rev_assurance_approval_date'
        targets = 'targets'
        trp_updated_time = 'trp_updated_time'
        trp_value = 'trp_value'
        uom = 'uom'

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
            target_class=AdTopline,
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
        'account_id': 'string',
        'client_approval_date': 'datetime',
        'created_by': 'string',
        'created_date': 'datetime',
        'description': 'string',
        'flight_end_date': 'datetime',
        'flight_start_date': 'datetime',
        'func_cap_amount': 'string',
        'func_cap_amount_with_offset': 'string',
        'func_line_amount': 'string',
        'func_line_amount_with_offset': 'string',
        'func_price': 'string',
        'func_price_with_offset': 'string',
        'gender': 'string',
        'id': 'string',
        'impressions': 'int',
        'io_number': 'int',
        'is_bonus_line': 'int',
        'keywords': 'string',
        'last_updated_by': 'string',
        'last_updated_date': 'datetime',
        'line_number': 'int',
        'line_position': 'int',
        'line_type': 'string',
        'location': 'string',
        'max_age': 'string',
        'max_budget': 'string',
        'min_age': 'string',
        'price_per_trp': 'string',
        'product_type': 'string',
        'rev_assurance_approval_date': 'datetime',
        'targets': 'string',
        'trp_updated_time': 'int',
        'trp_value': 'string',
        'uom': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


