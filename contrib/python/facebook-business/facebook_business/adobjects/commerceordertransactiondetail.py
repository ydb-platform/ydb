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

class CommerceOrderTransactionDetail(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isCommerceOrderTransactionDetail = True
        super(CommerceOrderTransactionDetail, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        merchant_order_id = 'merchant_order_id'
        net_payment_amount = 'net_payment_amount'
        order_created = 'order_created'
        order_details = 'order_details'
        order_id = 'order_id'
        payout_reference_id = 'payout_reference_id'
        postal_code = 'postal_code'
        processing_fee = 'processing_fee'
        state = 'state'
        tax_rate = 'tax_rate'
        transaction_date = 'transaction_date'
        transaction_type = 'transaction_type'
        transfer_id = 'transfer_id'
        id = 'id'

    def get_items(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
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
            endpoint='/items',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AbstractCrudObject,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AbstractCrudObject, api=self._api),
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

    def get_tax_details(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
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
            endpoint='/tax_details',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AbstractCrudObject,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AbstractCrudObject, api=self._api),
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
        'merchant_order_id': 'string',
        'net_payment_amount': 'Object',
        'order_created': 'string',
        'order_details': 'CommerceOrder',
        'order_id': 'string',
        'payout_reference_id': 'string',
        'postal_code': 'string',
        'processing_fee': 'Object',
        'state': 'string',
        'tax_rate': 'string',
        'transaction_date': 'string',
        'transaction_type': 'string',
        'transfer_id': 'string',
        'id': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


