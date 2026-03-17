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

class PaymentSubscription(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isPaymentSubscription = True
        super(PaymentSubscription, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        amount = 'amount'
        app_param_data = 'app_param_data'
        application = 'application'
        billing_period = 'billing_period'
        canceled_reason = 'canceled_reason'
        created_time = 'created_time'
        currency = 'currency'
        id = 'id'
        last_payment = 'last_payment'
        next_bill_time = 'next_bill_time'
        next_period_amount = 'next_period_amount'
        next_period_currency = 'next_period_currency'
        next_period_product = 'next_period_product'
        payment_status = 'payment_status'
        pending_cancel = 'pending_cancel'
        period_start_time = 'period_start_time'
        product = 'product'
        status = 'status'
        test = 'test'
        trial_amount = 'trial_amount'
        trial_currency = 'trial_currency'
        trial_expiry_time = 'trial_expiry_time'
        updated_time = 'updated_time'
        user = 'user'

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
            target_class=PaymentSubscription,
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
        'amount': 'string',
        'app_param_data': 'string',
        'application': 'Application',
        'billing_period': 'string',
        'canceled_reason': 'string',
        'created_time': 'datetime',
        'currency': 'string',
        'id': 'string',
        'last_payment': 'PaymentEnginePayment',
        'next_bill_time': 'datetime',
        'next_period_amount': 'string',
        'next_period_currency': 'string',
        'next_period_product': 'string',
        'payment_status': 'string',
        'pending_cancel': 'bool',
        'period_start_time': 'datetime',
        'product': 'string',
        'status': 'string',
        'test': 'unsigned int',
        'trial_amount': 'string',
        'trial_currency': 'string',
        'trial_expiry_time': 'datetime',
        'updated_time': 'datetime',
        'user': 'User',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


