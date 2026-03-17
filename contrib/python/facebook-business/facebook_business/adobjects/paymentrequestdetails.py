# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

from facebook_business.adobjects.abstractobject import AbstractObject

"""
This class is auto-generated.

For any issues or feature requests related to this class, please let us know on
github and we'll fix in our codegen framework. We'll not be able to accept
pull request for this class.
"""

class PaymentRequestDetails(
    AbstractObject,
):

    def __init__(self, api=None):
        super(PaymentRequestDetails, self).__init__()
        self._isPaymentRequestDetails = True
        self._api = api

    class Field(AbstractObject.Field):
        amount = 'amount'
        creation_time = 'creation_time'
        note = 'note'
        payment_request_id = 'payment_request_id'
        receiver_id = 'receiver_id'
        reference_number = 'reference_number'
        sender_id = 'sender_id'
        status = 'status'
        transaction_time = 'transaction_time'

    _field_types = {
        'amount': 'Object',
        'creation_time': 'int',
        'note': 'string',
        'payment_request_id': 'string',
        'receiver_id': 'string',
        'reference_number': 'string',
        'sender_id': 'string',
        'status': 'string',
        'transaction_time': 'int',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


