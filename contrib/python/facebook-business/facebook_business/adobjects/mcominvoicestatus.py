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

class McomInvoiceStatus(
    AbstractObject,
):

    def __init__(self, api=None):
        super(McomInvoiceStatus, self).__init__()
        self._isMcomInvoiceStatus = True
        self._api = api

    class Field(AbstractObject.Field):
        bank_account_number = 'bank_account_number'
        bank_code = 'bank_code'
        invoice_id = 'invoice_id'
        invoice_status = 'invoice_status'
        page_id = 'page_id'
        payment_method = 'payment_method'
        payment_type = 'payment_type'
        payout_amount = 'payout_amount'
        slip_verification_error = 'slip_verification_error'
        slip_verification_status = 'slip_verification_status'
        sof_transfer_id = 'sof_transfer_id'
        sof_transfer_timestamp = 'sof_transfer_timestamp'
        transaction_fee = 'transaction_fee'
        transfer_slip = 'transfer_slip'
        transfer_slip_qr_code = 'transfer_slip_qr_code'

    _field_types = {
        'bank_account_number': 'string',
        'bank_code': 'string',
        'invoice_id': 'string',
        'invoice_status': 'string',
        'page_id': 'string',
        'payment_method': 'string',
        'payment_type': 'string',
        'payout_amount': 'Object',
        'slip_verification_error': 'string',
        'slip_verification_status': 'string',
        'sof_transfer_id': 'string',
        'sof_transfer_timestamp': 'int',
        'transaction_fee': 'Object',
        'transfer_slip': 'string',
        'transfer_slip_qr_code': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


