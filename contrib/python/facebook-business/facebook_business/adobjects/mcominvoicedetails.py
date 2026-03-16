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

class McomInvoiceDetails(
    AbstractObject,
):

    def __init__(self, api=None):
        super(McomInvoiceDetails, self).__init__()
        self._isMcomInvoiceDetails = True
        self._api = api

    class Field(AbstractObject.Field):
        additional_amounts = 'additional_amounts'
        buyer_notes = 'buyer_notes'
        currency_amount = 'currency_amount'
        external_invoice_id = 'external_invoice_id'
        features = 'features'
        invoice_created = 'invoice_created'
        invoice_id = 'invoice_id'
        invoice_instructions = 'invoice_instructions'
        invoice_instructions_image_url = 'invoice_instructions_image_url'
        invoice_updated = 'invoice_updated'
        outstanding_amount = 'outstanding_amount'
        paid_amount = 'paid_amount'
        payments = 'payments'
        platform_logo_url = 'platform_logo_url'
        platform_name = 'platform_name'
        product_items = 'product_items'
        shipping_address = 'shipping_address'
        status = 'status'
        tracking_info = 'tracking_info'

    _field_types = {
        'additional_amounts': 'list<Object>',
        'buyer_notes': 'string',
        'currency_amount': 'Object',
        'external_invoice_id': 'string',
        'features': 'Object',
        'invoice_created': 'int',
        'invoice_id': 'string',
        'invoice_instructions': 'string',
        'invoice_instructions_image_url': 'string',
        'invoice_updated': 'int',
        'outstanding_amount': 'Object',
        'paid_amount': 'Object',
        'payments': 'list<Object>',
        'platform_logo_url': 'string',
        'platform_name': 'string',
        'product_items': 'list<Object>',
        'shipping_address': 'Object',
        'status': 'string',
        'tracking_info': 'Object',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


