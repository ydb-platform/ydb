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

class Transaction(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isTransaction = True
        super(Transaction, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        account_id = 'account_id'
        app_amount = 'app_amount'
        billing_end_time = 'billing_end_time'
        billing_reason = 'billing_reason'
        billing_start_time = 'billing_start_time'
        card_charge_mode = 'card_charge_mode'
        charge_type = 'charge_type'
        checkout_campaign_group_id = 'checkout_campaign_group_id'
        credential_id = 'credential_id'
        fatura_id = 'fatura_id'
        id = 'id'
        is_business_ec_charge = 'is_business_ec_charge'
        is_funding_event = 'is_funding_event'
        payment_option = 'payment_option'
        product_type = 'product_type'
        provider_amount = 'provider_amount'
        status = 'status'
        time = 'time'
        tracking_id = 'tracking_id'
        transaction_type = 'transaction_type'
        tx_type = 'tx_type'
        vat_invoice_id = 'vat_invoice_id'

    class ProductType:
        cp_return_label = 'cp_return_label'
        facebook_ad = 'facebook_ad'
        ig_ad = 'ig_ad'
        whatsapp = 'whatsapp'
        workplace = 'workplace'

    # @deprecated get_endpoint function is deprecated
    @classmethod
    def get_endpoint(cls):
        return 'transactions'

    _field_types = {
        'account_id': 'string',
        'app_amount': 'Object',
        'billing_end_time': 'unsigned int',
        'billing_reason': 'string',
        'billing_start_time': 'unsigned int',
        'card_charge_mode': 'unsigned int',
        'charge_type': 'string',
        'checkout_campaign_group_id': 'string',
        'credential_id': 'string',
        'fatura_id': 'unsigned int',
        'id': 'string',
        'is_business_ec_charge': 'bool',
        'is_funding_event': 'bool',
        'payment_option': 'string',
        'product_type': 'ProductType',
        'provider_amount': 'Object',
        'status': 'string',
        'time': 'unsigned int',
        'tracking_id': 'string',
        'transaction_type': 'string',
        'tx_type': 'unsigned int',
        'vat_invoice_id': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['ProductType'] = Transaction.ProductType.__dict__.values()
        return field_enum_info


