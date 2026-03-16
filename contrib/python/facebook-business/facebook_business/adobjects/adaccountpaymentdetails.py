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

class AdAccountPaymentDetails(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isAdAccountPaymentDetails = True
        super(AdAccountPaymentDetails, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        amount = 'amount'
        create_date = 'create_date'
        id = 'id'
        last_action_status = 'last_action_status'
        metadata = 'metadata'
        payment_details_id = 'payment_details_id'

    _field_types = {
        'amount': 'CurrencyAmount',
        'create_date': 'int',
        'id': 'string',
        'last_action_status': 'string',
        'metadata': 'Object',
        'payment_details_id': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


