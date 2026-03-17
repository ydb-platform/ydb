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

class CreditCard(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isCreditCard = True
        super(CreditCard, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        billing_address = 'billing_address'
        card_cobadging = 'card_cobadging'
        card_holder_name = 'card_holder_name'
        card_type = 'card_type'
        credential_id = 'credential_id'
        default_receiving_method_products = 'default_receiving_method_products'
        expiry_month = 'expiry_month'
        expiry_year = 'expiry_year'
        id = 'id'
        is_cvv_tricky_bin = 'is_cvv_tricky_bin'
        is_enabled = 'is_enabled'
        is_last_used = 'is_last_used'
        is_network_tokenized_in_india = 'is_network_tokenized_in_india'
        is_soft_disabled = 'is_soft_disabled'
        is_user_verified = 'is_user_verified'
        is_zip_verified = 'is_zip_verified'
        last4 = 'last4'
        readable_card_type = 'readable_card_type'
        time_created = 'time_created'
        time_created_ts = 'time_created_ts'
        type = 'type'

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
            target_class=CreditCard,
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
        'billing_address': 'Object',
        'card_cobadging': 'string',
        'card_holder_name': 'string',
        'card_type': 'string',
        'credential_id': 'int',
        'default_receiving_method_products': 'list<string>',
        'expiry_month': 'string',
        'expiry_year': 'string',
        'id': 'string',
        'is_cvv_tricky_bin': 'bool',
        'is_enabled': 'bool',
        'is_last_used': 'bool',
        'is_network_tokenized_in_india': 'bool',
        'is_soft_disabled': 'bool',
        'is_user_verified': 'bool',
        'is_zip_verified': 'bool',
        'last4': 'string',
        'readable_card_type': 'string',
        'time_created': 'datetime',
        'time_created_ts': 'int',
        'type': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


