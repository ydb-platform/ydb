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

class ExtendedCreditApplication(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isExtendedCreditApplication = True
        super(ExtendedCreditApplication, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        billing_country = 'billing_country'
        city = 'city'
        cnpj = 'cnpj'
        country = 'country'
        display_currency = 'display_currency'
        duns_number = 'duns_number'
        id = 'id'
        invoice_email_address = 'invoice_email_address'
        is_umi = 'is_umi'
        legal_entity_name = 'legal_entity_name'
        original_online_limit = 'original_online_limit'
        phone_number = 'phone_number'
        postal_code = 'postal_code'
        product_types = 'product_types'
        proposed_credit_limit = 'proposed_credit_limit'
        registration_number = 'registration_number'
        run_id = 'run_id'
        state = 'state'
        status = 'status'
        street1 = 'street1'
        street2 = 'street2'
        submitter = 'submitter'
        tax_exempt_status = 'tax_exempt_status'
        tax_id = 'tax_id'
        terms = 'terms'

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
            target_class=ExtendedCreditApplication,
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
        'billing_country': 'string',
        'city': 'string',
        'cnpj': 'string',
        'country': 'string',
        'display_currency': 'string',
        'duns_number': 'string',
        'id': 'string',
        'invoice_email_address': 'string',
        'is_umi': 'bool',
        'legal_entity_name': 'string',
        'original_online_limit': 'CurrencyAmount',
        'phone_number': 'string',
        'postal_code': 'string',
        'product_types': 'list<string>',
        'proposed_credit_limit': 'CurrencyAmount',
        'registration_number': 'string',
        'run_id': 'string',
        'state': 'string',
        'status': 'string',
        'street1': 'string',
        'street2': 'string',
        'submitter': 'User',
        'tax_exempt_status': 'string',
        'tax_id': 'string',
        'terms': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


