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

class CRMAddress(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isCRMAddress = True
        super(CRMAddress, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        city = 'city'
        cnpj_tax_id = 'cnpj_tax_id'
        country = 'country'
        id = 'id'
        postal_code = 'postal_code'
        registration_label = 'registration_label'
        registration_number = 'registration_number'
        state = 'state'
        street1 = 'street1'
        street2 = 'street2'
        street3 = 'street3'
        street4 = 'street4'
        validation_status = 'validation_status'
        vat_tax_id = 'vat_tax_id'

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
            target_class=CRMAddress,
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
        'city': 'string',
        'cnpj_tax_id': 'string',
        'country': 'string',
        'id': 'string',
        'postal_code': 'string',
        'registration_label': 'string',
        'registration_number': 'string',
        'state': 'string',
        'street1': 'string',
        'street2': 'string',
        'street3': 'string',
        'street4': 'string',
        'validation_status': 'string',
        'vat_tax_id': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


