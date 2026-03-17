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

class DirectDebit(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isDirectDebit = True
        super(DirectDebit, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        bank_account_last_4 = 'bank_account_last_4'
        bank_code_last_4 = 'bank_code_last_4'
        bank_name = 'bank_name'
        default_receiving_method_products = 'default_receiving_method_products'
        display_string = 'display_string'
        id = 'id'
        last_four_digits = 'last_four_digits'
        onboarding_url = 'onboarding_url'
        owner_name = 'owner_name'
        status = 'status'

    _field_types = {
        'bank_account_last_4': 'string',
        'bank_code_last_4': 'string',
        'bank_name': 'string',
        'default_receiving_method_products': 'list<string>',
        'display_string': 'string',
        'id': 'string',
        'last_four_digits': 'string',
        'onboarding_url': 'string',
        'owner_name': 'string',
        'status': 'int',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


