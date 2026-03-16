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

class LeadGenThankYouPage(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isLeadGenThankYouPage = True
        super(LeadGenThankYouPage, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        body = 'body'
        business_phone_number = 'business_phone_number'
        button_text = 'button_text'
        button_type = 'button_type'
        country_code = 'country_code'
        enable_messenger = 'enable_messenger'
        gated_file = 'gated_file'
        id = 'id'
        lead_gen_use_case = 'lead_gen_use_case'
        status = 'status'
        title = 'title'
        website_url = 'website_url'

    _field_types = {
        'body': 'string',
        'business_phone_number': 'string',
        'button_text': 'string',
        'button_type': 'string',
        'country_code': 'string',
        'enable_messenger': 'bool',
        'gated_file': 'LeadGenThankYouPageGatedFile',
        'id': 'string',
        'lead_gen_use_case': 'string',
        'status': 'string',
        'title': 'string',
        'website_url': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


