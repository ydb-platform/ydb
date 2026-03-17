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

class CTWAWhatsAppNumbersInfo(
    AbstractObject,
):

    def __init__(self, api=None):
        super(CTWAWhatsAppNumbersInfo, self).__init__()
        self._isCTWAWhatsAppNumbersInfo = True
        self._api = api

    class Field(AbstractObject.Field):
        can_manage_wa_flows = 'can_manage_wa_flows'
        formatted_whatsapp_number = 'formatted_whatsapp_number'
        is_business_number = 'is_business_number'
        is_calling_enabled = 'is_calling_enabled'
        number_country_prefix = 'number_country_prefix'
        page_whatsapp_number_id = 'page_whatsapp_number_id'
        waba_id = 'waba_id'
        whatsapp_number = 'whatsapp_number'
        whatsapp_smb_device = 'whatsapp_smb_device'

    _field_types = {
        'can_manage_wa_flows': 'bool',
        'formatted_whatsapp_number': 'string',
        'is_business_number': 'bool',
        'is_calling_enabled': 'bool',
        'number_country_prefix': 'string',
        'page_whatsapp_number_id': 'string',
        'waba_id': 'string',
        'whatsapp_number': 'string',
        'whatsapp_smb_device': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


