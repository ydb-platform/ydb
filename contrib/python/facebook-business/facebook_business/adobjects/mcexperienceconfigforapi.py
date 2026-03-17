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

class MCExperienceConfigForApi(
    AbstractObject,
):

    def __init__(self, api=None):
        super(MCExperienceConfigForApi, self).__init__()
        self._isMCExperienceConfigForApi = True
        self._api = api

    class Field(AbstractObject.Field):
        is_campaign_enabled = 'is_campaign_enabled'
        is_terms_signed = 'is_terms_signed'
        is_user_manually_toggle_mc_off = 'is_user_manually_toggle_mc_off'
        merchant_type = 'merchant_type'

    _field_types = {
        'is_campaign_enabled': 'bool',
        'is_terms_signed': 'bool',
        'is_user_manually_toggle_mc_off': 'bool',
        'merchant_type': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


