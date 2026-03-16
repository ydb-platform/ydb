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

class PartnershipAdsIdentity(
    AbstractObject,
):

    def __init__(self, api=None):
        super(PartnershipAdsIdentity, self).__init__()
        self._isPartnershipAdsIdentity = True
        self._api = api

    class Field(AbstractObject.Field):
        is_recommended = 'is_recommended'
        is_saved = 'is_saved'
        post_types = 'post_types'
        secondary_identities = 'secondary_identities'

    _field_types = {
        'is_recommended': 'bool',
        'is_saved': 'bool',
        'post_types': 'list<string>',
        'secondary_identities': 'list<Object>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


