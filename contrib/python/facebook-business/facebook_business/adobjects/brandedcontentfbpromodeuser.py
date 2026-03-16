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

class BrandedContentFBPromodeUser(
    AbstractObject,
):

    def __init__(self, api=None):
        super(BrandedContentFBPromodeUser, self).__init__()
        self._isBrandedContentFBPromodeUser = True
        self._api = api

    class Field(AbstractObject.Field):
        delegate_page_for_ads_only_id = 'delegate_page_for_ads_only_id'
        is_iabp = 'is_iabp'
        is_managed = 'is_managed'
        name = 'name'
        profile_picture_url = 'profile_picture_url'

    _field_types = {
        'delegate_page_for_ads_only_id': 'string',
        'is_iabp': 'bool',
        'is_managed': 'bool',
        'name': 'string',
        'profile_picture_url': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


