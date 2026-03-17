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

class AdCreativeFacebookBrandedContent(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdCreativeFacebookBrandedContent, self).__init__()
        self._isAdCreativeFacebookBrandedContent = True
        self._api = api

    class Field(AbstractObject.Field):
        shared_to_sponsor_status = 'shared_to_sponsor_status'
        sponsor_page_id = 'sponsor_page_id'
        sponsor_relationship = 'sponsor_relationship'

    _field_types = {
        'shared_to_sponsor_status': 'string',
        'sponsor_page_id': 'string',
        'sponsor_relationship': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


