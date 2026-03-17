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

class AdCreativeStaticFallbackSpec(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdCreativeStaticFallbackSpec, self).__init__()
        self._isAdCreativeStaticFallbackSpec = True
        self._api = api

    class Field(AbstractObject.Field):
        call_to_action = 'call_to_action'
        description = 'description'
        image_hash = 'image_hash'
        link = 'link'
        message = 'message'
        name = 'name'

    _field_types = {
        'call_to_action': 'AdCreativeLinkDataCallToAction',
        'description': 'string',
        'image_hash': 'string',
        'link': 'string',
        'message': 'string',
        'name': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


