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

class AdCreativeCollectionThumbnailInfo(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdCreativeCollectionThumbnailInfo, self).__init__()
        self._isAdCreativeCollectionThumbnailInfo = True
        self._api = api

    class Field(AbstractObject.Field):
        element_child_index = 'element_child_index'
        element_crops = 'element_crops'
        element_id = 'element_id'

    _field_types = {
        'element_child_index': 'int',
        'element_crops': 'AdsImageCrops',
        'element_id': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


