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

class AdAssetFeedSpecImage(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdAssetFeedSpecImage, self).__init__()
        self._isAdAssetFeedSpecImage = True
        self._api = api

    class Field(AbstractObject.Field):
        adlabels = 'adlabels'
        hash = 'hash'
        image_crops = 'image_crops'
        url = 'url'
        url_tags = 'url_tags'

    _field_types = {
        'adlabels': 'list<AdAssetFeedSpecAssetLabel>',
        'hash': 'string',
        'image_crops': 'AdsImageCrops',
        'url': 'string',
        'url_tags': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


