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

class AdAssetFeedSpecVideo(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdAssetFeedSpecVideo, self).__init__()
        self._isAdAssetFeedSpecVideo = True
        self._api = api

    class Field(AbstractObject.Field):
        adlabels = 'adlabels'
        caption_ids = 'caption_ids'
        thumbnail_hash = 'thumbnail_hash'
        thumbnail_url = 'thumbnail_url'
        url_tags = 'url_tags'
        video_id = 'video_id'

    _field_types = {
        'adlabels': 'list<AdAssetFeedSpecAssetLabel>',
        'caption_ids': 'list<string>',
        'thumbnail_hash': 'string',
        'thumbnail_url': 'string',
        'url_tags': 'string',
        'video_id': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


