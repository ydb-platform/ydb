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

class AdAssetFeedSpecCarousel(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdAssetFeedSpecCarousel, self).__init__()
        self._isAdAssetFeedSpecCarousel = True
        self._api = api

    class Field(AbstractObject.Field):
        adlabels = 'adlabels'
        child_attachments = 'child_attachments'
        multi_share_end_card = 'multi_share_end_card'
        multi_share_optimized = 'multi_share_optimized'

    _field_types = {
        'adlabels': 'list<AdAssetFeedSpecAssetLabel>',
        'child_attachments': 'list<AdAssetFeedSpecCarouselChildAttachment>',
        'multi_share_end_card': 'bool',
        'multi_share_optimized': 'bool',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


