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

class AdgroupMetadata(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdgroupMetadata, self).__init__()
        self._isAdgroupMetadata = True
        self._api = api

    class Field(AbstractObject.Field):
        ad_standard_enhancements_edit_source = 'ad_standard_enhancements_edit_source'
        adgroup_creation_source = 'adgroup_creation_source'
        adgroup_edit_source = 'adgroup_edit_source'
        adgroup_media_source = 'adgroup_media_source'
        carousel_style = 'carousel_style'
        carousel_with_static_card_style = 'carousel_with_static_card_style'

    _field_types = {
        'ad_standard_enhancements_edit_source': 'int',
        'adgroup_creation_source': 'string',
        'adgroup_edit_source': 'string',
        'adgroup_media_source': 'string',
        'carousel_style': 'string',
        'carousel_with_static_card_style': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


