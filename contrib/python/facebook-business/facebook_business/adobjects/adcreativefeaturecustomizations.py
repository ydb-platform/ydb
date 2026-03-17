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

class AdCreativeFeatureCustomizations(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdCreativeFeatureCustomizations, self).__init__()
        self._isAdCreativeFeatureCustomizations = True
        self._api = api

    class Field(AbstractObject.Field):
        aspect_ratio_config = 'aspect_ratio_config'
        background_color = 'background_color'
        catalog_feed_tag_name = 'catalog_feed_tag_name'
        font_name = 'font_name'
        image_crop_style = 'image_crop_style'
        pe_carousel = 'pe_carousel'
        showcase_card_display = 'showcase_card_display'
        text_extraction = 'text_extraction'
        text_style = 'text_style'

    _field_types = {
        'aspect_ratio_config': 'Object',
        'background_color': 'string',
        'catalog_feed_tag_name': 'string',
        'font_name': 'string',
        'image_crop_style': 'string',
        'pe_carousel': 'Object',
        'showcase_card_display': 'string',
        'text_extraction': 'Object',
        'text_style': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


