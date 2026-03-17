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

class AdCreativeLinkDataImageOverlaySpec(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdCreativeLinkDataImageOverlaySpec, self).__init__()
        self._isAdCreativeLinkDataImageOverlaySpec = True
        self._api = api

    class Field(AbstractObject.Field):
        custom_text_type = 'custom_text_type'
        float_with_margin = 'float_with_margin'
        overlay_template = 'overlay_template'
        position = 'position'
        text_font = 'text_font'
        text_template_tags = 'text_template_tags'
        text_type = 'text_type'
        theme_color = 'theme_color'

    class CustomTextType:
        free_shipping = 'free_shipping'
        popular = 'popular'
        sale = 'sale'

    class OverlayTemplate:
        circle_with_text = 'circle_with_text'
        pill_with_text = 'pill_with_text'
        triangle_with_text = 'triangle_with_text'

    class Position:
        bottom_left = 'bottom_left'
        bottom_right = 'bottom_right'
        top_left = 'top_left'
        top_right = 'top_right'

    class TextFont:
        droid_serif_regular = 'droid_serif_regular'
        dynads_hybrid_bold = 'dynads_hybrid_bold'
        lato_regular = 'lato_regular'
        noto_sans_regular = 'noto_sans_regular'
        nunito_sans_bold = 'nunito_sans_bold'
        open_sans_bold = 'open_sans_bold'
        open_sans_condensed_bold = 'open_sans_condensed_bold'
        pt_serif_bold = 'pt_serif_bold'
        roboto_condensed_regular = 'roboto_condensed_regular'
        roboto_medium = 'roboto_medium'

    class TextType:
        automated_personalize = 'automated_personalize'
        custom = 'custom'
        disclaimer = 'disclaimer'
        from_price = 'from_price'
        guest_rating = 'guest_rating'
        percentage_off = 'percentage_off'
        price = 'price'
        star_rating = 'star_rating'
        strikethrough_price = 'strikethrough_price'
        sustainable = 'sustainable'

    class ThemeColor:
        background_000000_text_ffffff = 'background_000000_text_ffffff'
        background_0090ff_text_ffffff = 'background_0090ff_text_ffffff'
        background_00af4c_text_ffffff = 'background_00af4c_text_ffffff'
        background_595959_text_ffffff = 'background_595959_text_ffffff'
        background_755dde_text_ffffff = 'background_755dde_text_ffffff'
        background_e50900_text_ffffff = 'background_e50900_text_ffffff'
        background_f23474_text_ffffff = 'background_f23474_text_ffffff'
        background_f78400_text_ffffff = 'background_f78400_text_ffffff'
        background_ffffff_text_000000 = 'background_ffffff_text_000000'
        background_ffffff_text_007ad0 = 'background_ffffff_text_007ad0'
        background_ffffff_text_009c2a = 'background_ffffff_text_009c2a'
        background_ffffff_text_646464 = 'background_ffffff_text_646464'
        background_ffffff_text_755dde = 'background_ffffff_text_755dde'
        background_ffffff_text_c91b00 = 'background_ffffff_text_c91b00'
        background_ffffff_text_f23474 = 'background_ffffff_text_f23474'
        background_ffffff_text_f78400 = 'background_ffffff_text_f78400'

    _field_types = {
        'custom_text_type': 'CustomTextType',
        'float_with_margin': 'bool',
        'overlay_template': 'OverlayTemplate',
        'position': 'Position',
        'text_font': 'TextFont',
        'text_template_tags': 'list<string>',
        'text_type': 'TextType',
        'theme_color': 'ThemeColor',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['CustomTextType'] = AdCreativeLinkDataImageOverlaySpec.CustomTextType.__dict__.values()
        field_enum_info['OverlayTemplate'] = AdCreativeLinkDataImageOverlaySpec.OverlayTemplate.__dict__.values()
        field_enum_info['Position'] = AdCreativeLinkDataImageOverlaySpec.Position.__dict__.values()
        field_enum_info['TextFont'] = AdCreativeLinkDataImageOverlaySpec.TextFont.__dict__.values()
        field_enum_info['TextType'] = AdCreativeLinkDataImageOverlaySpec.TextType.__dict__.values()
        field_enum_info['ThemeColor'] = AdCreativeLinkDataImageOverlaySpec.ThemeColor.__dict__.values()
        return field_enum_info


