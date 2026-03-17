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

class AdCreativeVideoDataCustomOverlaySpec(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdCreativeVideoDataCustomOverlaySpec, self).__init__()
        self._isAdCreativeVideoDataCustomOverlaySpec = True
        self._api = api

    class Field(AbstractObject.Field):
        background_color = 'background_color'
        background_opacity = 'background_opacity'
        duration = 'duration'
        float_with_margin = 'float_with_margin'
        full_width = 'full_width'
        option = 'option'
        position = 'position'
        start = 'start'
        template = 'template'
        text_color = 'text_color'

    class BackgroundOpacity:
        half = 'half'
        solid = 'solid'

    class Option:
        bank_transfer = 'bank_transfer'
        boleto = 'boleto'
        cash_on_delivery = 'cash_on_delivery'
        discount_with_boleto = 'discount_with_boleto'
        fast_delivery = 'fast_delivery'
        free_shipping = 'free_shipping'
        home_delivery = 'home_delivery'
        inventory = 'inventory'
        pay_at_hotel = 'pay_at_hotel'
        pay_on_arrival = 'pay_on_arrival'

    class Position:
        middle_center = 'middle_center'
        middle_left = 'middle_left'
        middle_right = 'middle_right'
        top_center = 'top_center'
        top_left = 'top_left'
        top_right = 'top_right'

    class Template:
        rectangle_with_text = 'rectangle_with_text'

    _field_types = {
        'background_color': 'string',
        'background_opacity': 'BackgroundOpacity',
        'duration': 'int',
        'float_with_margin': 'bool',
        'full_width': 'bool',
        'option': 'Option',
        'position': 'Position',
        'start': 'int',
        'template': 'Template',
        'text_color': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['BackgroundOpacity'] = AdCreativeVideoDataCustomOverlaySpec.BackgroundOpacity.__dict__.values()
        field_enum_info['Option'] = AdCreativeVideoDataCustomOverlaySpec.Option.__dict__.values()
        field_enum_info['Position'] = AdCreativeVideoDataCustomOverlaySpec.Position.__dict__.values()
        field_enum_info['Template'] = AdCreativeVideoDataCustomOverlaySpec.Template.__dict__.values()
        return field_enum_info


