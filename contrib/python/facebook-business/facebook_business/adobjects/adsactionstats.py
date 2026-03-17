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

class AdsActionStats(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdsActionStats, self).__init__()
        self._isAdsActionStats = True
        self._api = api

    class Field(AbstractObject.Field):
        field_1d_click = '1d_click'
        field_1d_click_all_conversions = '1d_click_all_conversions'
        field_1d_click_first_conversion = '1d_click_first_conversion'
        field_1d_ev = '1d_ev'
        field_1d_ev_all_conversions = '1d_ev_all_conversions'
        field_1d_ev_first_conversion = '1d_ev_first_conversion'
        field_1d_passback = '1d_passback'
        field_1d_view = '1d_view'
        field_1d_view_all_conversions = '1d_view_all_conversions'
        field_1d_view_first_conversion = '1d_view_first_conversion'
        field_28d_click = '28d_click'
        field_28d_click_all_conversions = '28d_click_all_conversions'
        field_28d_click_first_conversion = '28d_click_first_conversion'
        field_28d_passback = '28d_passback'
        field_28d_view = '28d_view'
        field_28d_view_all_conversions = '28d_view_all_conversions'
        field_28d_view_first_conversion = '28d_view_first_conversion'
        field_7d_click = '7d_click'
        field_7d_click_all_conversions = '7d_click_all_conversions'
        field_7d_click_first_conversion = '7d_click_first_conversion'
        field_7d_passback = '7d_passback'
        field_7d_view = '7d_view'
        field_7d_view_all_conversions = '7d_view_all_conversions'
        field_7d_view_first_conversion = '7d_view_first_conversion'
        action_brand = 'action_brand'
        action_canvas_component_id = 'action_canvas_component_id'
        action_canvas_component_name = 'action_canvas_component_name'
        action_carousel_card_id = 'action_carousel_card_id'
        action_carousel_card_name = 'action_carousel_card_name'
        action_category = 'action_category'
        action_converted_product_id = 'action_converted_product_id'
        action_destination = 'action_destination'
        action_device = 'action_device'
        action_event_channel = 'action_event_channel'
        action_link_click_destination = 'action_link_click_destination'
        action_location_code = 'action_location_code'
        action_reaction = 'action_reaction'
        action_target_id = 'action_target_id'
        action_type = 'action_type'
        action_video_asset_id = 'action_video_asset_id'
        action_video_sound = 'action_video_sound'
        action_video_type = 'action_video_type'
        dda = 'dda'
        incrementality = 'incrementality'
        incrementality_all_conversions = 'incrementality_all_conversions'
        incrementality_first_conversion = 'incrementality_first_conversion'
        inline = 'inline'
        interactive_component_sticker_id = 'interactive_component_sticker_id'
        interactive_component_sticker_response = 'interactive_component_sticker_response'
        skan_click = 'skan_click'
        skan_click_second_postback = 'skan_click_second_postback'
        skan_click_third_postback = 'skan_click_third_postback'
        skan_view = 'skan_view'
        skan_view_second_postback = 'skan_view_second_postback'
        skan_view_third_postback = 'skan_view_third_postback'
        value = 'value'

    _field_types = {
        '1d_click': 'string',
        '1d_click_all_conversions': 'string',
        '1d_click_first_conversion': 'string',
        '1d_ev': 'string',
        '1d_ev_all_conversions': 'string',
        '1d_ev_first_conversion': 'string',
        '1d_passback': 'string',
        '1d_view': 'string',
        '1d_view_all_conversions': 'string',
        '1d_view_first_conversion': 'string',
        '28d_click': 'string',
        '28d_click_all_conversions': 'string',
        '28d_click_first_conversion': 'string',
        '28d_passback': 'string',
        '28d_view': 'string',
        '28d_view_all_conversions': 'string',
        '28d_view_first_conversion': 'string',
        '7d_click': 'string',
        '7d_click_all_conversions': 'string',
        '7d_click_first_conversion': 'string',
        '7d_passback': 'string',
        '7d_view': 'string',
        '7d_view_all_conversions': 'string',
        '7d_view_first_conversion': 'string',
        'action_brand': 'string',
        'action_canvas_component_id': 'string',
        'action_canvas_component_name': 'string',
        'action_carousel_card_id': 'string',
        'action_carousel_card_name': 'string',
        'action_category': 'string',
        'action_converted_product_id': 'string',
        'action_destination': 'string',
        'action_device': 'string',
        'action_event_channel': 'string',
        'action_link_click_destination': 'string',
        'action_location_code': 'string',
        'action_reaction': 'string',
        'action_target_id': 'string',
        'action_type': 'string',
        'action_video_asset_id': 'string',
        'action_video_sound': 'string',
        'action_video_type': 'string',
        'dda': 'string',
        'incrementality': 'string',
        'incrementality_all_conversions': 'string',
        'incrementality_first_conversion': 'string',
        'inline': 'string',
        'interactive_component_sticker_id': 'string',
        'interactive_component_sticker_response': 'string',
        'skan_click': 'string',
        'skan_click_second_postback': 'string',
        'skan_click_third_postback': 'string',
        'skan_view': 'string',
        'skan_view_second_postback': 'string',
        'skan_view_third_postback': 'string',
        'value': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


