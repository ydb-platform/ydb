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

class AdCreativeLinkDataImageLayerSpec(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdCreativeLinkDataImageLayerSpec, self).__init__()
        self._isAdCreativeLinkDataImageLayerSpec = True
        self._api = api

    class Field(AbstractObject.Field):
        blending_mode = 'blending_mode'
        content = 'content'
        frame_auto_show_enroll_status = 'frame_auto_show_enroll_status'
        frame_image_hash = 'frame_image_hash'
        frame_source = 'frame_source'
        image_source = 'image_source'
        layer_type = 'layer_type'
        opacity = 'opacity'
        overlay_position = 'overlay_position'
        overlay_shape = 'overlay_shape'
        scale = 'scale'
        shape_color = 'shape_color'
        text_color = 'text_color'
        text_font = 'text_font'

    class BlendingMode:
        lighten = 'lighten'
        multiply = 'multiply'
        normal = 'normal'

    class FrameSource:
        custom = 'custom'

    class ImageSource:
        catalog = 'catalog'

    class LayerType:
        frame_overlay = 'frame_overlay'
        image = 'image'
        text_overlay = 'text_overlay'

    class OverlayPosition:
        bottom = 'bottom'
        bottom_left = 'bottom_left'
        bottom_right = 'bottom_right'
        center = 'center'
        left = 'left'
        right = 'right'
        top = 'top'
        top_left = 'top_left'
        top_right = 'top_right'

    class OverlayShape:
        circle = 'circle'
        none = 'none'
        pill = 'pill'
        rectangle = 'rectangle'
        triangle = 'triangle'

    class TextFont:
        droid_serif_regular = 'droid_serif_regular'
        lato_regular = 'lato_regular'
        noto_sans_regular = 'noto_sans_regular'
        nunito_sans_bold = 'nunito_sans_bold'
        open_sans_bold = 'open_sans_bold'
        open_sans_condensed_bold = 'open_sans_condensed_bold'
        pt_serif_bold = 'pt_serif_bold'
        roboto_condensed_regular = 'roboto_condensed_regular'
        roboto_medium = 'roboto_medium'

    _field_types = {
        'blending_mode': 'BlendingMode',
        'content': 'Object',
        'frame_auto_show_enroll_status': 'string',
        'frame_image_hash': 'string',
        'frame_source': 'FrameSource',
        'image_source': 'ImageSource',
        'layer_type': 'LayerType',
        'opacity': 'int',
        'overlay_position': 'OverlayPosition',
        'overlay_shape': 'OverlayShape',
        'scale': 'int',
        'shape_color': 'string',
        'text_color': 'string',
        'text_font': 'TextFont',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['BlendingMode'] = AdCreativeLinkDataImageLayerSpec.BlendingMode.__dict__.values()
        field_enum_info['FrameSource'] = AdCreativeLinkDataImageLayerSpec.FrameSource.__dict__.values()
        field_enum_info['ImageSource'] = AdCreativeLinkDataImageLayerSpec.ImageSource.__dict__.values()
        field_enum_info['LayerType'] = AdCreativeLinkDataImageLayerSpec.LayerType.__dict__.values()
        field_enum_info['OverlayPosition'] = AdCreativeLinkDataImageLayerSpec.OverlayPosition.__dict__.values()
        field_enum_info['OverlayShape'] = AdCreativeLinkDataImageLayerSpec.OverlayShape.__dict__.values()
        field_enum_info['TextFont'] = AdCreativeLinkDataImageLayerSpec.TextFont.__dict__.values()
        return field_enum_info


