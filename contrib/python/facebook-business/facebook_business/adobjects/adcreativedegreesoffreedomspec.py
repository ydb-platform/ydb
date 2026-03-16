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

class AdCreativeDegreesOfFreedomSpec(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdCreativeDegreesOfFreedomSpec, self).__init__()
        self._isAdCreativeDegreesOfFreedomSpec = True
        self._api = api

    class Field(AbstractObject.Field):
        ad_handle_type = 'ad_handle_type'
        creative_features_spec = 'creative_features_spec'
        degrees_of_freedom_type = 'degrees_of_freedom_type'
        image_transformation_types = 'image_transformation_types'
        multi_media_transformation_type = 'multi_media_transformation_type'
        stories_transformation_types = 'stories_transformation_types'
        text_transformation_types = 'text_transformation_types'
        video_transformation_types = 'video_transformation_types'

    _field_types = {
        'ad_handle_type': 'string',
        'creative_features_spec': 'AdCreativeFeaturesSpec',
        'degrees_of_freedom_type': 'string',
        'image_transformation_types': 'list<string>',
        'multi_media_transformation_type': 'string',
        'stories_transformation_types': 'list<string>',
        'text_transformation_types': 'list<string>',
        'video_transformation_types': 'list<string>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


