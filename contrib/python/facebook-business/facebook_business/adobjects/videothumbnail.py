# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

from facebook_business.adobjects.abstractobject import AbstractObject
from facebook_business.adobjects.abstractcrudobject import AbstractCrudObject
from facebook_business.adobjects.objectparser import ObjectParser
from facebook_business.api import FacebookRequest
from facebook_business.typechecker import TypeChecker

"""
This class is auto-generated.

For any issues or feature requests related to this class, please let us know on
github and we'll fix in our codegen framework. We'll not be able to accept
pull request for this class.
"""

class VideoThumbnail(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isVideoThumbnail = True
        super(VideoThumbnail, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        height = 'height'
        id = 'id'
        is_preferred = 'is_preferred'
        name = 'name'
        scale = 'scale'
        uri = 'uri'
        width = 'width'

    # @deprecated get_endpoint function is deprecated
    @classmethod
    def get_endpoint(cls):
        return 'thumbnails'

    _field_types = {
        'height': 'unsigned int',
        'id': 'string',
        'is_preferred': 'bool',
        'name': 'string',
        'scale': 'float',
        'uri': 'string',
        'width': 'unsigned int',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


