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

class VideoStatusUploadingPhase(
    AbstractObject,
):

    def __init__(self, api=None):
        super(VideoStatusUploadingPhase, self).__init__()
        self._isVideoStatusUploadingPhase = True
        self._api = api

    class Field(AbstractObject.Field):
        bytes_transferred = 'bytes_transferred'
        errors = 'errors'
        source_file_size = 'source_file_size'
        status = 'status'

    _field_types = {
        'bytes_transferred': 'unsigned int',
        'errors': 'list<VideoStatusError>',
        'source_file_size': 'unsigned int',
        'status': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


