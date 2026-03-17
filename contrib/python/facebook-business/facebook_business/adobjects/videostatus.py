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

class VideoStatus(
    AbstractObject,
):

    def __init__(self, api=None):
        super(VideoStatus, self).__init__()
        self._isVideoStatus = True
        self._api = api

    class Field(AbstractObject.Field):
        copyright_check_status = 'copyright_check_status'
        processing_phase = 'processing_phase'
        processing_progress = 'processing_progress'
        publishing_phase = 'publishing_phase'
        uploading_phase = 'uploading_phase'
        video_status = 'video_status'

    _field_types = {
        'copyright_check_status': 'VideoCopyrightCheckStatus',
        'processing_phase': 'VideoStatusProcessingPhase',
        'processing_progress': 'unsigned int',
        'publishing_phase': 'VideoStatusPublishingPhase',
        'uploading_phase': 'VideoStatusUploadingPhase',
        'video_status': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


