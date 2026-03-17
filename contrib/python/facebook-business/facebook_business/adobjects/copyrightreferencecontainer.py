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

class CopyrightReferenceContainer(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isCopyrightReferenceContainer = True
        super(CopyrightReferenceContainer, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        content_type = 'content_type'
        copyright_creation_time = 'copyright_creation_time'
        download_hd_url = 'download_hd_url'
        duration_in_sec = 'duration_in_sec'
        id = 'id'
        iswc = 'iswc'
        metadata = 'metadata'
        playable_video_uri = 'playable_video_uri'
        published_time = 'published_time'
        thumbnail_url = 'thumbnail_url'
        title = 'title'
        universal_content_id = 'universal_content_id'
        writer_names = 'writer_names'

    _field_types = {
        'content_type': 'string',
        'copyright_creation_time': 'datetime',
        'download_hd_url': 'string',
        'duration_in_sec': 'float',
        'id': 'string',
        'iswc': 'string',
        'metadata': 'Object',
        'playable_video_uri': 'string',
        'published_time': 'datetime',
        'thumbnail_url': 'string',
        'title': 'string',
        'universal_content_id': 'string',
        'writer_names': 'list<string>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


