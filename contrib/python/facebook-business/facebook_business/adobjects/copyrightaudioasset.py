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

class CopyrightAudioAsset(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isCopyrightAudioAsset = True
        super(CopyrightAudioAsset, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        audio_availability_status = 'audio_availability_status'
        audio_library_policy = 'audio_library_policy'
        creation_time = 'creation_time'
        id = 'id'
        reference_files = 'reference_files'
        title = 'title'
        update_time = 'update_time'

    _field_types = {
        'audio_availability_status': 'string',
        'audio_library_policy': 'list<map<string, list<map<string, Object>>>>',
        'creation_time': 'datetime',
        'id': 'string',
        'reference_files': 'list<Object>',
        'title': 'string',
        'update_time': 'datetime',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


