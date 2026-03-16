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

class AdgroupIssuesInfo(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdgroupIssuesInfo, self).__init__()
        self._isAdgroupIssuesInfo = True
        self._api = api

    class Field(AbstractObject.Field):
        error_code = 'error_code'
        error_message = 'error_message'
        error_summary = 'error_summary'
        error_type = 'error_type'
        level = 'level'
        mid = 'mid'

    _field_types = {
        'error_code': 'int',
        'error_message': 'string',
        'error_summary': 'string',
        'error_type': 'string',
        'level': 'string',
        'mid': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


