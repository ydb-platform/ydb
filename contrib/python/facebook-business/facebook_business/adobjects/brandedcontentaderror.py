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

class BrandedContentAdError(
    AbstractObject,
):

    def __init__(self, api=None):
        super(BrandedContentAdError, self).__init__()
        self._isBrandedContentAdError = True
        self._api = api

    class Field(AbstractObject.Field):
        blame_field_spec = 'blame_field_spec'
        error_code = 'error_code'
        error_description = 'error_description'
        error_message = 'error_message'
        error_placement = 'error_placement'
        error_severity = 'error_severity'
        help_center_id = 'help_center_id'

    _field_types = {
        'blame_field_spec': 'list<string>',
        'error_code': 'int',
        'error_description': 'string',
        'error_message': 'string',
        'error_placement': 'string',
        'error_severity': 'string',
        'help_center_id': 'int',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


