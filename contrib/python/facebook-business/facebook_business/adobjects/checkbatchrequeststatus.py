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

class CheckBatchRequestStatus(
    AbstractObject,
):

    def __init__(self, api=None):
        super(CheckBatchRequestStatus, self).__init__()
        self._isCheckBatchRequestStatus = True
        self._api = api

    class Field(AbstractObject.Field):
        errors = 'errors'
        errors_total_count = 'errors_total_count'
        handle = 'handle'
        ids_of_invalid_requests = 'ids_of_invalid_requests'
        status = 'status'
        warnings = 'warnings'
        warnings_total_count = 'warnings_total_count'

    class ErrorPriority:
        high = 'HIGH'
        low = 'LOW'
        medium = 'MEDIUM'

    _field_types = {
        'errors': 'list<Object>',
        'errors_total_count': 'int',
        'handle': 'string',
        'ids_of_invalid_requests': 'list<string>',
        'status': 'string',
        'warnings': 'list<Object>',
        'warnings_total_count': 'int',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['ErrorPriority'] = CheckBatchRequestStatus.ErrorPriority.__dict__.values()
        return field_enum_info


