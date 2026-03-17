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

class TimezoneOffset(
    AbstractObject,
):

    def __init__(self, api=None):
        super(TimezoneOffset, self).__init__()
        self._isTimezoneOffset = True
        self._api = api

    class Field(AbstractObject.Field):
        abbr = 'abbr'
        isdst = 'isdst'
        offset = 'offset'
        time = 'time'
        ts = 'ts'

    _field_types = {
        'abbr': 'string',
        'isdst': 'bool',
        'offset': 'int',
        'time': 'string',
        'ts': 'unsigned int',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


