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

class ReportingAudience(
    AbstractObject,
):

    def __init__(self, api=None):
        super(ReportingAudience, self).__init__()
        self._isReportingAudience = True
        self._api = api

    class Field(AbstractObject.Field):
        custom_audiences = 'custom_audiences'
        custom_audiences_url_param_name = 'custom_audiences_url_param_name'
        custom_audiences_url_param_type = 'custom_audiences_url_param_type'

    _field_types = {
        'custom_audiences': 'list<RawCustomAudience>',
        'custom_audiences_url_param_name': 'string',
        'custom_audiences_url_param_type': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


