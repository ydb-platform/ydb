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

class ExternalEventSourceCPASEventsDebugging(
    AbstractObject,
):

    def __init__(self, api=None):
        super(ExternalEventSourceCPASEventsDebugging, self).__init__()
        self._isExternalEventSourceCPASEventsDebugging = True
        self._api = api

    class Field(AbstractObject.Field):
        actual_event_time = 'actual_event_time'
        app_version = 'app_version'
        content_url = 'content_url'
        device_os = 'device_os'
        diagnostic = 'diagnostic'
        event_name = 'event_name'
        event_time = 'event_time'
        missing_ids = 'missing_ids'
        severity = 'severity'

    _field_types = {
        'actual_event_time': 'int',
        'app_version': 'string',
        'content_url': 'string',
        'device_os': 'string',
        'diagnostic': 'string',
        'event_name': 'string',
        'event_time': 'int',
        'missing_ids': 'string',
        'severity': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


