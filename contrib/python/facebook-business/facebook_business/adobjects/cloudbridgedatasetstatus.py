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

class CloudbridgeDatasetStatus(
    AbstractObject,
):

    def __init__(self, api=None):
        super(CloudbridgeDatasetStatus, self).__init__()
        self._isCloudbridgeDatasetStatus = True
        self._api = api

    class Field(AbstractObject.Field):
        app_redacted_event = 'app_redacted_event'
        app_sensitive_params = 'app_sensitive_params'
        app_unverified_event = 'app_unverified_event'
        has_app_associated = 'has_app_associated'
        is_app_prohibited = 'is_app_prohibited'
        is_dataset = 'is_dataset'

    _field_types = {
        'app_redacted_event': 'list<string>',
        'app_sensitive_params': 'list<map<string, list<string>>>',
        'app_unverified_event': 'list<string>',
        'has_app_associated': 'bool',
        'is_app_prohibited': 'bool',
        'is_dataset': 'bool',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


