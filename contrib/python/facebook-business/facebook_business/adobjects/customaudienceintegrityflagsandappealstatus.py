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

class CustomAudienceIntegrityFlagsAndAppealStatus(
    AbstractObject,
):

    def __init__(self, api=None):
        super(CustomAudienceIntegrityFlagsAndAppealStatus, self).__init__()
        self._isCustomAudienceIntegrityFlagsAndAppealStatus = True
        self._api = api

    class Field(AbstractObject.Field):
        closeout_time = 'closeout_time'
        days_until_enforcement = 'days_until_enforcement'
        flagged_fields = 'flagged_fields'
        is_enforcement_rolled_out = 'is_enforcement_rolled_out'
        latest_appeal_requestor = 'latest_appeal_requestor'
        latest_appeal_time = 'latest_appeal_time'
        restriction_status = 'restriction_status'

    _field_types = {
        'closeout_time': 'unsigned int',
        'days_until_enforcement': 'unsigned int',
        'flagged_fields': 'list<string>',
        'is_enforcement_rolled_out': 'bool',
        'latest_appeal_requestor': 'string',
        'latest_appeal_time': 'unsigned int',
        'restriction_status': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


