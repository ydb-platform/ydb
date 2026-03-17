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

class LeadGenAppointmentBookingInfo(
    AbstractObject,
):

    def __init__(self, api=None):
        super(LeadGenAppointmentBookingInfo, self).__init__()
        self._isLeadGenAppointmentBookingInfo = True
        self._api = api

    class Field(AbstractObject.Field):
        advertiser_timezone_offset = 'advertiser_timezone_offset'
        appointment_durations = 'appointment_durations'
        appointment_slots_by_day = 'appointment_slots_by_day'

    _field_types = {
        'advertiser_timezone_offset': 'string',
        'appointment_durations': 'list<string>',
        'appointment_slots_by_day': 'list<LeadGenAppointmentSlotsByDay>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


