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

class ProductEventStat(
    AbstractObject,
):

    def __init__(self, api=None):
        super(ProductEventStat, self).__init__()
        self._isProductEventStat = True
        self._api = api

    class Field(AbstractObject.Field):
        date_start = 'date_start'
        date_stop = 'date_stop'
        device_type = 'device_type'
        event = 'event'
        event_source = 'event_source'
        total_content_ids_matched_other_catalogs = 'total_content_ids_matched_other_catalogs'
        total_matched_content_ids = 'total_matched_content_ids'
        total_unmatched_content_ids = 'total_unmatched_content_ids'
        unique_content_ids_matched_other_catalogs = 'unique_content_ids_matched_other_catalogs'
        unique_matched_content_ids = 'unique_matched_content_ids'
        unique_unmatched_content_ids = 'unique_unmatched_content_ids'

    class DeviceType:
        desktop = 'desktop'
        mobile_android_phone = 'mobile_android_phone'
        mobile_android_tablet = 'mobile_android_tablet'
        mobile_ipad = 'mobile_ipad'
        mobile_iphone = 'mobile_iphone'
        mobile_ipod = 'mobile_ipod'
        mobile_phone = 'mobile_phone'
        mobile_tablet = 'mobile_tablet'
        mobile_windows_phone = 'mobile_windows_phone'
        unknown = 'unknown'

    class Event:
        addtocart = 'AddToCart'
        addtowishlist = 'AddToWishlist'
        initiatecheckout = 'InitiateCheckout'
        lead = 'Lead'
        purchase = 'Purchase'
        search = 'Search'
        subscribe = 'Subscribe'
        viewcontent = 'ViewContent'

    class Breakdowns:
        device_type = 'DEVICE_TYPE'

    _field_types = {
        'date_start': 'string',
        'date_stop': 'string',
        'device_type': 'DeviceType',
        'event': 'Event',
        'event_source': 'ExternalEventSource',
        'total_content_ids_matched_other_catalogs': 'int',
        'total_matched_content_ids': 'int',
        'total_unmatched_content_ids': 'int',
        'unique_content_ids_matched_other_catalogs': 'int',
        'unique_matched_content_ids': 'int',
        'unique_unmatched_content_ids': 'int',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['DeviceType'] = ProductEventStat.DeviceType.__dict__.values()
        field_enum_info['Event'] = ProductEventStat.Event.__dict__.values()
        field_enum_info['Breakdowns'] = ProductEventStat.Breakdowns.__dict__.values()
        return field_enum_info


