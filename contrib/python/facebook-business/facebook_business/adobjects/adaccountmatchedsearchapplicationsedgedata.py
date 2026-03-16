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

class AdAccountMatchedSearchApplicationsEdgeData(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdAccountMatchedSearchApplicationsEdgeData, self).__init__()
        self._isAdAccountMatchedSearchApplicationsEdgeData = True
        self._api = api

    class Field(AbstractObject.Field):
        app_id = 'app_id'
        are_app_events_unavailable = 'are_app_events_unavailable'
        icon_url = 'icon_url'
        name = 'name'
        search_source_store = 'search_source_store'
        store = 'store'
        unique_id = 'unique_id'
        url = 'url'

    class AppStore:
        all_app_stores_for_android_and_ios = 'ALL_APP_STORES_FOR_ANDROID_AND_IOS'
        amazon_app_store = 'AMAZON_APP_STORE'
        apk_mirror = 'APK_MIRROR'
        apk_monk = 'APK_MONK'
        apk_pure = 'APK_PURE'
        aptoide_a1_store = 'APTOIDE_A1_STORE'
        bemobi_mobile_store = 'BEMOBI_MOBILE_STORE'
        digital_turbine_store = 'DIGITAL_TURBINE_STORE'
        does_not_exist = 'DOES_NOT_EXIST'
        fb_android_store = 'FB_ANDROID_STORE'
        fb_canvas = 'FB_CANVAS'
        fb_gameroom = 'FB_GAMEROOM'
        galaxy_store = 'GALAXY_STORE'
        google_play = 'GOOGLE_PLAY'
        instant_game = 'INSTANT_GAME'
        itunes = 'ITUNES'
        itunes_ipad = 'ITUNES_IPAD'
        neon_android_store = 'NEON_ANDROID_STORE'
        none = 'NONE'
        oculus_app_store = 'OCULUS_APP_STORE'
        oppo = 'OPPO'
        roku_store = 'ROKU_STORE'
        uptodown = 'UPTODOWN'
        vivo = 'VIVO'
        windows_10_store = 'WINDOWS_10_STORE'
        windows_store = 'WINDOWS_STORE'
        xiaomi = 'XIAOMI'

    class StoresToFilter:
        all_app_stores_for_android_and_ios = 'ALL_APP_STORES_FOR_ANDROID_AND_IOS'
        amazon_app_store = 'AMAZON_APP_STORE'
        apk_mirror = 'APK_MIRROR'
        apk_monk = 'APK_MONK'
        apk_pure = 'APK_PURE'
        aptoide_a1_store = 'APTOIDE_A1_STORE'
        bemobi_mobile_store = 'BEMOBI_MOBILE_STORE'
        digital_turbine_store = 'DIGITAL_TURBINE_STORE'
        does_not_exist = 'DOES_NOT_EXIST'
        fb_android_store = 'FB_ANDROID_STORE'
        fb_canvas = 'FB_CANVAS'
        fb_gameroom = 'FB_GAMEROOM'
        galaxy_store = 'GALAXY_STORE'
        google_play = 'GOOGLE_PLAY'
        instant_game = 'INSTANT_GAME'
        itunes = 'ITUNES'
        itunes_ipad = 'ITUNES_IPAD'
        neon_android_store = 'NEON_ANDROID_STORE'
        none = 'NONE'
        oculus_app_store = 'OCULUS_APP_STORE'
        oppo = 'OPPO'
        roku_store = 'ROKU_STORE'
        uptodown = 'UPTODOWN'
        vivo = 'VIVO'
        windows_10_store = 'WINDOWS_10_STORE'
        windows_store = 'WINDOWS_STORE'
        xiaomi = 'XIAOMI'

    _field_types = {
        'app_id': 'string',
        'are_app_events_unavailable': 'bool',
        'icon_url': 'string',
        'name': 'string',
        'search_source_store': 'string',
        'store': 'string',
        'unique_id': 'string',
        'url': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['AppStore'] = AdAccountMatchedSearchApplicationsEdgeData.AppStore.__dict__.values()
        field_enum_info['StoresToFilter'] = AdAccountMatchedSearchApplicationsEdgeData.StoresToFilter.__dict__.values()
        return field_enum_info


