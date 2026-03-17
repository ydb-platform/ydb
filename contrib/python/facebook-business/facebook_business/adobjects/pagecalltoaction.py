# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

from facebook_business.adobjects.abstractobject import AbstractObject
from facebook_business.adobjects.abstractcrudobject import AbstractCrudObject
from facebook_business.adobjects.objectparser import ObjectParser
from facebook_business.api import FacebookRequest
from facebook_business.typechecker import TypeChecker

"""
This class is auto-generated.

For any issues or feature requests related to this class, please let us know on
github and we'll fix in our codegen framework. We'll not be able to accept
pull request for this class.
"""

class PageCallToAction(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isPageCallToAction = True
        super(PageCallToAction, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        android_app = 'android_app'
        android_deeplink = 'android_deeplink'
        android_destination_type = 'android_destination_type'
        android_package_name = 'android_package_name'
        android_url = 'android_url'
        created_time = 'created_time'
        email_address = 'email_address'
        field_from = 'from'
        id = 'id'
        intl_number_with_plus = 'intl_number_with_plus'
        iphone_app = 'iphone_app'
        iphone_deeplink = 'iphone_deeplink'
        iphone_destination_type = 'iphone_destination_type'
        iphone_url = 'iphone_url'
        status = 'status'
        type = 'type'
        updated_time = 'updated_time'
        web_destination_type = 'web_destination_type'
        web_url = 'web_url'

    class AndroidDestinationType:
        app_deeplink = 'APP_DEEPLINK'
        become_a_volunteer = 'BECOME_A_VOLUNTEER'
        email = 'EMAIL'
        facebook_app = 'FACEBOOK_APP'
        follow = 'FOLLOW'
        marketplace_inventory_page = 'MARKETPLACE_INVENTORY_PAGE'
        menu_on_facebook = 'MENU_ON_FACEBOOK'
        messenger = 'MESSENGER'
        mini_shop = 'MINI_SHOP'
        mobile_center = 'MOBILE_CENTER'
        none = 'NONE'
        phone_call = 'PHONE_CALL'
        shop_on_facebook = 'SHOP_ON_FACEBOOK'
        website = 'WEBSITE'

    class IphoneDestinationType:
        app_deeplink = 'APP_DEEPLINK'
        become_a_volunteer = 'BECOME_A_VOLUNTEER'
        email = 'EMAIL'
        facebook_app = 'FACEBOOK_APP'
        follow = 'FOLLOW'
        marketplace_inventory_page = 'MARKETPLACE_INVENTORY_PAGE'
        menu_on_facebook = 'MENU_ON_FACEBOOK'
        messenger = 'MESSENGER'
        mini_shop = 'MINI_SHOP'
        none = 'NONE'
        phone_call = 'PHONE_CALL'
        shop_on_facebook = 'SHOP_ON_FACEBOOK'
        website = 'WEBSITE'

    class Type:
        become_a_volunteer = 'BECOME_A_VOLUNTEER'
        book_appointment = 'BOOK_APPOINTMENT'
        book_now = 'BOOK_NOW'
        buy_tickets = 'BUY_TICKETS'
        call_now = 'CALL_NOW'
        charity_donate = 'CHARITY_DONATE'
        check_in = 'CHECK_IN'
        contact_us = 'CONTACT_US'
        creator_storefront = 'CREATOR_STOREFRONT'
        donate_now = 'DONATE_NOW'
        email = 'EMAIL'
        follow_page = 'FOLLOW_PAGE'
        get_directions = 'GET_DIRECTIONS'
        get_offer = 'GET_OFFER'
        get_offer_view = 'GET_OFFER_VIEW'
        interested = 'INTERESTED'
        learn_more = 'LEARN_MORE'
        listen = 'LISTEN'
        local_dev_platform = 'LOCAL_DEV_PLATFORM'
        message = 'MESSAGE'
        mobile_center = 'MOBILE_CENTER'
        open_app = 'OPEN_APP'
        order_food = 'ORDER_FOOD'
        play_music = 'PLAY_MUSIC'
        play_now = 'PLAY_NOW'
        purchase_gift_cards = 'PURCHASE_GIFT_CARDS'
        request_appointment = 'REQUEST_APPOINTMENT'
        request_quote = 'REQUEST_QUOTE'
        shop_now = 'SHOP_NOW'
        shop_on_facebook = 'SHOP_ON_FACEBOOK'
        sign_up = 'SIGN_UP'
        view_inventory = 'VIEW_INVENTORY'
        view_menu = 'VIEW_MENU'
        view_shop = 'VIEW_SHOP'
        visit_group = 'VISIT_GROUP'
        watch_now = 'WATCH_NOW'
        woodhenge_support = 'WOODHENGE_SUPPORT'

    class WebDestinationType:
        become_a_volunteer = 'BECOME_A_VOLUNTEER'
        become_supporter = 'BECOME_SUPPORTER'
        email = 'EMAIL'
        follow = 'FOLLOW'
        messenger = 'MESSENGER'
        mobile_center = 'MOBILE_CENTER'
        none = 'NONE'
        shop_on_facebook = 'SHOP_ON_FACEBOOK'
        website = 'WEBSITE'

    def api_delete(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='DELETE',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AbstractCrudObject,
            api_type='NODE',
            response_parser=ObjectParser(reuse_object=self),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def api_get(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=PageCallToAction,
            api_type='NODE',
            response_parser=ObjectParser(reuse_object=self),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def api_update(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'android_app_id': 'int',
            'android_destination_type': 'android_destination_type_enum',
            'android_package_name': 'string',
            'android_url': 'string',
            'email_address': 'string',
            'intl_number_with_plus': 'string',
            'iphone_app_id': 'int',
            'iphone_destination_type': 'iphone_destination_type_enum',
            'iphone_url': 'string',
            'type': 'type_enum',
            'web_destination_type': 'web_destination_type_enum',
            'web_url': 'string',
        }
        enums = {
            'android_destination_type_enum': PageCallToAction.AndroidDestinationType.__dict__.values(),
            'iphone_destination_type_enum': PageCallToAction.IphoneDestinationType.__dict__.values(),
            'type_enum': PageCallToAction.Type.__dict__.values(),
            'web_destination_type_enum': PageCallToAction.WebDestinationType.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=PageCallToAction,
            api_type='NODE',
            response_parser=ObjectParser(reuse_object=self),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    _field_types = {
        'android_app': 'Application',
        'android_deeplink': 'string',
        'android_destination_type': 'string',
        'android_package_name': 'string',
        'android_url': 'string',
        'created_time': 'datetime',
        'email_address': 'string',
        'from': 'Page',
        'id': 'string',
        'intl_number_with_plus': 'string',
        'iphone_app': 'Application',
        'iphone_deeplink': 'string',
        'iphone_destination_type': 'string',
        'iphone_url': 'string',
        'status': 'string',
        'type': 'string',
        'updated_time': 'datetime',
        'web_destination_type': 'string',
        'web_url': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['AndroidDestinationType'] = PageCallToAction.AndroidDestinationType.__dict__.values()
        field_enum_info['IphoneDestinationType'] = PageCallToAction.IphoneDestinationType.__dict__.values()
        field_enum_info['Type'] = PageCallToAction.Type.__dict__.values()
        field_enum_info['WebDestinationType'] = PageCallToAction.WebDestinationType.__dict__.values()
        return field_enum_info


