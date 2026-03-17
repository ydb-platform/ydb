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

class TransactableItem(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isTransactableItem = True
        super(TransactableItem, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        action_title = 'action_title'
        applinks = 'applinks'
        category_specific_fields = 'category_specific_fields'
        currency = 'currency'
        description = 'description'
        duration_time = 'duration_time'
        duration_type = 'duration_type'
        id = 'id'
        image_fetch_status = 'image_fetch_status'
        images = 'images'
        order_index = 'order_index'
        price = 'price'
        price_type = 'price_type'
        sanitized_images = 'sanitized_images'
        session_type = 'session_type'
        time_padding_after_end = 'time_padding_after_end'
        title = 'title'
        transactable_item_id = 'transactable_item_id'
        url = 'url'
        visibility = 'visibility'

    class ImageFetchStatus:
        direct_upload = 'DIRECT_UPLOAD'
        fetched = 'FETCHED'
        fetch_failed = 'FETCH_FAILED'
        no_status = 'NO_STATUS'
        outdated = 'OUTDATED'
        partial_fetch = 'PARTIAL_FETCH'

    class Visibility:
        published = 'PUBLISHED'
        staging = 'STAGING'

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
            target_class=TransactableItem,
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

    def get_channels_to_integrity_status(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.catalogitemchannelstointegritystatus import CatalogItemChannelsToIntegrityStatus
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/channels_to_integrity_status',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=CatalogItemChannelsToIntegrityStatus,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=CatalogItemChannelsToIntegrityStatus, api=self._api),
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

    def get_override_details(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.overridedetails import OverrideDetails
        param_types = {
            'keys': 'list<string>',
            'type': 'type_enum',
        }
        enums = {
            'type_enum': OverrideDetails.Type.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/override_details',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=OverrideDetails,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=OverrideDetails, api=self._api),
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
        'action_title': 'string',
        'applinks': 'CatalogItemAppLinks',
        'category_specific_fields': 'CatalogSubVerticalList',
        'currency': 'string',
        'description': 'string',
        'duration_time': 'unsigned int',
        'duration_type': 'string',
        'id': 'string',
        'image_fetch_status': 'ImageFetchStatus',
        'images': 'list<string>',
        'order_index': 'unsigned int',
        'price': 'string',
        'price_type': 'string',
        'sanitized_images': 'list<string>',
        'session_type': 'string',
        'time_padding_after_end': 'unsigned int',
        'title': 'string',
        'transactable_item_id': 'string',
        'url': 'string',
        'visibility': 'Visibility',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['ImageFetchStatus'] = TransactableItem.ImageFetchStatus.__dict__.values()
        field_enum_info['Visibility'] = TransactableItem.Visibility.__dict__.values()
        return field_enum_info


