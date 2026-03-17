# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

from facebook_business.adobjects.abstractobject import AbstractObject
from facebook_business.adobjects.abstractcrudobject import AbstractCrudObject
from facebook_business.adobjects.objectparser import ObjectParser
from facebook_business.api import FacebookRequest
from facebook_business.typechecker import TypeChecker
from facebook_business.adobjects.helpers.adimagemixin import AdImageMixin

"""
This class is auto-generated.

For any issues or feature requests related to this class, please let us know on
github and we'll fix in our codegen framework. We'll not be able to accept
pull request for this class.
"""

class AdImage(
    AdImageMixin,
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isAdImage = True
        super(AdImage, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        account_id = 'account_id'
        created_time = 'created_time'
        creatives = 'creatives'
        hash = 'hash'
        height = 'height'
        id = 'id'
        is_associated_creatives_in_adgroups = 'is_associated_creatives_in_adgroups'
        name = 'name'
        original_height = 'original_height'
        original_width = 'original_width'
        owner_business = 'owner_business'
        permalink_url = 'permalink_url'
        status = 'status'
        updated_time = 'updated_time'
        url = 'url'
        url_128 = 'url_128'
        width = 'width'
        bytes = 'bytes'
        copy_from = 'copy_from'
        filename = 'filename'

    class Status:
        active = 'ACTIVE'
        deleted = 'DELETED'
        internal = 'INTERNAL'

    # @deprecated get_endpoint function is deprecated
    @classmethod
    def get_endpoint(cls):
        return 'adimages'

    # @deprecated api_create is being deprecated
    def api_create(self, parent_id, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.adobjects.adaccount import AdAccount
        return AdAccount(api=self._api, fbid=parent_id).create_ad_image(fields, params, batch, success, failure, pending)

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
            target_class=AdImage,
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
        'account_id': 'string',
        'created_time': 'datetime',
        'creatives': 'list<string>',
        'hash': 'string',
        'height': 'unsigned int',
        'id': 'string',
        'is_associated_creatives_in_adgroups': 'bool',
        'name': 'string',
        'original_height': 'unsigned int',
        'original_width': 'unsigned int',
        'owner_business': 'Business',
        'permalink_url': 'string',
        'status': 'Status',
        'updated_time': 'datetime',
        'url': 'string',
        'url_128': 'string',
        'width': 'unsigned int',
        'bytes': 'string',
        'copy_from': 'Object',
        'filename': 'file'
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['Status'] = AdImage.Status.__dict__.values()
        return field_enum_info


