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

class Canvas(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isCanvas = True
        super(Canvas, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        background_color = 'background_color'
        body_elements = 'body_elements'
        business_id = 'business_id'
        canvas_link = 'canvas_link'
        collection_hero_image = 'collection_hero_image'
        collection_hero_video = 'collection_hero_video'
        collection_thumbnails = 'collection_thumbnails'
        dynamic_setting = 'dynamic_setting'
        element_payload = 'element_payload'
        elements = 'elements'
        fb_body_elements = 'fb_body_elements'
        hero_asset_facebook_post_id = 'hero_asset_facebook_post_id'
        hero_asset_instagram_media_id = 'hero_asset_instagram_media_id'
        id = 'id'
        is_hidden = 'is_hidden'
        is_published = 'is_published'
        last_editor = 'last_editor'
        linked_documents = 'linked_documents'
        name = 'name'
        owner = 'owner'
        property_list = 'property_list'
        source_template = 'source_template'
        store_url = 'store_url'
        style_list = 'style_list'
        tags = 'tags'
        ui_property_list = 'ui_property_list'
        unused_body_elements = 'unused_body_elements'
        update_time = 'update_time'
        use_retailer_item_ids = 'use_retailer_item_ids'

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
            target_class=Canvas,
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
            'background_color': 'string',
            'body_element_ids': 'list<string>',
            'enable_swipe_to_open': 'bool',
            'hero_asset_facebook_post_id': 'string',
            'hero_asset_instagram_media_id': 'string',
            'is_hidden': 'bool',
            'is_published': 'bool',
            'name': 'string',
            'source_template_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Canvas,
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

    def get_preview(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.canvaspreview import CanvasPreview
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/preview',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=CanvasPreview,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=CanvasPreview, api=self._api),
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

    def get_previews(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.textwithentities import TextWithEntities
        param_types = {
            'user_ids': 'list<int>',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/previews',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=TextWithEntities,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=TextWithEntities, api=self._api),
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
        'background_color': 'string',
        'body_elements': 'list<Object>',
        'business_id': 'string',
        'canvas_link': 'string',
        'collection_hero_image': 'Photo',
        'collection_hero_video': 'AdVideo',
        'collection_thumbnails': 'list<CanvasCollectionThumbnail>',
        'dynamic_setting': 'CanvasDynamicSetting',
        'element_payload': 'string',
        'elements': 'list<RichMediaElement>',
        'fb_body_elements': 'list<Object>',
        'hero_asset_facebook_post_id': 'string',
        'hero_asset_instagram_media_id': 'string',
        'id': 'string',
        'is_hidden': 'bool',
        'is_published': 'bool',
        'last_editor': 'User',
        'linked_documents': 'list<Canvas>',
        'name': 'string',
        'owner': 'Page',
        'property_list': 'list<string>',
        'source_template': 'CanvasTemplate',
        'store_url': 'string',
        'style_list': 'list<string>',
        'tags': 'list<string>',
        'ui_property_list': 'list<string>',
        'unused_body_elements': 'list<Object>',
        'update_time': 'int',
        'use_retailer_item_ids': 'bool',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


