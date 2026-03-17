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

class OpenBridgeConfiguration(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isOpenBridgeConfiguration = True
        super(OpenBridgeConfiguration, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        active = 'active'
        blocked_event_types = 'blocked_event_types'
        blocked_websites = 'blocked_websites'
        browser_agent = 'browser_agent'
        cloud_provider = 'cloud_provider'
        cloud_region = 'cloud_region'
        destination_id = 'destination_id'
        endpoint = 'endpoint'
        event_enrichment_state = 'event_enrichment_state'
        fallback_domain = 'fallback_domain'
        first_party_domain = 'first_party_domain'
        host_business_id = 'host_business_id'
        id = 'id'
        instance_id = 'instance_id'
        instance_version = 'instance_version'
        is_sgw_instance = 'is_sgw_instance'
        is_sgw_pixel_from_meta_pixel = 'is_sgw_pixel_from_meta_pixel'
        partner_name = 'partner_name'
        pixel_id = 'pixel_id'
        sgw_account_id = 'sgw_account_id'
        sgw_instance_url = 'sgw_instance_url'
        sgw_pixel_id = 'sgw_pixel_id'

    class EventEnrichmentState:
        no = 'NO'
        not_initialized = 'NOT_INITIALIZED'
        yes = 'YES'

    # @deprecated get_endpoint function is deprecated
    @classmethod
    def get_endpoint(cls):
        return 'openbridge_configurations'

    # @deprecated api_create is being deprecated
    def api_create(self, parent_id, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.adobjects.business import Business
        return Business(api=self._api, fbid=parent_id).create_open_bridge_configuration(fields, params, batch, success, failure, pending)

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
            target_class=OpenBridgeConfiguration,
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
            'active': 'bool',
            'blocked_event_types': 'list<string>',
            'blocked_websites': 'list<string>',
            'cloud_provider': 'string',
            'cloud_region': 'string',
            'destination_id': 'string',
            'endpoint': 'string',
            'event_enrichment_state': 'event_enrichment_state_enum',
            'fallback_domain': 'string',
            'first_party_domain': 'string',
            'host_business_id': 'unsigned int',
            'instance_id': 'string',
            'instance_version': 'string',
            'is_sgw_instance': 'bool',
            'is_sgw_pixel_from_meta_pixel': 'bool',
            'partner_name': 'string',
            'sgw_account_id': 'string',
            'sgw_instance_url': 'string',
            'sgw_pixel_id': 'unsigned int',
        }
        enums = {
            'event_enrichment_state_enum': OpenBridgeConfiguration.EventEnrichmentState.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=OpenBridgeConfiguration,
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
        'active': 'bool',
        'blocked_event_types': 'list<string>',
        'blocked_websites': 'list<string>',
        'browser_agent': 'list<string>',
        'cloud_provider': 'string',
        'cloud_region': 'string',
        'destination_id': 'string',
        'endpoint': 'string',
        'event_enrichment_state': 'string',
        'fallback_domain': 'string',
        'first_party_domain': 'string',
        'host_business_id': 'string',
        'id': 'string',
        'instance_id': 'string',
        'instance_version': 'string',
        'is_sgw_instance': 'bool',
        'is_sgw_pixel_from_meta_pixel': 'bool',
        'partner_name': 'string',
        'pixel_id': 'string',
        'sgw_account_id': 'string',
        'sgw_instance_url': 'string',
        'sgw_pixel_id': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['EventEnrichmentState'] = OpenBridgeConfiguration.EventEnrichmentState.__dict__.values()
        return field_enum_info


