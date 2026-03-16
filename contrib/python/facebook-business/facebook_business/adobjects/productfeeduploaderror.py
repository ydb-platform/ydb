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

class ProductFeedUploadError(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isProductFeedUploadError = True
        super(ProductFeedUploadError, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        affected_surfaces = 'affected_surfaces'
        description = 'description'
        error_type = 'error_type'
        id = 'id'
        severity = 'severity'
        summary = 'summary'
        total_count = 'total_count'

    class AffectedSurfaces:
        dynamic_ads = 'Dynamic Ads'
        marketplace = 'Marketplace'
        us_marketplace = 'US Marketplace'

    class Severity:
        fatal = 'fatal'
        warning = 'warning'

    class ErrorPriority:
        high = 'HIGH'
        low = 'LOW'
        medium = 'MEDIUM'

    # @deprecated get_endpoint function is deprecated
    @classmethod
    def get_endpoint(cls):
        return 'errors'

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
            target_class=ProductFeedUploadError,
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

    def get_samples(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.productfeeduploaderrorsample import ProductFeedUploadErrorSample
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/samples',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductFeedUploadErrorSample,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductFeedUploadErrorSample, api=self._api),
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

    def get_suggested_rules(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.productfeedrulesuggestion import ProductFeedRuleSuggestion
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/suggested_rules',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductFeedRuleSuggestion,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductFeedRuleSuggestion, api=self._api),
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
        'affected_surfaces': 'list<AffectedSurfaces>',
        'description': 'string',
        'error_type': 'string',
        'id': 'string',
        'severity': 'Severity',
        'summary': 'string',
        'total_count': 'unsigned int',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['AffectedSurfaces'] = ProductFeedUploadError.AffectedSurfaces.__dict__.values()
        field_enum_info['Severity'] = ProductFeedUploadError.Severity.__dict__.values()
        field_enum_info['ErrorPriority'] = ProductFeedUploadError.ErrorPriority.__dict__.values()
        return field_enum_info


