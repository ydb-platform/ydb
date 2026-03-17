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

class FranchiseProgram(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isFranchiseProgram = True
        super(FranchiseProgram, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        business_asset_group = 'business_asset_group'
        creator_business = 'creator_business'
        description = 'description'
        end_date = 'end_date'
        id = 'id'
        name = 'name'
        program_access_type = 'program_access_type'
        program_approval_type = 'program_approval_type'
        program_image_link = 'program_image_link'
        program_url = 'program_url'
        shared_custom_audience = 'shared_custom_audience'
        start_date = 'start_date'

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
            target_class=FranchiseProgram,
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
        'business_asset_group': 'BusinessAssetGroup',
        'creator_business': 'Business',
        'description': 'string',
        'end_date': 'datetime',
        'id': 'string',
        'name': 'string',
        'program_access_type': 'string',
        'program_approval_type': 'string',
        'program_image_link': 'string',
        'program_url': 'string',
        'shared_custom_audience': 'CustomAudience',
        'start_date': 'datetime',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


