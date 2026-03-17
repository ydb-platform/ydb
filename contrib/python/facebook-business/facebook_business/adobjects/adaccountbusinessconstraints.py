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

class AdAccountBusinessConstraints(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isAdAccountBusinessConstraints = True
        super(AdAccountBusinessConstraints, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        audience_controls = 'audience_controls'
        campaigns_with_error = 'campaigns_with_error'
        placement_controls = 'placement_controls'
        status = 'status'

    class Status:
        active = 'ACTIVE'
        application_in_progress = 'APPLICATION_IN_PROGRESS'
        with_campaign_error = 'WITH_CAMPAIGN_ERROR'

    # @deprecated get_endpoint function is deprecated
    @classmethod
    def get_endpoint(cls):
        return 'account_controls'

    # @deprecated api_create is being deprecated
    def api_create(self, parent_id, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.adobjects.adaccount import AdAccount
        return AdAccount(api=self._api, fbid=parent_id).create_account_control(fields, params, batch, success, failure, pending)

    _field_types = {
        'audience_controls': 'Object',
        'campaigns_with_error': 'list<string>',
        'placement_controls': 'Object',
        'status': 'Status',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['Status'] = AdAccountBusinessConstraints.Status.__dict__.values()
        return field_enum_info


