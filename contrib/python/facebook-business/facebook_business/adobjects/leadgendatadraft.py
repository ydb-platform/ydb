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

class LeadGenDataDraft(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isLeadGenDataDraft = True
        super(LeadGenDataDraft, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        block_display_for_non_targeted_viewer = 'block_display_for_non_targeted_viewer'
        created_time = 'created_time'
        disqualified_end_component = 'disqualified_end_component'
        follow_up_action_url = 'follow_up_action_url'
        id = 'id'
        is_optimized_for_quality = 'is_optimized_for_quality'
        legal_content = 'legal_content'
        locale = 'locale'
        name = 'name'
        page = 'page'
        question_page_custom_headline = 'question_page_custom_headline'
        questions = 'questions'
        should_enforce_work_email = 'should_enforce_work_email'
        status = 'status'
        thank_you_page = 'thank_you_page'
        tracking_parameters = 'tracking_parameters'

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
            target_class=LeadGenDataDraft,
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
        'block_display_for_non_targeted_viewer': 'bool',
        'created_time': 'datetime',
        'disqualified_end_component': 'Object',
        'follow_up_action_url': 'string',
        'id': 'string',
        'is_optimized_for_quality': 'bool',
        'legal_content': 'Object',
        'locale': 'string',
        'name': 'string',
        'page': 'Page',
        'question_page_custom_headline': 'string',
        'questions': 'list<LeadGenDraftQuestion>',
        'should_enforce_work_email': 'bool',
        'status': 'string',
        'thank_you_page': 'Object',
        'tracking_parameters': 'list<map<string, string>>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


