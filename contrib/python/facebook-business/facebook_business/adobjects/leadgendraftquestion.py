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

class LeadGenDraftQuestion(
    AbstractObject,
):

    def __init__(self, api=None):
        super(LeadGenDraftQuestion, self).__init__()
        self._isLeadGenDraftQuestion = True
        self._api = api

    class Field(AbstractObject.Field):
        conditional_questions_choices = 'conditional_questions_choices'
        conditional_questions_group_id = 'conditional_questions_group_id'
        dependent_conditional_questions = 'dependent_conditional_questions'
        inline_context = 'inline_context'
        key = 'key'
        label = 'label'
        options = 'options'
        type = 'type'

    _field_types = {
        'conditional_questions_choices': 'list<LeadGenConditionalQuestionsGroupChoices>',
        'conditional_questions_group_id': 'string',
        'dependent_conditional_questions': 'list<LeadGenConditionalQuestionsGroupQuestions>',
        'inline_context': 'string',
        'key': 'string',
        'label': 'string',
        'options': 'list<LeadGenQuestionOption>',
        'type': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


