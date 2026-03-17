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

class LeadGenFormPreviewDetails(
    AbstractObject,
):

    def __init__(self, api=None):
        super(LeadGenFormPreviewDetails, self).__init__()
        self._isLeadGenFormPreviewDetails = True
        self._api = api

    class Field(AbstractObject.Field):
        book_on_website_text = 'book_on_website_text'
        call_business_text = 'call_business_text'
        call_to_action_title = 'call_to_action_title'
        chat_on_messenger_text = 'chat_on_messenger_text'
        chat_on_whatsapp_text = 'chat_on_whatsapp_text'
        contact_information_text = 'contact_information_text'
        creatives_overview_default_text = 'creatives_overview_default_text'
        custom_disclaimer_editor_state = 'custom_disclaimer_editor_state'
        custom_disclaimer_title = 'custom_disclaimer_title'
        data_privacy_policy_setting_description = 'data_privacy_policy_setting_description'
        default_appointment_scheduling_inline_context = 'default_appointment_scheduling_inline_context'
        default_disqualified_end_component = 'default_disqualified_end_component'
        default_thank_you_page = 'default_thank_you_page'
        disqualified_thank_you_card_transparency_info_text = 'disqualified_thank_you_card_transparency_info_text'
        edit_text = 'edit_text'
        email_inline_context_text = 'email_inline_context_text'
        email_messenger_push_opt_in_disclaimer = 'email_messenger_push_opt_in_disclaimer'
        email_messenger_push_opt_in_transparency_text = 'email_messenger_push_opt_in_transparency_text'
        form_clarity_description_content = 'form_clarity_description_content'
        form_clarity_description_title = 'form_clarity_description_title'
        form_clarity_headline = 'form_clarity_headline'
        gated_content_locked_description = 'gated_content_locked_description'
        gated_content_locked_headline = 'gated_content_locked_headline'
        gated_content_unlocked_description = 'gated_content_unlocked_description'
        gated_content_unlocked_headline = 'gated_content_unlocked_headline'
        how_it_works_section_headers = 'how_it_works_section_headers'
        next_button_text = 'next_button_text'
        optional_question_text = 'optional_question_text'
        personal_info_text = 'personal_info_text'
        phone_number_inline_context_text = 'phone_number_inline_context_text'
        privacy_policy_link_text = 'privacy_policy_link_text'
        privacy_policy_link_text_for_optional_privacy_policy = 'privacy_policy_link_text_for_optional_privacy_policy'
        privacy_policy_title_section_title_text = 'privacy_policy_title_section_title_text'
        privacy_setting_description = 'privacy_setting_description'
        products_section_headers = 'products_section_headers'
        qualified_thank_you_card_transparency_info_text = 'qualified_thank_you_card_transparency_info_text'
        redeem_promo_code_text = 'redeem_promo_code_text'
        return_to_facebook_text = 'return_to_facebook_text'
        review_your_info_text = 'review_your_info_text'
        secure_sharing_text = 'secure_sharing_text'
        secure_sharing_text_for_optional_privacy_policy = 'secure_sharing_text_for_optional_privacy_policy'
        slide_to_submit_text = 'slide_to_submit_text'
        social_proof_section_headers = 'social_proof_section_headers'
        submit_button_text = 'submit_button_text'
        view_file_text = 'view_file_text'
        whats_app_opt_in_body = 'whats_app_opt_in_body'
        whats_app_opt_in_title = 'whats_app_opt_in_title'

    _field_types = {
        'book_on_website_text': 'string',
        'call_business_text': 'string',
        'call_to_action_title': 'string',
        'chat_on_messenger_text': 'string',
        'chat_on_whatsapp_text': 'string',
        'contact_information_text': 'string',
        'creatives_overview_default_text': 'string',
        'custom_disclaimer_editor_state': 'string',
        'custom_disclaimer_title': 'string',
        'data_privacy_policy_setting_description': 'string',
        'default_appointment_scheduling_inline_context': 'string',
        'default_disqualified_end_component': 'Object',
        'default_thank_you_page': 'Object',
        'disqualified_thank_you_card_transparency_info_text': 'string',
        'edit_text': 'string',
        'email_inline_context_text': 'string',
        'email_messenger_push_opt_in_disclaimer': 'string',
        'email_messenger_push_opt_in_transparency_text': 'string',
        'form_clarity_description_content': 'string',
        'form_clarity_description_title': 'string',
        'form_clarity_headline': 'string',
        'gated_content_locked_description': 'string',
        'gated_content_locked_headline': 'string',
        'gated_content_unlocked_description': 'string',
        'gated_content_unlocked_headline': 'string',
        'how_it_works_section_headers': 'list<map<string, string>>',
        'next_button_text': 'string',
        'optional_question_text': 'string',
        'personal_info_text': 'string',
        'phone_number_inline_context_text': 'string',
        'privacy_policy_link_text': 'string',
        'privacy_policy_link_text_for_optional_privacy_policy': 'string',
        'privacy_policy_title_section_title_text': 'string',
        'privacy_setting_description': 'string',
        'products_section_headers': 'list<map<string, string>>',
        'qualified_thank_you_card_transparency_info_text': 'string',
        'redeem_promo_code_text': 'string',
        'return_to_facebook_text': 'string',
        'review_your_info_text': 'string',
        'secure_sharing_text': 'string',
        'secure_sharing_text_for_optional_privacy_policy': 'string',
        'slide_to_submit_text': 'string',
        'social_proof_section_headers': 'list<map<string, string>>',
        'submit_button_text': 'string',
        'view_file_text': 'string',
        'whats_app_opt_in_body': 'string',
        'whats_app_opt_in_title': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


