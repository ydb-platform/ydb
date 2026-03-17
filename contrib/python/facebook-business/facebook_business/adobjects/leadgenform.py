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

class LeadgenForm(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isLeadgenForm = True
        super(LeadgenForm, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        allow_organic_lead = 'allow_organic_lead'
        block_display_for_non_targeted_viewer = 'block_display_for_non_targeted_viewer'
        context_card = 'context_card'
        created_time = 'created_time'
        creator = 'creator'
        expired_leads_count = 'expired_leads_count'
        follow_up_action_text = 'follow_up_action_text'
        follow_up_action_url = 'follow_up_action_url'
        id = 'id'
        is_optimized_for_quality = 'is_optimized_for_quality'
        leads_count = 'leads_count'
        legal_content = 'legal_content'
        locale = 'locale'
        name = 'name'
        organic_leads_count = 'organic_leads_count'
        page = 'page'
        page_id = 'page_id'
        privacy_policy_url = 'privacy_policy_url'
        question_page_custom_headline = 'question_page_custom_headline'
        questions = 'questions'
        status = 'status'
        thank_you_page = 'thank_you_page'
        tracking_parameters = 'tracking_parameters'

    class Status:
        active = 'ACTIVE'
        archived = 'ARCHIVED'
        deleted = 'DELETED'
        draft = 'DRAFT'

    class Locale:
        ar_ar = 'AR_AR'
        cs_cz = 'CS_CZ'
        da_dk = 'DA_DK'
        de_de = 'DE_DE'
        el_gr = 'EL_GR'
        en_gb = 'EN_GB'
        en_us = 'EN_US'
        es_es = 'ES_ES'
        es_la = 'ES_LA'
        fi_fi = 'FI_FI'
        fr_fr = 'FR_FR'
        he_il = 'HE_IL'
        hi_in = 'HI_IN'
        hu_hu = 'HU_HU'
        id_id = 'ID_ID'
        it_it = 'IT_IT'
        ja_jp = 'JA_JP'
        ko_kr = 'KO_KR'
        nb_no = 'NB_NO'
        nl_nl = 'NL_NL'
        pl_pl = 'PL_PL'
        pt_br = 'PT_BR'
        pt_pt = 'PT_PT'
        ro_ro = 'RO_RO'
        ru_ru = 'RU_RU'
        sv_se = 'SV_SE'
        th_th = 'TH_TH'
        tr_tr = 'TR_TR'
        vi_vn = 'VI_VN'
        zh_cn = 'ZH_CN'
        zh_hk = 'ZH_HK'
        zh_tw = 'ZH_TW'

    # @deprecated get_endpoint function is deprecated
    @classmethod
    def get_endpoint(cls):
        return 'leadgen_forms'

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
            target_class=LeadgenForm,
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
            'status': 'status_enum',
        }
        enums = {
            'status_enum': LeadgenForm.Status.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=LeadgenForm,
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

    def get_leads(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.lead import Lead
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/leads',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Lead,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Lead, api=self._api),
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

    def get_test_leads(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.lead import Lead
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/test_leads',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Lead,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Lead, api=self._api),
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

    def create_test_lead(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.lead import Lead
        param_types = {
            'custom_disclaimer_responses': 'list<Object>',
            'field_data': 'list<Object>',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/test_leads',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Lead,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Lead, api=self._api),
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
        'allow_organic_lead': 'bool',
        'block_display_for_non_targeted_viewer': 'bool',
        'context_card': 'LeadGenContextCard',
        'created_time': 'datetime',
        'creator': 'User',
        'expired_leads_count': 'unsigned int',
        'follow_up_action_text': 'string',
        'follow_up_action_url': 'string',
        'id': 'string',
        'is_optimized_for_quality': 'bool',
        'leads_count': 'unsigned int',
        'legal_content': 'LeadGenLegalContent',
        'locale': 'string',
        'name': 'string',
        'organic_leads_count': 'unsigned int',
        'page': 'Page',
        'page_id': 'string',
        'privacy_policy_url': 'string',
        'question_page_custom_headline': 'string',
        'questions': 'list<LeadGenQuestion>',
        'status': 'string',
        'thank_you_page': 'LeadGenThankYouPage',
        'tracking_parameters': 'list<KeyValue>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['Status'] = LeadgenForm.Status.__dict__.values()
        field_enum_info['Locale'] = LeadgenForm.Locale.__dict__.values()
        return field_enum_info


