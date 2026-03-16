# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

from facebook_business.adobjects.abstractobject import AbstractObject
from facebook_business.adobjects.abstractcrudobject import AbstractCrudObject
from facebook_business.adobjects.objectparser import ObjectParser
from facebook_business.api import FacebookRequest
from facebook_business.typechecker import TypeChecker
from facebook_business.mixins import HasAdLabels
from facebook_business.mixins import CanValidate

"""
This class is auto-generated.

For any issues or feature requests related to this class, please let us know on
github and we'll fix in our codegen framework. We'll not be able to accept
pull request for this class.
"""

class Campaign(
    AbstractCrudObject,
    HasAdLabels,
    CanValidate,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isCampaign = True
        super(Campaign, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        account_id = 'account_id'
        adlabels = 'adlabels'
        advantage_state_info = 'advantage_state_info'
        bid_strategy = 'bid_strategy'
        boosted_object_id = 'boosted_object_id'
        brand_lift_studies = 'brand_lift_studies'
        budget_rebalance_flag = 'budget_rebalance_flag'
        budget_remaining = 'budget_remaining'
        buying_type = 'buying_type'
        campaign_group_active_time = 'campaign_group_active_time'
        can_create_brand_lift_study = 'can_create_brand_lift_study'
        can_use_spend_cap = 'can_use_spend_cap'
        configured_status = 'configured_status'
        created_time = 'created_time'
        daily_budget = 'daily_budget'
        effective_status = 'effective_status'
        has_secondary_skadnetwork_reporting = 'has_secondary_skadnetwork_reporting'
        id = 'id'
        is_adset_budget_sharing_enabled = 'is_adset_budget_sharing_enabled'
        is_budget_schedule_enabled = 'is_budget_schedule_enabled'
        is_direct_send_campaign = 'is_direct_send_campaign'
        is_message_campaign = 'is_message_campaign'
        is_skadnetwork_attribution = 'is_skadnetwork_attribution'
        issues_info = 'issues_info'
        last_budget_toggling_time = 'last_budget_toggling_time'
        lifetime_budget = 'lifetime_budget'
        name = 'name'
        objective = 'objective'
        pacing_type = 'pacing_type'
        primary_attribution = 'primary_attribution'
        promoted_object = 'promoted_object'
        recommendations = 'recommendations'
        smart_promotion_type = 'smart_promotion_type'
        source_campaign = 'source_campaign'
        source_campaign_id = 'source_campaign_id'
        source_recommendation_type = 'source_recommendation_type'
        special_ad_categories = 'special_ad_categories'
        special_ad_category = 'special_ad_category'
        special_ad_category_country = 'special_ad_category_country'
        spend_cap = 'spend_cap'
        start_time = 'start_time'
        status = 'status'
        stop_time = 'stop_time'
        topline_id = 'topline_id'
        updated_time = 'updated_time'
        adbatch = 'adbatch'
        budget_schedule_specs = 'budget_schedule_specs'
        execution_options = 'execution_options'
        iterative_split_test_configs = 'iterative_split_test_configs'

    class BidStrategy:
        cost_cap = 'COST_CAP'
        lowest_cost_without_cap = 'LOWEST_COST_WITHOUT_CAP'
        lowest_cost_with_bid_cap = 'LOWEST_COST_WITH_BID_CAP'
        lowest_cost_with_min_roas = 'LOWEST_COST_WITH_MIN_ROAS'

    class ConfiguredStatus:
        active = 'ACTIVE'
        archived = 'ARCHIVED'
        deleted = 'DELETED'
        paused = 'PAUSED'

    class EffectiveStatus:
        active = 'ACTIVE'
        archived = 'ARCHIVED'
        deleted = 'DELETED'
        in_process = 'IN_PROCESS'
        paused = 'PAUSED'
        with_issues = 'WITH_ISSUES'

    class Status:
        active = 'ACTIVE'
        archived = 'ARCHIVED'
        deleted = 'DELETED'
        paused = 'PAUSED'

    class DatePreset:
        data_maximum = 'data_maximum'
        last_14d = 'last_14d'
        last_28d = 'last_28d'
        last_30d = 'last_30d'
        last_3d = 'last_3d'
        last_7d = 'last_7d'
        last_90d = 'last_90d'
        last_month = 'last_month'
        last_quarter = 'last_quarter'
        last_week_mon_sun = 'last_week_mon_sun'
        last_week_sun_sat = 'last_week_sun_sat'
        last_year = 'last_year'
        maximum = 'maximum'
        this_month = 'this_month'
        this_quarter = 'this_quarter'
        this_week_mon_today = 'this_week_mon_today'
        this_week_sun_today = 'this_week_sun_today'
        this_year = 'this_year'
        today = 'today'
        yesterday = 'yesterday'

    class ExecutionOptions:
        include_recommendations = 'include_recommendations'
        validate_only = 'validate_only'

    class Objective:
        app_installs = 'APP_INSTALLS'
        brand_awareness = 'BRAND_AWARENESS'
        conversions = 'CONVERSIONS'
        event_responses = 'EVENT_RESPONSES'
        lead_generation = 'LEAD_GENERATION'
        link_clicks = 'LINK_CLICKS'
        local_awareness = 'LOCAL_AWARENESS'
        messages = 'MESSAGES'
        offer_claims = 'OFFER_CLAIMS'
        outcome_app_promotion = 'OUTCOME_APP_PROMOTION'
        outcome_awareness = 'OUTCOME_AWARENESS'
        outcome_engagement = 'OUTCOME_ENGAGEMENT'
        outcome_leads = 'OUTCOME_LEADS'
        outcome_sales = 'OUTCOME_SALES'
        outcome_traffic = 'OUTCOME_TRAFFIC'
        page_likes = 'PAGE_LIKES'
        post_engagement = 'POST_ENGAGEMENT'
        product_catalog_sales = 'PRODUCT_CATALOG_SALES'
        reach = 'REACH'
        store_visits = 'STORE_VISITS'
        video_views = 'VIDEO_VIEWS'

    class SmartPromotionType:
        guided_creation = 'GUIDED_CREATION'
        smart_app_promotion = 'SMART_APP_PROMOTION'

    class SpecialAdCategories:
        credit = 'CREDIT'
        employment = 'EMPLOYMENT'
        financial_products_services = 'FINANCIAL_PRODUCTS_SERVICES'
        housing = 'HOUSING'
        issues_elections_politics = 'ISSUES_ELECTIONS_POLITICS'
        none = 'NONE'
        online_gambling_and_gaming = 'ONLINE_GAMBLING_AND_GAMING'

    class SpecialAdCategoryCountry:
        ac = 'AC'
        ad = 'AD'
        ae = 'AE'
        af = 'AF'
        ag = 'AG'
        ai = 'AI'
        al = 'AL'
        am = 'AM'
        an = 'AN'
        ao = 'AO'
        aq = 'AQ'
        ar = 'AR'
        value_as = 'AS'
        at = 'AT'
        au = 'AU'
        aw = 'AW'
        ax = 'AX'
        az = 'AZ'
        ba = 'BA'
        bb = 'BB'
        bd = 'BD'
        be = 'BE'
        bf = 'BF'
        bg = 'BG'
        bh = 'BH'
        bi = 'BI'
        bj = 'BJ'
        bl = 'BL'
        bm = 'BM'
        bn = 'BN'
        bo = 'BO'
        bq = 'BQ'
        br = 'BR'
        bs = 'BS'
        bt = 'BT'
        bv = 'BV'
        bw = 'BW'
        by = 'BY'
        bz = 'BZ'
        ca = 'CA'
        cc = 'CC'
        cd = 'CD'
        cf = 'CF'
        cg = 'CG'
        ch = 'CH'
        ci = 'CI'
        ck = 'CK'
        cl = 'CL'
        cm = 'CM'
        cn = 'CN'
        co = 'CO'
        cr = 'CR'
        cu = 'CU'
        cv = 'CV'
        cw = 'CW'
        cx = 'CX'
        cy = 'CY'
        cz = 'CZ'
        de = 'DE'
        dj = 'DJ'
        dk = 'DK'
        dm = 'DM'
        do = 'DO'
        dz = 'DZ'
        ec = 'EC'
        ee = 'EE'
        eg = 'EG'
        eh = 'EH'
        er = 'ER'
        es = 'ES'
        et = 'ET'
        fi = 'FI'
        fj = 'FJ'
        fk = 'FK'
        fm = 'FM'
        fo = 'FO'
        fr = 'FR'
        ga = 'GA'
        gb = 'GB'
        gd = 'GD'
        ge = 'GE'
        gf = 'GF'
        gg = 'GG'
        gh = 'GH'
        gi = 'GI'
        gl = 'GL'
        gm = 'GM'
        gn = 'GN'
        gp = 'GP'
        gq = 'GQ'
        gr = 'GR'
        gs = 'GS'
        gt = 'GT'
        gu = 'GU'
        gw = 'GW'
        gy = 'GY'
        hk = 'HK'
        hm = 'HM'
        hn = 'HN'
        hr = 'HR'
        ht = 'HT'
        hu = 'HU'
        id = 'ID'
        ie = 'IE'
        il = 'IL'
        im = 'IM'
        value_in = 'IN'
        io = 'IO'
        iq = 'IQ'
        ir = 'IR'
        value_is = 'IS'
        it = 'IT'
        je = 'JE'
        jm = 'JM'
        jo = 'JO'
        jp = 'JP'
        ke = 'KE'
        kg = 'KG'
        kh = 'KH'
        ki = 'KI'
        km = 'KM'
        kn = 'KN'
        kp = 'KP'
        kr = 'KR'
        kw = 'KW'
        ky = 'KY'
        kz = 'KZ'
        la = 'LA'
        lb = 'LB'
        lc = 'LC'
        li = 'LI'
        lk = 'LK'
        lr = 'LR'
        ls = 'LS'
        lt = 'LT'
        lu = 'LU'
        lv = 'LV'
        ly = 'LY'
        ma = 'MA'
        mc = 'MC'
        md = 'MD'
        me = 'ME'
        mf = 'MF'
        mg = 'MG'
        mh = 'MH'
        mk = 'MK'
        ml = 'ML'
        mm = 'MM'
        mn = 'MN'
        mo = 'MO'
        mp = 'MP'
        mq = 'MQ'
        mr = 'MR'
        ms = 'MS'
        mt = 'MT'
        mu = 'MU'
        mv = 'MV'
        mw = 'MW'
        mx = 'MX'
        my = 'MY'
        mz = 'MZ'
        na = 'NA'
        nc = 'NC'
        ne = 'NE'
        nf = 'NF'
        ng = 'NG'
        ni = 'NI'
        nl = 'NL'
        no = 'NO'
        np = 'NP'
        nr = 'NR'
        nu = 'NU'
        nz = 'NZ'
        om = 'OM'
        pa = 'PA'
        pe = 'PE'
        pf = 'PF'
        pg = 'PG'
        ph = 'PH'
        pk = 'PK'
        pl = 'PL'
        pm = 'PM'
        pn = 'PN'
        pr = 'PR'
        ps = 'PS'
        pt = 'PT'
        pw = 'PW'
        py = 'PY'
        qa = 'QA'
        re = 'RE'
        ro = 'RO'
        rs = 'RS'
        ru = 'RU'
        rw = 'RW'
        sa = 'SA'
        sb = 'SB'
        sc = 'SC'
        sd = 'SD'
        se = 'SE'
        sg = 'SG'
        sh = 'SH'
        si = 'SI'
        sj = 'SJ'
        sk = 'SK'
        sl = 'SL'
        sm = 'SM'
        sn = 'SN'
        so = 'SO'
        sr = 'SR'
        ss = 'SS'
        st = 'ST'
        sv = 'SV'
        sx = 'SX'
        sy = 'SY'
        sz = 'SZ'
        tc = 'TC'
        td = 'TD'
        tf = 'TF'
        tg = 'TG'
        th = 'TH'
        tj = 'TJ'
        tk = 'TK'
        tl = 'TL'
        tm = 'TM'
        tn = 'TN'
        to = 'TO'
        tr = 'TR'
        tt = 'TT'
        tv = 'TV'
        tw = 'TW'
        tz = 'TZ'
        ua = 'UA'
        ug = 'UG'
        um = 'UM'
        us = 'US'
        uy = 'UY'
        uz = 'UZ'
        va = 'VA'
        vc = 'VC'
        ve = 'VE'
        vg = 'VG'
        vi = 'VI'
        vn = 'VN'
        vu = 'VU'
        wf = 'WF'
        ws = 'WS'
        xk = 'XK'
        ye = 'YE'
        yt = 'YT'
        za = 'ZA'
        zm = 'ZM'
        zw = 'ZW'

    class Operator:
        all = 'ALL'
        any = 'ANY'

    class SpecialAdCategory:
        credit = 'CREDIT'
        employment = 'EMPLOYMENT'
        financial_products_services = 'FINANCIAL_PRODUCTS_SERVICES'
        housing = 'HOUSING'
        issues_elections_politics = 'ISSUES_ELECTIONS_POLITICS'
        none = 'NONE'
        online_gambling_and_gaming = 'ONLINE_GAMBLING_AND_GAMING'

    class StatusOption:
        active = 'ACTIVE'
        inherited_from_source = 'INHERITED_FROM_SOURCE'
        paused = 'PAUSED'

    # @deprecated get_endpoint function is deprecated
    @classmethod
    def get_endpoint(cls):
        return 'campaigns'

    # @deprecated api_create is being deprecated
    def api_create(self, parent_id, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.adobjects.adaccount import AdAccount
        return AdAccount(api=self._api, fbid=parent_id).create_campaign(fields, params, batch, success, failure, pending)

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
            'am_call_tags': 'map',
            'date_preset': 'date_preset_enum',
            'from_adtable': 'bool',
            'time_range': 'map',
        }
        enums = {
            'date_preset_enum': [
                'data_maximum',
                'last_14d',
                'last_28d',
                'last_30d',
                'last_3d',
                'last_7d',
                'last_90d',
                'last_month',
                'last_quarter',
                'last_week_mon_sun',
                'last_week_sun_sat',
                'last_year',
                'maximum',
                'this_month',
                'this_quarter',
                'this_week_mon_today',
                'this_week_sun_today',
                'this_year',
                'today',
                'yesterday',
            ],
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Campaign,
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
            'adlabels': 'list<Object>',
            'adset_bid_amounts': 'map',
            'adset_budgets': 'list<map>',
            'bid_strategy': 'bid_strategy_enum',
            'budget_rebalance_flag': 'bool',
            'budget_schedule_specs': 'list<Object>',
            'daily_budget': 'unsigned int',
            'execution_options': 'list<execution_options_enum>',
            'is_adset_budget_sharing_enabled': 'bool',
            'is_budget_schedule_enabled': 'bool',
            'is_direct_send_campaign': 'bool',
            'is_message_campaign': 'bool',
            'is_skadnetwork_attribution': 'bool',
            'iterative_split_test_configs': 'list<Object>',
            'lifetime_budget': 'unsigned int',
            'migrate_to_advantage_plus': 'bool',
            'name': 'string',
            'objective': 'objective_enum',
            'pacing_type': 'list<string>',
            'promoted_object': 'Object',
            'smart_promotion_type': 'smart_promotion_type_enum',
            'special_ad_categories': 'list<special_ad_categories_enum>',
            'special_ad_category': 'special_ad_category_enum',
            'special_ad_category_country': 'list<special_ad_category_country_enum>',
            'spend_cap': 'unsigned int',
            'start_time': 'datetime',
            'status': 'status_enum',
            'stop_time': 'datetime',
        }
        enums = {
            'bid_strategy_enum': Campaign.BidStrategy.__dict__.values(),
            'execution_options_enum': Campaign.ExecutionOptions.__dict__.values(),
            'objective_enum': Campaign.Objective.__dict__.values(),
            'smart_promotion_type_enum': Campaign.SmartPromotionType.__dict__.values(),
            'special_ad_categories_enum': Campaign.SpecialAdCategories.__dict__.values(),
            'special_ad_category_enum': Campaign.SpecialAdCategory.__dict__.values(),
            'special_ad_category_country_enum': Campaign.SpecialAdCategoryCountry.__dict__.values(),
            'status_enum': Campaign.Status.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Campaign,
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

    def get_ad_studies(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adstudy import AdStudy
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/ad_studies',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdStudy,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdStudy, api=self._api),
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

    def create_ad_label(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'adlabels': 'list<Object>',
            'execution_options': 'list<execution_options_enum>',
        }
        enums = {
            'execution_options_enum': Campaign.ExecutionOptions.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/adlabels',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Campaign,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Campaign, api=self._api),
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

    def get_ad_rules_governed(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adrule import AdRule
        param_types = {
            'pass_evaluation': 'bool',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/adrules_governed',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdRule,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdRule, api=self._api),
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

    def get_ads(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.ad import Ad
        param_types = {
            'date_preset': 'date_preset_enum',
            'effective_status': 'list<string>',
            'time_range': 'map',
            'updated_since': 'int',
        }
        enums = {
            'date_preset_enum': Ad.DatePreset.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/ads',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Ad,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Ad, api=self._api),
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

    def get_ad_sets(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adset import AdSet
        param_types = {
            'date_preset': 'date_preset_enum',
            'effective_status': 'list<effective_status_enum>',
            'is_completed': 'bool',
            'time_range': 'map',
        }
        enums = {
            'date_preset_enum': AdSet.DatePreset.__dict__.values(),
            'effective_status_enum': AdSet.EffectiveStatus.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/adsets',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdSet,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdSet, api=self._api),
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

    def get_budget_schedules(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.highdemandperiod import HighDemandPeriod
        param_types = {
            'time_start': 'datetime',
            'time_stop': 'datetime',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/budget_schedules',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=HighDemandPeriod,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=HighDemandPeriod, api=self._api),
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

    def create_budget_schedule(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.highdemandperiod import HighDemandPeriod
        param_types = {
            'budget_value': 'unsigned int',
            'budget_value_type': 'budget_value_type_enum',
            'time_end': 'unsigned int',
            'time_start': 'unsigned int',
        }
        enums = {
            'budget_value_type_enum': HighDemandPeriod.BudgetValueType.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/budget_schedules',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=HighDemandPeriod,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=HighDemandPeriod, api=self._api),
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

    def get_copies(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'date_preset': 'date_preset_enum',
            'effective_status': 'list<effective_status_enum>',
            'is_completed': 'bool',
            'time_range': 'map',
        }
        enums = {
            'date_preset_enum': Campaign.DatePreset.__dict__.values(),
            'effective_status_enum': Campaign.EffectiveStatus.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/copies',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Campaign,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Campaign, api=self._api),
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

    def create_copy(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'deep_copy': 'bool',
            'end_time': 'datetime',
            'migrate_to_advantage_plus': 'bool',
            'parameter_overrides': 'Object',
            'rename_options': 'Object',
            'start_time': 'datetime',
            'status_option': 'status_option_enum',
        }
        enums = {
            'status_option_enum': Campaign.StatusOption.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/copies',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Campaign,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Campaign, api=self._api),
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

    def get_insights(self, fields=None, params=None, is_async=False, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adsinsights import AdsInsights
        if is_async:
          return self.get_insights_async(fields, params, batch, success, failure, pending)
        param_types = {
            'action_attribution_windows': 'list<action_attribution_windows_enum>',
            'action_breakdowns': 'list<action_breakdowns_enum>',
            'action_report_time': 'action_report_time_enum',
            'breakdowns': 'list<breakdowns_enum>',
            'date_preset': 'date_preset_enum',
            'default_summary': 'bool',
            'export_columns': 'list<string>',
            'export_format': 'string',
            'export_name': 'string',
            'fields': 'list<string>',
            'filtering': 'list<Object>',
            'graph_cache': 'bool',
            'level': 'level_enum',
            'limit': 'int',
            'product_id_limit': 'int',
            'sort': 'list<string>',
            'summary': 'list<string>',
            'summary_action_breakdowns': 'list<summary_action_breakdowns_enum>',
            'time_increment': 'string',
            'time_range': 'map',
            'time_ranges': 'list<map>',
            'use_account_attribution_setting': 'bool',
            'use_unified_attribution_setting': 'bool',
        }
        enums = {
            'action_attribution_windows_enum': AdsInsights.ActionAttributionWindows.__dict__.values(),
            'action_breakdowns_enum': AdsInsights.ActionBreakdowns.__dict__.values(),
            'action_report_time_enum': AdsInsights.ActionReportTime.__dict__.values(),
            'breakdowns_enum': AdsInsights.Breakdowns.__dict__.values(),
            'date_preset_enum': AdsInsights.DatePreset.__dict__.values(),
            'level_enum': AdsInsights.Level.__dict__.values(),
            'summary_action_breakdowns_enum': AdsInsights.SummaryActionBreakdowns.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/insights',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdsInsights,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdsInsights, api=self._api),
            include_summary=False,
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

    def get_insights_async(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adreportrun import AdReportRun
        from facebook_business.adobjects.adsinsights import AdsInsights
        param_types = {
            'action_attribution_windows': 'list<action_attribution_windows_enum>',
            'action_breakdowns': 'list<action_breakdowns_enum>',
            'action_report_time': 'action_report_time_enum',
            'breakdowns': 'list<breakdowns_enum>',
            'date_preset': 'date_preset_enum',
            'default_summary': 'bool',
            'export_columns': 'list<string>',
            'export_format': 'string',
            'export_name': 'string',
            'fields': 'list<string>',
            'filtering': 'list<Object>',
            'graph_cache': 'bool',
            'level': 'level_enum',
            'limit': 'int',
            'product_id_limit': 'int',
            'sort': 'list<string>',
            'summary': 'list<string>',
            'summary_action_breakdowns': 'list<summary_action_breakdowns_enum>',
            'time_increment': 'string',
            'time_range': 'map',
            'time_ranges': 'list<map>',
            'use_account_attribution_setting': 'bool',
            'use_unified_attribution_setting': 'bool',
        }
        enums = {
            'action_attribution_windows_enum': AdsInsights.ActionAttributionWindows.__dict__.values(),
            'action_breakdowns_enum': AdsInsights.ActionBreakdowns.__dict__.values(),
            'action_report_time_enum': AdsInsights.ActionReportTime.__dict__.values(),
            'breakdowns_enum': AdsInsights.Breakdowns.__dict__.values(),
            'date_preset_enum': AdsInsights.DatePreset.__dict__.values(),
            'level_enum': AdsInsights.Level.__dict__.values(),
            'summary_action_breakdowns_enum': AdsInsights.SummaryActionBreakdowns.__dict__.values(),
        }

        if fields is not None:
            params['fields'] = params.get('fields') if params.get('fields') is not None else list()
            params['fields'].extend(field for field in fields if field not in params['fields'])

        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/insights',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdReportRun,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdReportRun, api=self._api),
            include_summary=False,
        )
        request.add_params(params)

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
        'adlabels': 'list<AdLabel>',
        'advantage_state_info': 'AdCampaignGroupAdvantageState',
        'bid_strategy': 'BidStrategy',
        'boosted_object_id': 'string',
        'brand_lift_studies': 'list<AdStudy>',
        'budget_rebalance_flag': 'bool',
        'budget_remaining': 'string',
        'buying_type': 'string',
        'campaign_group_active_time': 'string',
        'can_create_brand_lift_study': 'bool',
        'can_use_spend_cap': 'bool',
        'configured_status': 'ConfiguredStatus',
        'created_time': 'datetime',
        'daily_budget': 'string',
        'effective_status': 'EffectiveStatus',
        'has_secondary_skadnetwork_reporting': 'bool',
        'id': 'string',
        'is_adset_budget_sharing_enabled': 'bool',
        'is_budget_schedule_enabled': 'bool',
        'is_direct_send_campaign': 'bool',
        'is_message_campaign': 'bool',
        'is_skadnetwork_attribution': 'bool',
        'issues_info': 'list<AdCampaignIssuesInfo>',
        'last_budget_toggling_time': 'datetime',
        'lifetime_budget': 'string',
        'name': 'string',
        'objective': 'string',
        'pacing_type': 'list<string>',
        'primary_attribution': 'string',
        'promoted_object': 'AdPromotedObject',
        'recommendations': 'list<AdRecommendation>',
        'smart_promotion_type': 'string',
        'source_campaign': 'Campaign',
        'source_campaign_id': 'string',
        'source_recommendation_type': 'string',
        'special_ad_categories': 'list<string>',
        'special_ad_category': 'string',
        'special_ad_category_country': 'list<string>',
        'spend_cap': 'string',
        'start_time': 'datetime',
        'status': 'Status',
        'stop_time': 'datetime',
        'topline_id': 'string',
        'updated_time': 'datetime',
        'adbatch': 'list<Object>',
        'budget_schedule_specs': 'list<Object>',
        'execution_options': 'list<ExecutionOptions>',
        'iterative_split_test_configs': 'list<Object>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['BidStrategy'] = Campaign.BidStrategy.__dict__.values()
        field_enum_info['ConfiguredStatus'] = Campaign.ConfiguredStatus.__dict__.values()
        field_enum_info['EffectiveStatus'] = Campaign.EffectiveStatus.__dict__.values()
        field_enum_info['Status'] = Campaign.Status.__dict__.values()
        field_enum_info['DatePreset'] = Campaign.DatePreset.__dict__.values()
        field_enum_info['ExecutionOptions'] = Campaign.ExecutionOptions.__dict__.values()
        field_enum_info['Objective'] = Campaign.Objective.__dict__.values()
        field_enum_info['SmartPromotionType'] = Campaign.SmartPromotionType.__dict__.values()
        field_enum_info['SpecialAdCategories'] = Campaign.SpecialAdCategories.__dict__.values()
        field_enum_info['SpecialAdCategoryCountry'] = Campaign.SpecialAdCategoryCountry.__dict__.values()
        field_enum_info['Operator'] = Campaign.Operator.__dict__.values()
        field_enum_info['SpecialAdCategory'] = Campaign.SpecialAdCategory.__dict__.values()
        field_enum_info['StatusOption'] = Campaign.StatusOption.__dict__.values()
        return field_enum_info


