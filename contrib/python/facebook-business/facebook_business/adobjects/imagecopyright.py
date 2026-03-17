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

class ImageCopyright(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isImageCopyright = True
        super(ImageCopyright, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        artist = 'artist'
        copyright_monitoring_status = 'copyright_monitoring_status'
        creation_time = 'creation_time'
        creator = 'creator'
        custom_id = 'custom_id'
        description = 'description'
        filename = 'filename'
        id = 'id'
        image = 'image'
        matches_count = 'matches_count'
        original_content_creation_date = 'original_content_creation_date'
        ownership_countries = 'ownership_countries'
        tags = 'tags'
        title = 'title'
        update_time = 'update_time'

    class GeoOwnership:
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
        tp = 'TP'
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
            target_class=ImageCopyright,
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
            'artist': 'string',
            'creator': 'string',
            'custom_id': 'string',
            'description': 'string',
            'geo_ownership': 'list<geo_ownership_enum>',
            'original_content_creation_date': 'unsigned int',
            'title': 'string',
        }
        enums = {
            'geo_ownership_enum': ImageCopyright.GeoOwnership.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ImageCopyright,
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
        'artist': 'string',
        'copyright_monitoring_status': 'string',
        'creation_time': 'datetime',
        'creator': 'string',
        'custom_id': 'string',
        'description': 'string',
        'filename': 'string',
        'id': 'string',
        'image': 'Photo',
        'matches_count': 'unsigned int',
        'original_content_creation_date': 'datetime',
        'ownership_countries': 'VideoCopyrightGeoGate',
        'tags': 'list<string>',
        'title': 'string',
        'update_time': 'datetime',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['GeoOwnership'] = ImageCopyright.GeoOwnership.__dict__.values()
        return field_enum_info


