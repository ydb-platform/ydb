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

class DeliveryWindow(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isDeliveryWindow = True
        super(DeliveryWindow, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        ac = 'ac'
        ad = 'ad'
        ae = 'ae'
        af = 'af'
        ag = 'ag'
        ai = 'ai'
        al = 'al'
        all = 'all'
        am = 'am'
        an = 'an'
        ao = 'ao'
        aq = 'aq'
        ar = 'ar'
        field_as = 'as'
        at = 'at'
        au = 'au'
        aw = 'aw'
        ax = 'ax'
        az = 'az'
        ba = 'ba'
        bb = 'bb'
        bd = 'bd'
        be = 'be'
        bf = 'bf'
        bg = 'bg'
        bh = 'bh'
        bi = 'bi'
        bj = 'bj'
        bl = 'bl'
        bm = 'bm'
        bn = 'bn'
        bo = 'bo'
        bq = 'bq'
        br = 'br'
        bs = 'bs'
        bt = 'bt'
        bv = 'bv'
        bw = 'bw'
        by = 'by'
        bz = 'bz'
        ca = 'ca'
        cc = 'cc'
        cd = 'cd'
        cf = 'cf'
        cg = 'cg'
        ch = 'ch'
        ci = 'ci'
        ck = 'ck'
        cl = 'cl'
        cm = 'cm'
        cn = 'cn'
        co = 'co'
        cr = 'cr'
        cu = 'cu'
        cv = 'cv'
        cw = 'cw'
        cx = 'cx'
        cy = 'cy'
        cz = 'cz'
        de = 'de'
        dj = 'dj'
        dk = 'dk'
        dm = 'dm'
        do = 'do'
        dz = 'dz'
        ec = 'ec'
        ee = 'ee'
        eg = 'eg'
        eh = 'eh'
        er = 'er'
        es = 'es'
        et = 'et'
        fi = 'fi'
        fj = 'fj'
        fk = 'fk'
        fm = 'fm'
        fo = 'fo'
        fr = 'fr'
        ga = 'ga'
        gb = 'gb'
        gd = 'gd'
        ge = 'ge'
        gf = 'gf'
        gg = 'gg'
        gh = 'gh'
        gi = 'gi'
        gl = 'gl'
        gm = 'gm'
        gn = 'gn'
        gp = 'gp'
        gq = 'gq'
        gr = 'gr'
        gs = 'gs'
        gt = 'gt'
        gu = 'gu'
        gw = 'gw'
        gy = 'gy'
        hk = 'hk'
        hm = 'hm'
        hn = 'hn'
        hr = 'hr'
        ht = 'ht'
        hu = 'hu'
        id = 'id'
        ie = 'ie'
        il = 'il'
        im = 'im'
        field_in = 'in'
        io = 'io'
        iq = 'iq'
        ir = 'ir'
        field_is = 'is'
        it = 'it'
        je = 'je'
        jm = 'jm'
        jo = 'jo'
        jp = 'jp'
        ke = 'ke'
        kg = 'kg'
        kh = 'kh'
        ki = 'ki'
        km = 'km'
        kn = 'kn'
        kp = 'kp'
        kr = 'kr'
        kw = 'kw'
        ky = 'ky'
        kz = 'kz'
        la = 'la'
        lb = 'lb'
        lc = 'lc'
        li = 'li'
        lk = 'lk'
        lr = 'lr'
        ls = 'ls'
        lt = 'lt'
        lu = 'lu'
        lv = 'lv'
        ly = 'ly'
        ma = 'ma'
        mc = 'mc'
        md = 'md'
        me = 'me'
        mf = 'mf'
        mg = 'mg'
        mh = 'mh'
        mk = 'mk'
        ml = 'ml'
        mm = 'mm'
        mn = 'mn'
        mo = 'mo'
        mp = 'mp'
        mq = 'mq'
        mr = 'mr'
        ms = 'ms'
        mt = 'mt'
        mu = 'mu'
        mv = 'mv'
        mw = 'mw'
        mx = 'mx'
        my = 'my'
        mz = 'mz'
        na = 'na'
        nc = 'nc'
        ne = 'ne'
        nf = 'nf'
        ng = 'ng'
        ni = 'ni'
        nl = 'nl'
        no = 'no'
        np = 'np'
        nr = 'nr'
        nu = 'nu'
        nz = 'nz'
        om = 'om'
        pa = 'pa'
        pe = 'pe'
        pf = 'pf'
        pg = 'pg'
        ph = 'ph'
        pk = 'pk'
        pl = 'pl'
        pm = 'pm'
        pn = 'pn'
        pr = 'pr'
        ps = 'ps'
        pt = 'pt'
        pw = 'pw'
        py = 'py'
        qa = 'qa'
        re = 're'
        ro = 'ro'
        rs = 'rs'
        ru = 'ru'
        rw = 'rw'
        sa = 'sa'
        sb = 'sb'
        sc = 'sc'
        sd = 'sd'
        se = 'se'
        sg = 'sg'
        sh = 'sh'
        si = 'si'
        sj = 'sj'
        sk = 'sk'
        sl = 'sl'
        sm = 'sm'
        sn = 'sn'
        so = 'so'
        sr = 'sr'
        ss = 'ss'
        st = 'st'
        sv = 'sv'
        sx = 'sx'
        sy = 'sy'
        sz = 'sz'
        tc = 'tc'
        td = 'td'
        tf = 'tf'
        tg = 'tg'
        th = 'th'
        tj = 'tj'
        tk = 'tk'
        tl = 'tl'
        tm = 'tm'
        tn = 'tn'
        to = 'to'
        tr = 'tr'
        tt = 'tt'
        tv = 'tv'
        tw = 'tw'
        tz = 'tz'
        ua = 'ua'
        ug = 'ug'
        um = 'um'
        us = 'us'
        uy = 'uy'
        uz = 'uz'
        va = 'va'
        vc = 'vc'
        ve = 've'
        vg = 'vg'
        vi = 'vi'
        vn = 'vn'
        vu = 'vu'
        wf = 'wf'
        ws = 'ws'
        xk = 'xk'
        ye = 'ye'
        yt = 'yt'
        za = 'za'
        zm = 'zm'
        zw = 'zw'

    _field_types = {
        'ac': 'int',
        'ad': 'int',
        'ae': 'int',
        'af': 'int',
        'ag': 'int',
        'ai': 'int',
        'al': 'int',
        'all': 'int',
        'am': 'int',
        'an': 'int',
        'ao': 'int',
        'aq': 'int',
        'ar': 'int',
        'as': 'int',
        'at': 'int',
        'au': 'int',
        'aw': 'int',
        'ax': 'int',
        'az': 'int',
        'ba': 'int',
        'bb': 'int',
        'bd': 'int',
        'be': 'int',
        'bf': 'int',
        'bg': 'int',
        'bh': 'int',
        'bi': 'int',
        'bj': 'int',
        'bl': 'int',
        'bm': 'int',
        'bn': 'int',
        'bo': 'int',
        'bq': 'int',
        'br': 'int',
        'bs': 'int',
        'bt': 'int',
        'bv': 'int',
        'bw': 'int',
        'by': 'int',
        'bz': 'int',
        'ca': 'int',
        'cc': 'int',
        'cd': 'int',
        'cf': 'int',
        'cg': 'int',
        'ch': 'int',
        'ci': 'int',
        'ck': 'int',
        'cl': 'int',
        'cm': 'int',
        'cn': 'int',
        'co': 'int',
        'cr': 'int',
        'cu': 'int',
        'cv': 'int',
        'cw': 'int',
        'cx': 'int',
        'cy': 'int',
        'cz': 'int',
        'de': 'int',
        'dj': 'int',
        'dk': 'int',
        'dm': 'int',
        'do': 'int',
        'dz': 'int',
        'ec': 'int',
        'ee': 'int',
        'eg': 'int',
        'eh': 'int',
        'er': 'int',
        'es': 'int',
        'et': 'int',
        'fi': 'int',
        'fj': 'int',
        'fk': 'int',
        'fm': 'int',
        'fo': 'int',
        'fr': 'int',
        'ga': 'int',
        'gb': 'int',
        'gd': 'int',
        'ge': 'int',
        'gf': 'int',
        'gg': 'int',
        'gh': 'int',
        'gi': 'int',
        'gl': 'int',
        'gm': 'int',
        'gn': 'int',
        'gp': 'int',
        'gq': 'int',
        'gr': 'int',
        'gs': 'int',
        'gt': 'int',
        'gu': 'int',
        'gw': 'int',
        'gy': 'int',
        'hk': 'int',
        'hm': 'int',
        'hn': 'int',
        'hr': 'int',
        'ht': 'int',
        'hu': 'int',
        'id': 'int',
        'ie': 'int',
        'il': 'int',
        'im': 'int',
        'in': 'int',
        'io': 'int',
        'iq': 'int',
        'ir': 'int',
        'is': 'int',
        'it': 'int',
        'je': 'int',
        'jm': 'int',
        'jo': 'int',
        'jp': 'int',
        'ke': 'int',
        'kg': 'int',
        'kh': 'int',
        'ki': 'int',
        'km': 'int',
        'kn': 'int',
        'kp': 'int',
        'kr': 'int',
        'kw': 'int',
        'ky': 'int',
        'kz': 'int',
        'la': 'int',
        'lb': 'int',
        'lc': 'int',
        'li': 'int',
        'lk': 'int',
        'lr': 'int',
        'ls': 'int',
        'lt': 'int',
        'lu': 'int',
        'lv': 'int',
        'ly': 'int',
        'ma': 'int',
        'mc': 'int',
        'md': 'int',
        'me': 'int',
        'mf': 'int',
        'mg': 'int',
        'mh': 'int',
        'mk': 'int',
        'ml': 'int',
        'mm': 'int',
        'mn': 'int',
        'mo': 'int',
        'mp': 'int',
        'mq': 'int',
        'mr': 'int',
        'ms': 'int',
        'mt': 'int',
        'mu': 'int',
        'mv': 'int',
        'mw': 'int',
        'mx': 'int',
        'my': 'int',
        'mz': 'int',
        'na': 'int',
        'nc': 'int',
        'ne': 'int',
        'nf': 'int',
        'ng': 'int',
        'ni': 'int',
        'nl': 'int',
        'no': 'int',
        'np': 'int',
        'nr': 'int',
        'nu': 'int',
        'nz': 'int',
        'om': 'int',
        'pa': 'int',
        'pe': 'int',
        'pf': 'int',
        'pg': 'int',
        'ph': 'int',
        'pk': 'int',
        'pl': 'int',
        'pm': 'int',
        'pn': 'int',
        'pr': 'int',
        'ps': 'int',
        'pt': 'int',
        'pw': 'int',
        'py': 'int',
        'qa': 'int',
        're': 'int',
        'ro': 'int',
        'rs': 'int',
        'ru': 'int',
        'rw': 'int',
        'sa': 'int',
        'sb': 'int',
        'sc': 'int',
        'sd': 'int',
        'se': 'int',
        'sg': 'int',
        'sh': 'int',
        'si': 'int',
        'sj': 'int',
        'sk': 'int',
        'sl': 'int',
        'sm': 'int',
        'sn': 'int',
        'so': 'int',
        'sr': 'int',
        'ss': 'int',
        'st': 'int',
        'sv': 'int',
        'sx': 'int',
        'sy': 'int',
        'sz': 'int',
        'tc': 'int',
        'td': 'int',
        'tf': 'int',
        'tg': 'int',
        'th': 'int',
        'tj': 'int',
        'tk': 'int',
        'tl': 'int',
        'tm': 'int',
        'tn': 'int',
        'to': 'int',
        'tr': 'int',
        'tt': 'int',
        'tv': 'int',
        'tw': 'int',
        'tz': 'int',
        'ua': 'int',
        'ug': 'int',
        'um': 'int',
        'us': 'int',
        'uy': 'int',
        'uz': 'int',
        'va': 'int',
        'vc': 'int',
        've': 'int',
        'vg': 'int',
        'vi': 'int',
        'vn': 'int',
        'vu': 'int',
        'wf': 'int',
        'ws': 'int',
        'xk': 'int',
        'ye': 'int',
        'yt': 'int',
        'za': 'int',
        'zm': 'int',
        'zw': 'int',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


