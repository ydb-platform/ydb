# -*- coding: utf-8 -*-
''' 24 solar terms are defined here.
About how to caculate 24 solar terms:
* https://en.wikipedia.org/wiki/Solar_term
* https://github.com/hotoo/nong/wiki/%E7%AE%97%E6%B3%95%E7%B3%BB%E5%88%97%E4%B9%8B%E5%8D%81%E5%85%AB%EF%BC%9A%E7%94%A8%E5%A4%A9%E6%96%87%E6%96%B9%E6%B3%95%E8%AE%A1%E7%AE%97%E4%BA%8C%E5%8D%81%E5%9B%9B%E8%8A%82%E6%B0%94%EF%BC%88%E4%B8%8A%EF%BC%89
* https://github.com/hotoo/nong/wiki/%E7%AE%97%E6%B3%95%E7%B3%BB%E5%88%97%E4%B9%8B%E5%8D%81%E5%85%AB%EF%BC%9A%E7%94%A8%E5%A4%A9%E6%96%87%E6%96%B9%E6%B3%95%E8%AE%A1%E7%AE%97%E4%BA%8C%E5%8D%81%E5%9B%9B%E8%8A%82%E6%B0%94%EF%BC%88%E4%B8%8B%EF%BC%89
'''

from __future__ import unicode_literals
import pytz
from ._calc import specified_solar_term
from .basefestival import Festival


en_terms = 'start of spring|rain water|awakening of insects|vernal equinox|clear and bright|grain rain|start of summer|grain full|grain in ear|summer solstice|minor heat|major heat|start of autumn|limit of heat|white dew|autumnal equinox|cold dew|frost descent|start of winter|minor snow|major snow|winter solstice|minor cold|major cold'.split('|')

zh_hans_terms = '立春|雨水|惊蛰|春分|清明|谷雨|立夏|小满|芒种|夏至|小暑|大暑|立秋|处暑|白露|秋分|寒露|霜降|立冬|小雪|大雪|冬至|小寒|大寒'.split('|')

zh_hant_terms = '立春|雨水|驚蟄|春分|清明|穀雨|立夏|小滿|芒種|夏至|小暑|大暑|立秋|處暑|白露|秋分|寒露|霜降|立冬|小雪|大雪|冬至|小寒|大寒'.split('|')

ja_terms = '立春|雨水|啓蟄|春分|清明|穀雨|立夏|小満|芒種|夏至|小暑|大暑|立秋|処暑|白露|秋分|寒露|霜降|立冬|小雪|大雪|冬至|小寒|大寒'.split('|')

ko_terms = '입춘|우수|경칩|춘분|청명|곡우|입하|소만|망종|하지|소서|대서|입추|처서|백로|추분|한로|상강|입동|소설|대설|동지|소한|대한'.split('|')

vi_terms = 'Lập xuân|Vũ thủy|Kinh trập|Xuân phân|Thanh minh|Cốc vũ|Lập hạ|Tiểu mãn|Mang chủng|Hạ chí|Tiểu thử|Đại thử|Lập thu|Xử thử|Bạch lộ|Thu phân|Hàn lộ|Sương giáng|Lập đông|Tiểu tuyết|Đại tuyết|Đông chí|Tiểu hàn|Đại hàn'.split('|')


solarterms = []
for i in range(0, 24):
    # minor cold(小寒) and major cold(大寒) should decrease by one year: year - 1
    # other should be normal: year
    offset = 1 if i >= 22 else 0
    solarterms.append(Festival(
        lambda year, _i=i, _offset=offset: specified_solar_term(
            year-_offset, _i).astimezone(tz=pytz.timezone('Asia/Shanghai')).date(),
        en=en_terms[i],
        zh_hans=zh_hans_terms[i],
        zh_hant=zh_hant_terms[i],
        ja=ja_terms[i],
        ko=ko_terms[i],
        vi=vi_terms[i],
    ))

zh_solarterms = solarterms
ja_solarterms = solarterms
ko_solarterms = solarterms
vi_solarterms = solarterms

LiChun, YuShui, JingZhe, ChunFen, QingMing, GuYu, \
    LiXia, XiaoMan, MangZhong, XiaZhi, XiaoShu, DaShu, \
    LiQiu, ChuShu, BaiLu, QiuFen, HanLu, ShuangJiang, \
    LiDong, XiaoXue, DaXue, DongZhi, XiaoHan, DaHan = solarterms
