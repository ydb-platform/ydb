# -*- coding: utf-8 -*-
''' all included festivals are defined here '''
from __future__ import unicode_literals
import datetime
from dateutil.easter import easter
from dateutil.rrule import rrule, MONTHLY, SU, TH
from .converter import Lunar
from .basefestival import Festival
from . import solarterm


# ######### Solar Festival ##########


NewYear = Festival(
        lambda year: datetime.date(year, 1, 1),
        en="New Year's Day",
        zh_hans="元旦,新年",
        zh_hant="元旦,新年",
        )

Valentine = Festival(
        lambda year: datetime.date(year, 2, 14),
        en="Valentine's Day",
        zh_hans="情人节",
        zh_hant="情人節",
        )

WomenDay = Festival(
        lambda year: datetime.date(year, 3, 8),
        en="Women's Day",
        zh_hans="妇女节",
        zh_hant="婦女節",
        )

ArborDay = Festival(
        lambda year: datetime.date(year, 3, 12),
        en="Arbor Day",
        zh_hans="植树节",
        zh_hant="植樹節",
        )

ChingMing = Festival(
        lambda year: solarterm.QingMing(year),
        en="Ching Ming Festival",
        zh_hans="清明节,踏青节",
        zh_hant="清明節,踏青節",
        )

LabourDay = Festival(
        lambda year: datetime.date(year, 5, 1),
        en="Labour Day",
        zh_hans="劳动节",
        zh_hant="勞動節",
        )

YouthDay = Festival(
        lambda year: datetime.date(year, 5, 4),
        en="Youth Day in China",
        zh_hans="青年节",
        zh_hant="青年節",
        )

NurseDay = Festival(
        lambda year: datetime.date(year, 5, 12),
        en="International Nurses Day",
        zh_hans="护士节",
        zh_hant="護士節",
        )

MotherDay = Festival(
        lambda year: rrule(MONTHLY, count=1, bymonth=5, byweekday=SU(2), dtstart=datetime.datetime(year, 5, 1))[0].date(),
        en="Mother's Day",
        zh_hans="母亲节",
        zh_hant="母親節",
        )

ChildrenDay = Festival(
        lambda year: datetime.date(year, 6, 1),
        en="Children's Day",
        zh_hans="儿童节",
        zh_hant="兒童節",
        )

FatherDay = Festival(
        lambda year: rrule(MONTHLY, count=1, bymonth=6, byweekday=SU(3), dtstart=datetime.datetime(year, 6, 1))[0].date(),
        en="Father's Day",
        zh_hans="父亲节",
        zh_hant="父親節",
        )

TeacherDay = Festival(
        lambda year: datetime.date(year, 9, 10),
        en="Teacher's Day",
        zh_hans="教师节,老师节",
        zh_hant="教師節,老師節",
        )

NationDay = Festival(
        lambda year: datetime.date(year, 10, 1),
        en="National Day of the People's Republic of China",
        zh_hans="国庆节",
        zh_hant="國慶節",
        )

Halloween = Festival(
        lambda year: datetime.date(year, 10, 31),
        en="Halloween",
        zh_hans="万圣夜,万圣节前夜,万鬼节",
        zh_hant="萬聖夜,萬聖節前夜,萬鬼節",
        )

Thanksgiving = Festival(
        lambda year: rrule(MONTHLY, count=1, bymonth=11, byweekday=TH(4), dtstart=datetime.datetime(year, 11, 1))[0].date(),
        en="Thanksgiving Day",
        zh_hans="感恩节",
        zh_hant="感恩節",
        )

ChristmasEve = Festival(
        lambda year: datetime.date(year, 12, 24),
        en="Christmas Eve",
        zh_hans="平安夜,圣诞夜,圣诞节前夕",
        zh_hant="平安夜,聖誕夜,聖誕節前夕",
        )

ChristmasDay = Festival(
        lambda year: datetime.date(year, 12, 25),
        en="Christmas Day",
        zh_hans="圣诞节,圣诞,耶诞节,基督弥撒",
        zh_hant="聖誕節,聖誕,耶誕節,基督彌撒",
        )

Easter = Festival(
        lambda year: easter(year),
        en="Easter",
        zh_hans="复活节,主复活日",
        zh_hant="覆活節,主覆活日",
        )

# ######### Lunar Festival ##########


LaBa = Festival(
        lambda year: Lunar(year-1, 12, 8).to_date(),
        en="LaBa Festival",
        zh_hans="腊八节",
        zh_hant="臘八節",
        )

NewYearEve = Festival(
        lambda year: Lunar(year, 1, 1).to_date() - datetime.timedelta(days=1),
        en="New Year's Eve",
        zh_hans="除夕,除夕夜,大年夜,年夜,年三十,除夜,岁除,大晦日",
        zh_hant="除夕,除夕夜,大年夜,年夜,年三十,除夜,歲除,大晦日",
        )

XiaoNian = Festival(
        lambda year: Lunar(year-1, 12, 23).to_date(),
        en="XiaoNian",
        zh_hans="小年,谢节,送灶,祭灶节,灶王节,送神",
        zh_hant="小年,謝節,送竈,祭竈節,竈王節,送神",
        )

ChineseNewYear = Festival(
        lambda year: Lunar(year, 1, 1).to_date(),
        en="Chinese New Year",
        zh_hans="春节,中国新年,年结,岁首,新春,正旦,正月朔日,过新年,过年",
        zh_hant="春節,中國新年,年結,歲首,新春,正旦,正月朔日,過新年,過年",
        )

PoWu = Festival(
        lambda year: Lunar(year, 1, 5).to_date(),
        en="PoWu Festival",
        zh_hans="破五节,隔开日,接财神,迎财神",
        zh_hant="破五節,隔開日,接財神,迎財神",
        )

Lantern = Festival(
        lambda year: Lunar(year, 1, 15).to_date(),
        en="Lantern Festival",
        zh_hans="元宵节,上元节,小正月,元夕,灯节,灯笼节",
        zh_hant="元宵節,上元節,小正月,元夕,燈節,燈籠節",
        )

DragonHead = Festival(
        lambda year: Lunar(year, 2, 2).to_date(),
        en="Dragon Head Festival",
        zh_hans="龙抬头,龙头节,春龙节,春耕节,农事节",
        zh_hant="龍擡頭,龍頭節,春龍節,春耕節,農事節",
        )

DragonBoat = Festival(
        lambda year: Lunar(year, 5, 5).to_date(),
        en="Dragon Boat Festival",
        zh_hans="端午节,龙舟节,端阳节,端日节,午日节,粽子节,五日节,五月节,天中节,菖蒲节",
        zh_hant="端午節,龍舟節,端陽節,端日節,午日節,粽子節,五日節,五月節,天中節,菖蒲節",
        )

Qixi = Festival(
        lambda year: Lunar(year, 7, 7).to_date(),
        en="Qixi Festival",
        zh_hans="七夕节,七巧节,乞巧节,七姐诞,七娘生",
        zh_hant="七夕節,七巧節,乞巧節,七姐誕,七娘生",
        )

Ghost = Festival(
        lambda year: Lunar(year, 7, 15).to_date(),
        en="Ghost Festival",
        zh_hans="中元节,鬼节,施孤,七月半,盂兰盆节",
        zh_hant="中元節,鬼節,施孤,七月半,盂蘭盆節",
        )

MidAutumn = Festival(
        lambda year: Lunar(year, 8, 15).to_date(),
        en="Mid-Autumn Festival",
        zh_hans="中秋节,月夕,秋节,仲秋节,八月节,八月会,追月节,玩月节,拜月节,女儿节,团圆节,赏月节",
        zh_hant="中秋節,月夕,秋節,仲秋節,八月節,八月會,追月節,玩月節,拜月節,女兒節,團圓節,賞月節",
        )

ChongYang = Festival(
        lambda year: Lunar(year, 9, 9).to_date(),
        en="Double Ninth Festival",
        zh_hans="重阳节,重九节,晒秋节,踏秋,老人节,敬老节,登山节,登高节",
        zh_hant="重陽節,重九節,曬秋節,踏秋,老人節,敬老節,登山節,登高節",
        )

Hanyi = Festival(
        lambda year: Lunar(year, 10, 1).to_date(),
        en="HanYi Festival",
        zh_hans="寒衣节,十月朝,祭祖节,冥阴节,鬼头日,包袱节",
        zh_hant="寒衣節,十月朝,祭祖節,冥陰節,鬼頭日,包袱節",
        )

DongJie = Festival(
        lambda year: solarterm.DongZhi(year),
        en="Dong Festival",
        zh_hans="冬节,冬至",
        zh_hant="冬節,冬至",
        )


# ######### data ##########
festivals = [f for _, f in globals().items() if isinstance(f, Festival)]
zh_festivals = [f for f in festivals if f.get_lang('zh_hans')]
