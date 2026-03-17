#  holidays
#  --------
#  A fast, efficient Python library for generating country, province and state
#  specific sets of holidays on the fly. It aims to make determining whether a
#  specific date is a holiday as fast and flexible as possible.
#
#  Authors: Vacanza Team and individual contributors (see CONTRIBUTORS file)
#           dr-prodigy <dr.prodigy.github@gmail.com> (c) 2017-2023
#           ryanss <ryanssdev@icloud.com> (c) 2014-2017
#  Website: https://github.com/vacanza/holidays
#  License: MIT (see LICENSE file)

from gettext import gettext as tr

from holidays.calendars.gregorian import JAN, FEB, MAR, APR, MAY, JUN, SEP, OCT, DEC
from holidays.constants import HALF_DAY, PUBLIC
from holidays.groups import ChineseCalendarHolidays, InternationalHolidays, StaticHolidays
from holidays.observed_holiday_base import ObservedHolidayBase, SAT_SUN_TO_NEXT_WORKDAY


class China(ObservedHolidayBase, ChineseCalendarHolidays, InternationalHolidays, StaticHolidays):
    """China holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_China>
        * [Festivals and Public Holidays](https://zh.wikipedia.org/wiki/中华人民共和国节日与公众假期)
        * [2026](https://web.archive.org/web/20251111205945/https://www.gov.cn/zhengce/content/202511/content_7047090.htm)
        * [2025](https://web.archive.org/web/20250424041657/https://www.gov.cn/zhengce/content/202411/content_6986382.htm)
        * [2024 changes (Order #795)](https://web.archive.org/web/20250228151847/https://www.gov.cn/zhengce/content/202411/content_6986380.htm)
        * [2024](https://web.archive.org/web/20250227033646/https://www.gov.cn/zhengce/content/202310/content_6911527.htm)
        * [2023](https://web.archive.org/web/20250414125053/https://www.gov.cn/gongbao/content/2023/content_5736714.htm)
        * [2022](https://web.archive.org/web/20250413071341/http://www.gov.cn/gongbao/content/2021/content_5651728.htm)
        * [2021](https://web.archive.org/web/20250424075325/https://www.gov.cn/gongbao/content/2020/content_5567750.htm)
        * [2020 Extensions](https://web.archive.org/web/20250427184932/https://www.gov.cn/zhengce/zhengceku/2020-01/27/content_5472352.htm)
        * [2020](https://web.archive.org/web/20241222150612/https://www.gov.cn/gongbao/content/2019/content_5459138.htm)
        * [2019](https://web.archive.org/web/20241202235023/https://www.gov.cn/gongbao/content/2018/content_5350046.htm)
        * [2018](https://web.archive.org/web/20231205013402/https://www.gov.cn/gongbao/content/2017/content_5248221.htm)
        * [2017](https://web.archive.org/web/20231205013333/https://www.gov.cn/gongbao/content/2016/content_5148793.htm)
        * [2016](https://web.archive.org/web/20231205013233/https://www.gov.cn/gongbao/content/2016/content_2979719.htm)
        * [2015](https://web.archive.org/web/20250427025304/https://www.gov.cn/gongbao/content/2015/content_2799019.htm)
        * [2014](https://web.archive.org/web/20250426080722/https://www.gov.cn/gongbao/content/2014/content_2561299.htm)
        * [2013](https://web.archive.org/web/20240229121946/https://www.gov.cn/gongbao/content/2012/content_2292057.htm)
        * [2012](https://web.archive.org/web/20250420220922/https://www.gov.cn/gongbao/content/2011/content_2020918.htm)
        * [2011](https://web.archive.org/web/20240812070712/https://www.gov.cn/gongbao/content/2010/content_1765282.htm)
        * [2010](https://web.archive.org/web/20101210083603/http://www.gov.cn:80/gongbao/content/2009/content_1487011.htm)
        * [2009](https://web.archive.org/web/20230726083438/https://www.gov.cn/gongbao/content/2008/content_1175823.htm)
        * [2008](https://web.archive.org/web/20240610103541/https://www.gov.cn/gongbao/content/2008/content_859870.htm)
        * [2007](https://web.archive.org/web/20230727050141/https://www.gov.cn/gongbao/content/2007/content_503397.htm)
        * [2006](https://web.archive.org/web/20221130214247/https://zh.wikisource.org/wiki/国务院办公厅关于2006年部分节假日安排的通知)
        * [2005](https://web.archive.org/web/20210212184000/https://zh.wikisource.org/wiki/国务院办公厅关于2005年部分节假日安排的通知)
        * [2004](https://web.archive.org/web/20210212183857/https://zh.wikisource.org/wiki/国务院办公厅关于2004年部分节假日安排的通知)
        * [2003](https://web.archive.org/web/20210302090553/https://zh.wikisource.org/wiki/国务院办公厅关于2003年部分节假日休息安排的通知)
        * [2002](https://web.archive.org/web/20180122201149/https://zh.wikisource.org/wiki/国务院办公厅关于2002年部分节假日休息安排的通知)
        * [2001](https://web.archive.org/web/20180123032517/https://zh.wikisource.org/wiki/国务院办公厅关于2001年春节、“五一”、“十一”放假安排的通知)

    Checked With:
        * <https://web.archive.org/web/20250213085558/https://www.officeholidays.com/countries/china/2023>
        * <https://web.archive.org/web/20250210121944/https://www.china-briefing.com/news/china-public-holiday-2023-schedule/>
        * <https://web.archive.org/web/20250218010025/https://www.timeanddate.com/calendar/?year=2023&country=41>
        * [2001-2010](https://web.archive.org/web/20250429074341/https://m.wannianli.tianqi.com/fangjiaanpai/2001.html)

    Limitations:
        * Only checked with the official General Office of the State Council Notice from 2001
            onwards.
        * Due to its complexity, need yearly checks 3-weeks before year's end each year.
    """

    country = "CN"
    # %s (estimated).
    estimated_label = tr("%s（推定）")
    # %s (observed, estimated).
    observed_estimated_label = tr("%s（观察日，推定）")
    # %s (observed).
    observed_label = tr("%s（观察日）")
    supported_categories = (PUBLIC, HALF_DAY)
    default_language = "zh_CN"
    supported_languages = ("en_US", "th", "zh_CN", "zh_TW")
    # Proclamation of the People's Republic of China on Oct 1, 1949.
    start_year = 1950

    def __init__(self, *args, **kwargs):
        ChineseCalendarHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, cls=ChinaStaticHolidays)
        kwargs.setdefault("observed_rule", SAT_SUN_TO_NEXT_WORKDAY)
        kwargs.setdefault("observed_since", 2000)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        dts_observed = set()

        # 元旦 (simp.) / 新年 (trad.)
        # Status: In-Use (Statutory).
        # Jan 1 in 1949, 1999, 2007, and 2013 revision.
        # Consecutive Holidays are available from 2002, except in 2014/2016/2017/2018.

        # New Year's Day.
        dts_observed.add(self._add_new_years_day(tr("元旦")))

        # 春节
        # Status: In-Use (Statutory).
        # Day 1-3 of Chinese New Year in 1949, 1999, 2007, and 2013 revision.
        # 2007 revision introduced New Year's Eve (农历除夕) instead of
        # New Year's 3rd day; 2013 revision returned it back;
        # 2024 revision returned also New Year's Eve.

        # Spring Festival Golden Weekend
        # Checked with Official Notice from 2001-2025.
        # Consecutive Holidays are available from 2000 (1999 rev.).

        # Chinese New Year (Spring Festival).
        chinese_new_year = tr("春节")
        # Chinese New Year's Eve.
        chinese_new_years_eve = tr("农历除夕")
        dts_observed.add(self._add_chinese_new_years_day(chinese_new_year))
        dts_observed.add(self._add_chinese_new_years_day_two(chinese_new_year))
        if 2008 <= self._year <= 2013:
            dts_observed.add(self._add_chinese_new_years_eve(chinese_new_years_eve))
        else:
            dts_observed.add(self._add_chinese_new_years_day_three(chinese_new_year))
        if self._year >= 2025:
            dts_observed.add(self._add_chinese_new_years_eve(chinese_new_years_eve))

        # 劳动节
        # Status: In-Use (Statutory).
        # May 1 in 1949, 1999, 2007, and 2013 revision.
        # Additional Holidays (May 2-3) are available from 2000 (1999 rev.) - 2007 (2007 rev.).
        # May 2 returned in 2024 revision.

        # Labor Day Golden Weekend
        # Checked with Official Notice from 2001-2025.
        # Consecutive Holidays are available from 2002, with exception of ????-????.

        # Labor Day.
        labor_day = tr("劳动节")
        dts_observed.add(self._add_labor_day(labor_day))
        if 2000 <= self._year <= 2007:
            dts_observed.add(self._add_labor_day_two(labor_day))
            dts_observed.add(self._add_labor_day_three(labor_day))
        elif self._year >= 2025:
            dts_observed.add(self._add_labor_day_two(labor_day))

        # 国庆节
        # Status: In-Use (Statutory).
        # Oct 1-2 in 1949, 1999, 2007, and 2013 revision
        # Additional Holiday (Oct 3) is available from Sep 1999 (1999 rev.).

        # National Day Golden Weekend
        # Checked with Official Notice from 2001-2023.

        # National Day.
        national_day = tr("国庆节")
        dts_observed.add(self._add_holiday_oct_1(national_day))
        dts_observed.add(self._add_holiday_oct_2(national_day))
        if self._year >= 1999:
            dts_observed.add(self._add_holiday_oct_3(national_day))

        if self._year >= 2008:
            # 清明节
            # Status: In-Use (Statutory).
            # Tomb-Sweeping Day in 2007, and 2013 revision.
            # Consecutive Holidays are available from 2008, except in 2014/2015/2016/2019.

            # Tomb-Sweeping Day.
            dts_observed.add(self._add_qingming_festival(tr("清明节")))

            # 端午节
            # Status: In-Use (Statutory).
            # Dragon Boat Festival in 2007, and 2013 revision.
            # Consecutive Holidays are available from 2008, except in 2014/2015/2018/2019/2023.

            # Dragon Boat Festival.
            dragon_boat_festival = self._add_dragon_boat_festival(tr("端午节"))
            if self._year != 2012:
                dts_observed.add(dragon_boat_festival)

            # 中秋节
            # Status: In-Use (Statutory).
            # Mid-Autumn Festival in 2007, and 2013 revision.
            # Consecutive Holidays are available from 2008, except in 2010/2014/2015/2018/2019.
            # Extra Day (Oct 8) is instead added to the National Day Week if overlaps.

            # Mid-Autumn Festival.
            mid_autumn_festival = self._add_mid_autumn_festival(tr("中秋节"))
            if self._year != 2015:
                dts_observed.add(mid_autumn_festival)

        if self.observed:
            self._populate_observed(dts_observed, multiple=True)

    def _populate_half_day_holidays(self):
        # No in lieus are given for this category.

        # International Women's Day.
        self._add_womens_day(tr("国际妇女节"))

        # Youth Day.
        self._add_holiday_may_4(tr("五四青年节"))

        # Children's Day.
        self._add_childrens_day(tr("六一儿童节"))

        # Army Day.
        self._add_holiday_aug_1(tr("建军节"))


class CN(China):
    pass


class CHN(China):
    pass


class ChinaStaticHolidays:
    # Date format (see strftime() Format Codes).
    substituted_date_format = tr("%Y-%m-%d")
    # Day off (substituted from %s).
    substituted_label = tr("休息日（%s日起取代）")

    # Chinese New Year (Spring Festival).
    chinese_new_year = tr("春节")

    # Chinese New Year (Spring Festival) Extended Holiday.
    chinese_new_year_extended = tr("春节延长假期")

    # Dragon Boat Festival.
    dragon_boat_festival = tr("端午节")

    # Mid-Autumn Festival.
    mid_autumn_festival = tr("中秋节")

    # 70th Anniversary of the Victory of the Chinese People's War of Resistance against
    # Japanese Aggression and the World Anti-Fascist War.
    victory_70_anniversary = tr("中国人民抗日战争暨世界反法西斯战争胜利70周年纪念日")

    special_public_holidays = {
        2001: (
            (JAN, 29, JAN, 20),  # Spring Festival
            (JAN, 30, JAN, 21),  # Spring Festival
            (MAY, 4, APR, 28),  # Labor Day
            (MAY, 7, APR, 29),  # Labor Day
            (OCT, 4, SEP, 29),  # National Day
            (OCT, 5, SEP, 30),  # National Day
        ),
        2002: (
            (JAN, 2, DEC, 29, 2001),  # New Year's Day
            (JAN, 3, DEC, 30, 2001),  # New Year's Day
            (FEB, 15, FEB, 9),  # Spring Festival
            (FEB, 18, FEB, 10),  # Spring Festival
            (MAY, 6, APR, 27),  # Labor Day
            (MAY, 7, APR, 28),  # Labor Day
            (OCT, 4, SEP, 28),  # National Day
            (OCT, 7, SEP, 29),  # National Day
        ),
        2003: (
            (FEB, 6, FEB, 8),  # Spring Festival
            (FEB, 7, FEB, 9),  # Spring Festival
            (MAY, 6, APR, 26),  # Labor Day
            (MAY, 7, APR, 27),  # Labor Day
            (OCT, 6, SEP, 27),  # National Day
            (OCT, 7, SEP, 28),  # National Day
        ),
        2004: (
            (JAN, 27, JAN, 17),  # Spring Festival
            (JAN, 28, JAN, 18),  # Spring Festival
            (MAY, 6, MAY, 8),  # Labor Day
            (MAY, 7, MAY, 9),  # Labor Day
            (OCT, 6, OCT, 9),  # National Day
            (OCT, 7, OCT, 10),  # National Day
        ),
        2005: (
            (FEB, 14, FEB, 5),  # Spring Festival
            (FEB, 15, FEB, 6),  # Spring Festival
            (MAY, 5, APR, 30),  # Labor Day
            (MAY, 6, MAY, 8),  # Labor Day
            (OCT, 6, OCT, 8),  # National Day
            (OCT, 7, OCT, 9),  # National Day
        ),
        2006: (
            (JAN, 3, DEC, 31, 2005),  # New Year's Day
            (FEB, 2, JAN, 28),  # Spring Festival
            (FEB, 3, FEB, 5),  # Spring Festival
            (MAY, 4, APR, 29),  # Labor Day
            (MAY, 5, APR, 30),  # Labor Day
            (OCT, 5, SEP, 30),  # National Day
            (OCT, 6, OCT, 8),  # National Day
        ),
        2007: (
            (JAN, 2, DEC, 30, 2006),  # New Year's Day
            (JAN, 3, DEC, 31, 2006),  # New Year's Day
            (FEB, 22, FEB, 17),  # Spring Festival
            (FEB, 23, FEB, 25),  # Spring Festival
            (MAY, 4, APR, 28),  # Labor Day
            (MAY, 7, APR, 29),  # Labor Day
            (OCT, 4, SEP, 29),  # National Day
            (OCT, 5, SEP, 30),  # National Day
            (DEC, 31, DEC, 29),  # New Year's Day
        ),
        2008: (
            (FEB, 11, FEB, 2),  # Spring Festival
            (FEB, 12, FEB, 3),  # Spring Festival
            (MAY, 2, MAY, 4),  # Labor Day
            (SEP, 29, SEP, 27),  # National Day
            (SEP, 30, SEP, 28),  # National Day
        ),
        2009: (
            (JAN, 2, JAN, 4),  # New Year's Day
            (JAN, 29, JAN, 24),  # Spring Festival
            (JAN, 30, FEB, 1),  # Spring Festival
            (MAY, 29, MAY, 31),  # Dragon Boat Festival
            (OCT, 7, SEP, 27),  # National Day
            (OCT, 8, OCT, 10),  # National Day
        ),
        2010: (
            (FEB, 18, FEB, 20),  # Spring Festival
            (FEB, 19, FEB, 21),  # Spring Festival
            (JUN, 14, JUN, 12),  # Dragon Boat Festival
            (JUN, 15, JUN, 13),  # Dragon Boat Festival
            (SEP, 23, SEP, 19),  # Mid-Autumn Festival
            (SEP, 24, SEP, 25),  # Mid-Autumn Festival
            (OCT, 6, SEP, 26),  # National Day
            (OCT, 7, OCT, 9),  # National Day
        ),
        2011: (
            (FEB, 7, JAN, 30),  # Spring Festival
            (FEB, 8, FEB, 12),  # Spring Festival
            (APR, 4, APR, 2),  # Tomb-Sweeping Day
            (OCT, 6, OCT, 8),  # National Day
            (OCT, 7, OCT, 9),  # National Day
        ),
        2012: (
            (JAN, 3, DEC, 31, 2011),  # New Year's Day
            (JAN, 26, JAN, 21),  # Spring Festival
            (JAN, 27, JAN, 29),  # Spring Festival
            (APR, 2, MAR, 31),  # Tomb-Sweeping Day
            (APR, 3, APR, 1),  # Tomb-Sweeping Day
            (APR, 30, APR, 28),  # Labor Day
            (OCT, 5, SEP, 29),  # National Day
        ),
        2013: (
            (JAN, 2, JAN, 5),  # New Year's Day
            (JAN, 3, JAN, 6),  # New Year's Day
            (FEB, 14, FEB, 16),  # Spring Festival
            (FEB, 15, FEB, 17),  # Spring Festival
            (APR, 5, APR, 7),  # Tomb-Sweeping Day
            (APR, 29, APR, 27),  # Labor Day
            (APR, 30, APR, 28),  # Labor Day
            (JUN, 10, JUN, 8),  # Dragon Boat Festival
            (JUN, 11, JUN, 9),  # Dragon Boat Festival
            (SEP, 20, SEP, 22),  # Mid-Autumn Festival
            (OCT, 4, SEP, 29),  # National Day
            (OCT, 7, OCT, 12),  # National Day
        ),
        2014: (
            (FEB, 5, JAN, 26),  # Spring Festival
            (FEB, 6, FEB, 8),  # Spring Festival
            (MAY, 2, MAY, 4),  # Labor Day
            (OCT, 6, SEP, 28),  # National Day
            (OCT, 7, OCT, 11),  # National Day
        ),
        2015: (
            (JAN, 2, JAN, 4),  # New Year's Day
            (FEB, 18, FEB, 15),  # Spring Festival
            (FEB, 24, FEB, 28),  # Spring Festival
            (SEP, 3, victory_70_anniversary),
            (SEP, 4, SEP, 6),  # 70th Anniversary of the Victory
            (OCT, 7, OCT, 10),  # National Day
        ),
        2016: (
            (FEB, 11, FEB, 6),  # Spring Festival
            (FEB, 12, FEB, 14),  # Spring Festival
            (JUN, 10, JUN, 12),  # Dragon Boat Festival
            (SEP, 16, SEP, 18),  # Mid-Autumn Festival
            (OCT, 6, OCT, 8),  # National Day
            (OCT, 7, OCT, 9),  # National Day
        ),
        2017: (
            (JAN, 27, JAN, 22),  # Spring Festival
            (FEB, 2, FEB, 4),  # Spring Festival
            (APR, 3, APR, 1),  # Tomb-Sweeping Day
            (MAY, 29, MAY, 27),  # Dragon Boat Festival
            (OCT, 6, SEP, 30),  # National Day
        ),
        2018: (
            (FEB, 15, FEB, 11),  # Spring Festival
            (FEB, 21, FEB, 24),  # Spring Festival
            (APR, 6, APR, 8),  # Tomb-Sweeping Day
            (APR, 30, APR, 28),  # Labor Day
            (OCT, 4, SEP, 29),  # National Day
            (OCT, 5, SEP, 30),  # National Day
            (DEC, 31, DEC, 29),  # New Year's Day
        ),
        2019: (
            (FEB, 4, FEB, 2),  # Spring Festival
            (FEB, 8, FEB, 3),  # Spring Festival
            (OCT, 4, SEP, 29),  # National Day
            (OCT, 7, OCT, 12),  # National Day
        ),
        2020: (
            (JAN, 24, JAN, 19),  # Spring Festival
            # JAN, 30 in special_public_holidays_observed
            (JAN, 31, chinese_new_year_extended),  # Spring Festival Extended Holiday
            (FEB, 1, chinese_new_year_extended),  # Spring Festival Extended Holiday
            (FEB, 2, chinese_new_year_extended),  # Spring Festival Extended Holiday
            (MAY, 4, APR, 26),  # Labor Day
            (MAY, 5, MAY, 9),  # Labor Day
            (JUN, 26, JUN, 28),  # Dragon Boat Festival
            (OCT, 7, SEP, 27),  # National Day
            (OCT, 8, OCT, 10),  # National Day
        ),
        2021: (
            (FEB, 11, FEB, 7),  # Spring Festival
            (FEB, 17, FEB, 20),  # Spring Festival
            (MAY, 4, APR, 25),  # Labor Day
            (MAY, 5, MAY, 8),  # Labor Day
            (SEP, 20, SEP, 18),  # Mid-Autumn Festival
            (OCT, 6, SEP, 26),  # National Day
            (OCT, 7, OCT, 9),  # National Day
        ),
        2022: (
            (JAN, 31, JAN, 29),  # Spring Festival
            (FEB, 4, JAN, 30),  # Spring Festival
            (APR, 4, APR, 2),  # Tomb-Sweeping Day
            (MAY, 3, APR, 24),  # Labor Day
            (MAY, 4, MAY, 7),  # Labor Day
            (OCT, 6, OCT, 8),  # National Day
            (OCT, 7, OCT, 9),  # National Day
        ),
        2023: (
            (JAN, 26, JAN, 28),  # Spring Festival
            (JAN, 27, JAN, 29),  # Spring Festival
            (MAY, 2, APR, 23),  # Labor Day
            (MAY, 3, MAY, 6),  # Labor Day
            (JUN, 23, JUN, 25),  # Dragon Boat Festival
            (OCT, 5, OCT, 7),  # National Day
            (OCT, 6, OCT, 8),  # National Day
        ),
        2024: (
            (FEB, 15, FEB, 4),  # Spring Festival
            (FEB, 16, FEB, 18),  # Spring Festival
            (APR, 5, APR, 7),  # Tomb-Sweeping Day
            (MAY, 2, APR, 28),  # Labor Day
            (MAY, 3, MAY, 11),  # Labor Day
            (SEP, 16, SEP, 14),  # Mid-Autumn Festival
            (OCT, 4, SEP, 29),  # National Day
            (OCT, 7, OCT, 12),  # National Day
        ),
        2025: (
            (FEB, 3, JAN, 26),  # Spring Festival
            (FEB, 4, FEB, 8),  # Spring Festival
            (MAY, 5, APR, 27),  # Labor Day
            (OCT, 7, SEP, 28),  # National Day
            (OCT, 8, OCT, 11),  # National Day
        ),
        2026: (
            (JAN, 2, JAN, 4),  # New Year's Day
            (FEB, 20, FEB, 14),  # Spring Festival
            (FEB, 23, FEB, 28),  # Spring Festival
            (MAY, 5, MAY, 9),  # Labor Day
            (OCT, 6, SEP, 20),  # National Day
            (OCT, 7, OCT, 10),  # National Day
        ),
    }

    special_public_holidays_observed = {
        2012: (JUN, 22, dragon_boat_festival),  # observed from Jun 23
        2015: (OCT, 6, mid_autumn_festival),  # observed from Sep 27
        2020: (
            (JAN, 30, chinese_new_year),  # Spring Festival (extended due to Covid-19 decree)
            (OCT, 6, mid_autumn_festival),  # observed from Oct 1, overlap with National Day
        ),
    }
