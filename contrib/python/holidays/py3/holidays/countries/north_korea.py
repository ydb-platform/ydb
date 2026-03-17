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

from holidays.calendars.chinese import KOREAN_CALENDAR
from holidays.groups import ChineseCalendarHolidays, InternationalHolidays
from holidays.holiday_base import HolidayBase


class NorthKorea(HolidayBase, ChineseCalendarHolidays, InternationalHolidays):
    """North Korea holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_North_Korea>
        * <https://en.wikipedia.org/wiki/Day_of_Songun>
        * <https://en.wikipedia.org/wiki/Dano_(festival)>
        * <https://en.wikipedia.org/wiki/Cold_Food_Festival>
        * <https://ko.wikipedia.org/wiki/조국해방전쟁_승리_기념일>
        * [2025 Holidays](https://namu.wiki/w/공휴일/북한)
        * [Korean People's Army](https://web.archive.org/web/20250824194534/https://namu.wiki/w/조선인민군)
        * [Founding Day of the Korean People's Revolutionary Army](https://web.archive.org/web/20250909074255/https://www.nocutnews.co.kr/news/1091995)
        * [Holidays and Anniversaries](http://archive.today/2025.09.21-064344/https://unikorea.go.kr/nkhr/current/life/living/daily/?boardId=bbs_0000000000000078&mode=view&cntId=51400)
        * [Day of Songun](http://archive.today/2025.09.21-063344/https://namu.wiki/w/선군절)
        * [2013 North Korea Calendar](https://web.archive.org/web/20250921062305/https://nk.chosun.com/news/articleView.html?idxno=151905)
        * [Daeboreum in North Korea](https://web.archive.org/web/20230210091341/https://www.newsis.com/view/?id=NISX20200207_0000911479)
        * [Day of the Shining Star](https://web.archive.org/web/20250921063234/https://www.munhwa.com/article/11412228)
        * [Day of the Sun](http://archive.today/2025.09.21-063705/https://namu.wiki/w/태양절)
        * [Cheongmyeong Festival](https://web.archive.org/web/20250324201145/https://namu.wiki/w/청명)
    """

    country = "KP"
    default_language = "ko_KP"
    # %s (estimated).
    estimated_label = tr("%s (추정된)")
    start_year = 1948
    supported_languages = ("en_US", "ko_KP")

    def __init__(self, *args, **kwargs):
        ChineseCalendarHolidays.__init__(self, calendar=KOREAN_CALENDAR)
        InternationalHolidays.__init__(self)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("양력설"))

        if self._year <= 1967 or self._year >= 1989:
            # Korean New Year.
            self._add_chinese_new_years_day(tr("설명절"))

        if self._year >= 2003:
            # Daeboreum.
            self._add_daeboreum_day(tr("대보름"))

        if self._year <= 1977 or self._year >= 2018:
            # Founding Day of the Korean People's Army.
            self._add_holiday_feb_8(tr("조선인민군창건일"))

        if self._year >= 1975:
            name = (
                # Day of the Shining Star.
                tr("광명성절")
                if self._year >= 2012
                # Kim Jong Il's Birthday.
                else tr("김정일의 생일")
            )
            self._add_holiday_feb_16(name)
            if self._year >= 1986:
                self._add_holiday_feb_17(name)

        # International Women's Day.
        self._add_womens_day(tr("국제부녀절"))

        if self._year >= 2012:
            # Cheongmyeong Festival.
            self._add_qingming_festival(tr("청명"))

        if self._year <= 1967:
            # Hanshi Festival.
            self._add_hanshi_festival(tr("한식"))

        if self._year == 1962 or self._year >= 1968:
            name = (
                # Day of the Sun.
                tr("태양절")
                if self._year >= 1998
                # Kim Il-Sung's Birthday.
                else tr("김일성의 생일")
            )
            self._add_holiday_apr_15(name)
            if self._year >= 1998:
                self._add_holiday_apr_16(name)

        if self._year >= 1978:
            self._add_holiday_apr_25(
                # Founding Day of the Korean People's Revolutionary Army.
                tr("조선인민혁명군 창건일")
                if self._year >= 2018
                # Armed Forces Day.
                else tr("건군절")
            )

        # International Workers' Day.
        self._add_labor_day(tr("전세계근로자들의 국제적명절"))

        if self._year <= 1967:
            # Dano.
            self._add_dragon_boat_festival(tr("단오"))

        # Foundation Day of the Korean Children's Union.
        self._add_holiday_jun_6(tr("조선소년단 창립절"))

        if self._year >= 1996:
            # Day of Victory in the Great Fatherland Liberation War.
            self._add_holiday_jul_27(tr("조국해방전쟁승리기념일"))

        # Liberation Day.
        self._add_holiday_aug_15(tr("조국해방절"))

        if self._year <= 1966 or self._year >= 1989:
            # Chuseok.
            self._add_mid_autumn_festival(tr("추석"))

        if self._year >= 2013:
            # Day of Songun.
            self._add_holiday_aug_25(tr("선군절"))

        # Youth Day.
        self._add_holiday_aug_28(tr("청년절"))

        # Founding Day of the DPRK.
        self._add_holiday_sep_9(tr("조선민주주의인민공화국창건일"))

        # Foundation Day of the Workers' Party of Korea.
        self._add_holiday_oct_10(tr("조선로동당창건일"))

        if self._year >= 2012:
            # Mother's Day.
            self._add_holiday_nov_16(tr("어머니날"))

        if self._year >= 1972:
            # Socialist Constitution Day.
            self._add_holiday_dec_27(tr("사회주의헌법절"))


class KP(NorthKorea):
    pass


class PRK(NorthKorea):
    pass
