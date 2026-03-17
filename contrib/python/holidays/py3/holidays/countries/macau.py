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

from holidays.calendars.gregorian import FEB, SEP, OCT, DEC
from holidays.constants import GOVERNMENT, OPTIONAL, PUBLIC
from holidays.groups import (
    ChineseCalendarHolidays,
    ChristianHolidays,
    InternationalHolidays,
    StaticHolidays,
)
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    SUN_TO_NEXT_WORKDAY,
    SAT_SUN_TO_NEXT_WORKDAY,
    SAT_TO_NONE,
    SUN_TO_NONE,
)


class Macau(
    ObservedHolidayBase,
    ChineseCalendarHolidays,
    ChristianHolidays,
    InternationalHolidays,
    StaticHolidays,
):
    """Macau holidays.

    References:
        * [Decreto-Lei n.º 4/82/M](https://web.archive.org/web/20250422171106/https://bo.io.gov.mo/bo/i/82/04/declei04.asp)
        * [Decreto-Lei n.º 38/87/M](https://web.archive.org/web/20250421204103/https://bo.io.gov.mo/bo/i/87/25/declei38.asp)
        * [Decreto-Lei n.º 15/93/M](https://web.archive.org/web/20250421233338/https://bo.io.gov.mo/bo/i/93/17/declei15.asp)
        * [Decreto-Lei n.º 7/97/M](https://web.archive.org/web/20250421102457/https://bo.io.gov.mo/bo/i/97/11/declei07.asp)
        * [Portaria n.º 85/97/M](https://web.archive.org/web/20250421075645/https://bo.io.gov.mo/bo/i/97/15/port85.asp)
        * [Portaria n.º 242/98/M](https://web.archive.org/web/20250421194357/https://bo.io.gov.mo/bo/i/98/48/port242.asp)
        * [Regulamento Administrativo n.º 4/1999](https://web.archive.org/web/20250421071204/https://bo.io.gov.mo/bo/i/1999/01/regadm04.asp)
        * [Regulamento Administrativo n.º 5/1999](https://web.archive.org/web/20240529183711/https://bo.io.gov.mo/bo/i/1999/01/regadm05.asp)
        * [Ordem Executiva n.º 60/2000](https://web.archive.org/web/20250421064911/https://bo.io.gov.mo/bo/i/2000/40/ordem60.asp)
        * [Lei n.º 27/2024](https://web.archive.org/web/20250404201226/https://bo.io.gov.mo/bo/i/2025/01/lei27.asp)

    Mandatory Holidays References:
        * [Decreto-Lei n.º 101/84/M](https://web.archive.org/web/20240518230957/https://bo.io.gov.mo/bo/i/84/35/declei101.asp)
        * [Decreto-Lei n.º 24/89/M](https://web.archive.org/web/20241215223116/https://bo.io.gov.mo/bo/i/89/14/declei24.asp)
        * [Lei n.º 8/2000](https://web.archive.org/web/20240303230903/https://bo.io.gov.mo/bo/i/2000/19/lei08.asp)
        * [Lei n.º 7/2008](https://web.archive.org/web/20250403121227/https://bo.io.gov.mo/bo/i/2008/33/lei07.asp)

    Cross-Checking:
        * [Public Holidays for 2017-2025](https://web.archive.org/web/20210509143637/https://www.gov.mo/en/public-holidays/year-2017/)
        * [Public Holidays for 2005-2018](https://web.archive.org/web/20171207162948/http://portal.gov.mo/web/guest/info_detail?infoid=1887061)
        * [Mandatory Holidays for 2009-2029](https://web.archive.org/web/20250421090753/https://www.dsal.gov.mo/pt/standard/holiday_table.html)
    """

    country = "MO"
    default_language = "zh_MO"
    # %s (estimated).
    estimated_label = tr("%s（推定）")
    # Decreto-Lei n.º 4/82/M.
    start_year = 1982
    subdivisions = (
        "I",  # Ilhas.
        "M",  # Macau.
    )
    subdivisions_aliases = {
        # Municipalities.
        "Concelho das Ilhas": "I",
        "海島市": "I",
        "海岛市": "I",
        "Concelho de Macau": "M",
        "澳門市": "M",
        "澳门市": "M",
    }
    supported_categories = (GOVERNMENT, OPTIONAL, PUBLIC)
    supported_languages = ("en_MO", "en_US", "pt_MO", "th", "zh_CN", "zh_MO")

    def __init__(self, *args, **kwargs):
        ChineseCalendarHolidays.__init__(self)
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, MacauStaticHolidays)
        kwargs.setdefault("observed_rule", SAT_SUN_TO_NEXT_WORKDAY)
        # Systemic in-lieus starts in 2011.
        kwargs.setdefault("observed_since", 2011)
        super().__init__(*args, **kwargs)

    def _populate_common(self):
        # New Year's Day.
        self._add_new_years_day(tr("元旦"))

        # Chinese New Year's Day.
        self._add_chinese_new_years_day(tr("農曆正月初一"))

        # The second day of Chinese New Year.
        self._add_chinese_new_years_day_two(tr("農曆正月初二"))

        # The third day of Chinese New Year.
        self._add_chinese_new_years_day_three(tr("農曆正月初三"))

        # Labor Day.
        self._add_labor_day(tr("勞動節"))

        # Double Ninth Festival.
        self._add_double_ninth_festival(tr("重陽節"))

        # National Day of the People's Republic of China.
        self._add_holiday_oct_1(tr("中華人民共和國國慶日"))

    def _populate_public_holidays(self):
        """Macau Mandatory (Statutory) Holidays.

        Decreto-Lei n.º 101/84/M - Earliest Available Version Online.
        Decreto-Lei n.º 24/89/M - Added Ching Ming Festival.
        Lei n.º 8/2000 - Removed Day of Portugal
                       - Added Macao S.A.R. Establishment Day.
                       - Moved Mid-Autumn to Day following Mid-Autumn to match Public Holidays.
        Lei n.º 7/2008 - Consolidated with other laws, reaffirming 2000 Amendment list.
        """
        if self._year <= 1984:
            return None

        self._populate_common()

        # Decreto-Lei n.º 24/89/M - Adds Ching Ming as a Mandatory Holiday.
        if self._year >= 1989:
            # Tomb-Sweeping Day.
            self._add_qingming_festival(tr("清明節"))

        # Lei n.º 8/2000 - Removed Day of Portugal as a Mandatory Holiday.
        #                - Changed observance from Mid-Autumn to the following day.
        #                - Adds Macao S.A.R. Establishment Day as a Mandatory Holiday.
        if self._year <= 1999:
            # Day of Portugal, Camões, and the Portuguese Communities.
            self._add_holiday_jun_10(tr("葡國日、賈梅士日暨葡僑日"))

            # Mid-Autumn Festival.
            self._add_mid_autumn_festival(tr("中秋節"))

        else:
            # The Day following Mid-Autumn Festival.
            self._add_mid_autumn_festival_day_two(tr("中秋節翌日"))

            # Macao S.A.R. Establishment Day.
            self._add_holiday_dec_20(tr("澳門特別行政區成立紀念日"))

    def _populate_optional_holidays(self):
        """Macau General Holidays."""
        self._populate_common()

        # Tomb-Sweeping Day.
        self._add_qingming_festival(tr("清明節"))

        # Regulamento Administrativo n.º 4/1999 - Name changed in Chinese for Good Friday.
        self._add_good_friday(
            # Good Friday.
            tr("耶穌受難日")
            if self._year >= 2000
            # Good Friday.
            else tr("聖周星期五")
        )

        # Regulamento Administrativo n.º 4/1999 - Name changed to The Day before Easter.
        self._add_holy_saturday(
            # The Day before Easter.
            tr("復活節前日")
            if self._year >= 2000
            # Holy Saturday.
            else tr("聖周星期六")
        )

        # Dragon Boat Festival.
        self._add_dragon_boat_festival(tr("端午節"))

        # The Day following Mid-Autumn Festival.
        self._add_mid_autumn_festival_day_two(tr("中秋節翌日"))

        # All Soul's Day.
        self._add_all_souls_day(tr("追思節"))

        # Immaculate Conception.
        self._add_immaculate_conception_day(tr("聖母無原罪瞻禮"))

        # Regulamento Administrativo n.º 4/1999 - Moved from DEC 22 to DEC 21.
        # Ordem Executiva n.º 60/2000 - Switched to Movable.

        # Winter Solstice.
        name = tr("冬至")
        if self._year >= 2001:
            self._add_dongzhi_festival(name)
        elif self._year == 2000:
            self._add_holiday_dec_21(name)
        else:
            self._add_holiday_dec_22(name)

        # Portaria n.º 242/98/M - Name changed in Chinese for Christmas Eve.
        # Regulamento Administrativo n.º 4/1999 - Further Chinese name standardization.
        if self._year >= 2000:
            # Christmas Eve.
            name = tr("聖誕節前日")
        elif self._year == 1999:
            # Christmas Eve.
            name = tr("聖誕節前夕")
        else:
            # Christmas Eve.
            name = tr("聖誕前夕")
        self._add_christmas_eve(name)

        # Portaria n.º 242/98/M - Name changed in Chinese for Christmas Day.
        self._add_christmas_day(
            # Christmas Day.
            tr("聖誕節")
            if self._year >= 1999
            # Christmas Day.
            else tr("聖誕")
        )

        # Decreto-Lei n.º 38/87/M - Removed Assumption Day and All Saints' Day as Public Holiday.
        if self._year <= 1986:
            # Assumption Day.
            self._add_assumption_of_mary_day(tr("聖母升天"))

            # All Saints' Day.
            self._add_all_saints_day(tr("諸聖節"))

        # Decreto-Lei n.º 38/87/M - Removed Corpus Christi as Public Holiday.
        if self._year <= 1987:
            # Corpus Christi.
            self._add_corpus_christi_day(tr("基督聖體聖血節"))

        # Portaria n.º 242/98/M - De Facto adds Macao S.A.R. Establishment Day on DEC 20 for 1999.
        # Regulamento Administrativo n.º 5/1999 - Special Name for 1999 (see StaticHolidays).
        # Ordem Executiva n.º 60/2000 - Removed all Portugal-derived holidays.
        #                             - Adds The Buddha's Birthday.
        #                             - Adds The day following National Day of the PRC on OCT 2.
        #                             - Adds "Anniversary of " to Macao S.A.R. holiday in Chinese.
        if self._year <= 1999:
            # Freedom Day.
            self._add_holiday_apr_25(tr("自由日"))

            # Day of Portugal, Camões, and the Portuguese Communities.
            self._add_holiday_jun_10(tr("葡國日、賈梅士日暨葡僑日"))

            # Republic Day.
            self._add_holiday_oct_5(tr("葡萄牙共和國國慶日"))

            # Restoration of Independence Day.
            self._add_holiday_dec_1(tr("恢復獨立紀念日"))
        else:
            # The Buddha's Birthday.
            self._add_chinese_birthday_of_buddha(tr("佛誕節"))

            # The day following National Day of the People's Republic of China.
            self._add_holiday_oct_2(tr("中華人民共和國國慶日翌日"))

            # Macao S.A.R. Establishment Day.
            self._add_holiday_dec_20(tr("澳門特別行政區成立紀念日"))

    def _populate_government_holidays(self):
        # While Cross-Checking References are available for from 2005-2025,
        # SUN in-lieus starts in 2011; SAT-SUN in-lieus starts in 2012.
        if self._year <= 2004:
            return None

        dts_observed = set()

        # %s (Afternoon).
        begin_time_label = self.tr("%s（下午）")

        # New Year's Day.
        dts_observed.add(self._add_new_years_day(tr("元旦")))

        if not (
            self._is_sunday(self._chinese_new_year) or self._is_monday(self._chinese_new_year)
        ):
            # Chinese New Year's Eve.
            self._add_chinese_new_years_eve(begin_time_label % self.tr("農曆除夕"))

        if self._year >= 2019:
            # Chinese New Year's Day.
            dts_observed.add(self._add_chinese_new_years_day(tr("農曆正月初一")))

            # The second day of Chinese New Year.
            dts_observed.add(self._add_chinese_new_years_day_two(tr("農曆正月初二")))

            # The third day of Chinese New Year.
            dts_observed.add(self._add_chinese_new_years_day_three(tr("農曆正月初三")))
        else:
            if self._is_friday(self._chinese_new_year) or self._is_weekend(self._chinese_new_year):
                # The fourth day of Chinese New Year.
                self._add_chinese_new_years_day_four(tr("農曆正月初四"))
            if self._year >= 2012 and (
                self._is_thursday(self._chinese_new_year)
                or self._is_friday(self._chinese_new_year)
                or self._is_saturday(self._chinese_new_year)
            ):
                # The fifth day of Chinese New Year.
                self._add_chinese_new_years_day_five(tr("農曆正月初五"))

        # The Day before Easter.
        dts_observed.add(self._add_holy_saturday(tr("復活節前日")))

        # Tomb-Sweeping Day.
        dts_observed.add(self._add_qingming_festival(tr("清明節")))

        # Labor Day.
        dts_observed.add(self._add_labor_day(tr("勞動節")))

        # The Buddha's Birthday.
        dts_observed.add(self._add_chinese_birthday_of_buddha(tr("佛誕節")))

        # Dragon Boat Festival.
        dts_observed.add(self._add_dragon_boat_festival(tr("端午節")))

        # The Day following Mid-Autumn Festival.
        dts_observed.add(self._add_mid_autumn_festival_day_two(tr("中秋節翌日")))

        # Double Ninth Festival.
        dts_observed.add(self._add_double_ninth_festival(tr("重陽節")))

        # National Day of the People's Republic of China.
        dts_observed.add(self._add_holiday_oct_1(tr("中華人民共和國國慶日")))

        # The day following National Day of the People's Republic of China.
        dts_observed.add(self._add_holiday_oct_2(tr("中華人民共和國國慶日翌日")))

        # All Soul's Day.
        dts_observed.add(self._add_all_souls_day(tr("追思節")))

        # Immaculate Conception.
        dts_observed.add(self._add_immaculate_conception_day(tr("聖母無原罪瞻禮")))

        # Macao S.A.R. Establishment Day.
        dts_observed.add(self._add_holiday_dec_20(tr("澳門特別行政區成立紀念日")))

        # Winter Solstice.
        dts_observed.add(self._add_dongzhi_festival(tr("冬至")))

        # Christmas Eve.
        dts_observed.add(self._add_christmas_eve(tr("聖誕節前日")))

        # Christmas Day.
        dts_observed.add(self._add_christmas_day(tr("聖誕節")))

        # 2012's Full-Day New Year's Eve is declared discretely.
        if self._year >= 2007 and self._year != 2012:
            self._move_holiday(
                # New Year's Eve.
                self._add_new_years_eve(begin_time_label % self.tr("除夕")),
                rule=SAT_TO_NONE + SUN_TO_NONE,
            )

        if self.observed:
            self.observed_label = (
                # Compensatory rest day for %s.
                tr("%s的補假")
                if self._year >= 2020
                # The first working day after %s.
                else tr("%s後首個工作日")
            )
            self.observed_estimated_label = (
                # Compensatory rest day for %s (estimated).
                self.tr("%s的補假（推定）")
                if self._year >= 2020
                # The first working day after %s (estimated).
                else self.tr("%s後首個工作日（推定）")
            )
            # Prior to 2012, in-lieus are only given for holidays which falls on Sunday.
            self._observed_rule = (
                SUN_TO_NEXT_WORKDAY if self._year <= 2011 else SAT_SUN_TO_NEXT_WORKDAY
            )
            self._populate_observed(dts_observed, multiple=True)

    def _populate_subdiv_i_optional_holidays(self):
        # Decreto-Lei n.º 15/93/M - Moved Day of the Municipality of Ilhas from JUL 13 to NOV 30.
        # Regulamento Administrativo n.º 4/1999 - Removed as a Public Holiday.
        if self._year <= 1999:
            # Day of the Municipality of Ilhas.
            name = tr("海島市日")
            if self._year <= 1992:
                self._add_holiday_nov_30(name)
            else:
                self._add_holiday_jul_13(name)

    def _populate_subdiv_m_optional_holidays(self):
        # Regulamento Administrativo n.º 4/1999 - Removed Macau City Day as a Public Holiday.
        if self._year <= 1999:
            # Macau City Day.
            self._add_holiday_jun_24(tr("澳門市日"))


class MO(Macau):
    pass


class MAC(Macau):
    pass


class MacauStaticHolidays:
    """Macau special holidays.

    Special General and Government Holidays:
        * <https://web.archive.org/web/20240421052702/https://www.io.gov.mo/pt/legis/rec/111020>

    Special Mandatory (Statutory) Holidays:
        * <https://web.archive.org/web/20250421090753/https://www.dsal.gov.mo/pt/standard/holiday_table.html>

    Cross-Checking:
        * [Public Holidays for 2017-2025](https://web.archive.org/web/20210509143637/https://www.gov.mo/en/public-holidays/year-2017/)
        * [Public Holidays for 2005-2018](https://web.archive.org/web/20171207162948/http://portal.gov.mo/web/guest/info_detail?infoid=1887061)
    """

    # Additional Public Holiday.
    name_fullday = tr("額外公眾假期")

    # Additional Half-Day Public Holiday.
    name_halfday = tr("額外公眾半日假")

    # 70th Anniversary of the Victory of the Chinese People's War of Resistance against
    # Japanese Aggression and the World Anti-Fascist War.
    name_70th_war_of_resistance = tr("中國人民抗日戰爭暨世界反法西斯戰爭勝利七十周年紀念日")

    # Overlapping of the Day following National Day of the People's Republic of China
    # and the Day following Mid-Autumn Festival.
    name_mid_autumn_festival_day_2_national_day_2_overlap = tr(
        "中華人民共和國國慶日翌日及中秋節翌日重疊"
    )

    # Overlapping of the Day following National Day of the People's Republic of China
    # and the Double Ninth Festival.
    name_double_ninth_festival_national_day_2_overlap = tr("中華人民共和國國慶日翌日及重陽節重疊")

    # Overlapping of the National Day of the People's Republic of China
    # and the Day following Mid-Autumn Festival.
    name_mid_autumn_festival_day_2_national_day_overlap = tr(
        "中華人民共和國國慶日及中秋節翌日重疊"
    )

    # New Year's Eve.
    name_new_years_eve = tr("除夕")

    special_government_holidays = {
        # Additional Government Holiday.
        2008: (DEC, 22, tr("額外政府假期")),
        2012: (
            (OCT, 3, name_mid_autumn_festival_day_2_national_day_overlap),
            (DEC, 31, name_new_years_eve),
        ),
        2014: (OCT, 3, name_double_ninth_festival_national_day_2_overlap),
        2020: (OCT, 5, name_mid_autumn_festival_day_2_national_day_2_overlap),
    }
    special_public_holidays = {
        2015: (SEP, 3, name_70th_war_of_resistance),
    }
    special_optional_holidays = {
        1998: (
            (DEC, 23, name_fullday),
            (DEC, 31, name_halfday),
        ),
        1999: (
            (FEB, 15, name_fullday),
            # The Handover of Macau to China and the Establishment of the Macau
            # Special Administrative Region of the People's Republic of China.
            (DEC, 20, tr("澳門回歸祖國暨中華人民共和國澳門特別行政區成立日")),
            # The day following the Handover of Macau to China and the Establishment of the Macau
            # Special Administrative Region of the People's Republic of China.
            (DEC, 21, tr("澳門回歸祖國暨中華人民共和國澳門特別行政區成立日翌日")),
            (DEC, 31, name_halfday),
        ),
        2000: (FEB, 4, name_halfday),
        2015: (SEP, 3, name_70th_war_of_resistance),
    }
