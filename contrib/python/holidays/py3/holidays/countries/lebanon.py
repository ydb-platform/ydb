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

from holidays.calendars import _CustomIslamicHolidays
from holidays.calendars.gregorian import JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DEC
from holidays.calendars.julian import JULIAN_CALENDAR
from holidays.constants import BANK, GOVERNMENT, PUBLIC
from holidays.groups import ChristianHolidays, InternationalHolidays, IslamicHolidays
from holidays.observed_holiday_base import ObservedHolidayBase, SUN_TO_NEXT_MON


class Lebanon(ObservedHolidayBase, ChristianHolidays, InternationalHolidays, IslamicHolidays):
    """Lebanon holidays.

    References:
        * [Decree No. 35 of 1977](https://web.archive.org/web/20250704170408/http://legiliban.ul.edu.lb/LawArticles.aspx?LawTreeSectionID=204659&LawID=202420&language=ar)
        * [Decree No. 2512 of 1985](https://web.archive.org/web/20250704170328/http://legiliban.ul.edu.lb/LawArticles.aspx?LawTreeSectionID=186992&LawID=184753&language=ar)
        * [Decree No. 5112 of 1994](https://web.archive.org/web/20250704170257/http://legiliban.ul.edu.lb/LawArticles.aspx?LawTreeSectionID=173902&LawID=171663&language=ar)
        * [Decree No. 15215 of 2005](https://web.archive.org/web/20250704170428/http://legiliban.ul.edu.lb/LawArticles.aspx?LawTreeSectionID=213954&LawID=211715&language=ar)
        * [Decree No. 16237 of 2006](https://web.archive.org/web/20250704170424/http://legiliban.ul.edu.lb/LawArticles.aspx?LawTreeSectionID=215180&LawID=212941&language=ar)
        * [Decree No. 3369 of 2010](https://web.archive.org/web/20250704170437/http://legiliban.ul.edu.lb/LawArticles.aspx?LawTreeSectionID=227216&LawID=224978&language=ar)
        * <https://web.archive.org/web/20200925173058/https://www.abl.org.lb/english/abl-and-banking-sector-news/official-holidays>
    """

    country = "LB"
    default_language = "ar"
    # %s (estimated).
    estimated_label = tr("%s (المقدرة)")
    # %s (observed).
    observed_label = tr("%s (يُحتفل به)")
    # %s (observed, estimated).
    observed_estimated_label = tr("%s (يُحتفل به، المقدرة)")
    start_year = 1978
    supported_categories = (BANK, GOVERNMENT, PUBLIC)
    supported_languages = ("ar", "en_US")

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.
        """
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(
            self, cls=LebanonIslamicHolidays, show_estimated=islamic_show_estimated
        )
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("رأس السنة الميلادية"))

        if self._year >= 1995:
            # Armenian Orthodox Christmas Day.
            self._add_holiday_jan_6(tr("عيد الميلاد عند الطوائف الارمنية الارثوذكسية"))

        # Saint Maron's Day.
        self._add_holiday_feb_9(tr("عيد مار مارون"))

        if 1995 <= self._year <= 2005 or self._year >= 2010:
            # Feast of the Annunciation.
            self._add_holiday_mar_25(tr("عيد بشارة السيدة مريم العذراء"))

        # Catholic Good Friday.
        catholic_good_friday = self._add_good_friday(tr("الجمعة العظيمة عند الطوائف الكاثوليكية"))

        orthodox_good_friday = self._add_good_friday(
            # Orthodox Good Friday.
            tr("الجمعة العظيمة عند الطوائف الأرثوذكسية"),
            calendar=JULIAN_CALENDAR,
        )

        if orthodox_good_friday == catholic_good_friday:
            # Orthodox Holy Saturday.
            self._add_holy_saturday(tr("سبت النور للطائفة الأرثوذكسية"))

        if 1986 <= self._year <= 1994:
            catholic_easter_monday = self._add_easter_monday(
                # Catholic Easter Monday.
                tr("اثنين الفصح عند الطوائف الكاثوليكية")
            )

            orthodox_easter_monday = self._add_easter_monday(
                # Orthodox Easter Monday.
                tr("اثنين الفصح عند الطوائف الأرثوذكسية"),
                calendar=JULIAN_CALENDAR,
            )

            if catholic_easter_monday == orthodox_easter_monday:
                # Orthodox Easter Tuesday.
                self._add_easter_tuesday(tr("ثلاثاء الفصح للطوائف الأرثوذكسية"))

        # Labor Day.
        dt = self._add_labor_day(tr("عيد العمل"))
        if self._year >= 1995:
            self._add_observed(dt, rule=SUN_TO_NEXT_MON)

        # Martyrs' Day.
        name = tr("عيد الشهداء")
        if self._year <= 1993:
            self._add_holiday_1st_sun_from_may_6(name)
        elif self._year >= 2006:
            self._add_holiday_1st_sun_of_may(name)

        if self._year >= 2006:
            # Resistance and Liberation Day.
            self._add_holiday_2nd_sun_of_may(tr("عيد المقاومة والتحرير"))

        # Assumption Day.
        self._add_assumption_of_mary_day(tr("عيد انتقال العذراء"))

        if 1985 <= self._year <= 1993:
            # All Saints' Day.
            self._add_all_saints_day(tr("عيد جميع القديسين"))

        # Independence Day.
        self._add_holiday_nov_22(tr("ذكرى الاستقلال"))

        # Christmas Day.
        self._add_christmas_day(tr("عيد الميلاد"))

        # Islamic New Year.
        self._add_islamic_new_year_day(tr("عيد رأس السنة الهجرية"))

        # Ashura.
        self._add_ashura_day(tr("عاشوراء"))

        # Prophet's Birthday.
        self._add_mawlid_day(tr("ذكرى المولد النبوي الشريف"))

        # Eid al-Fitr.
        name = tr("عيد الفطر")
        self._add_eid_al_fitr_day(name)
        if self._year >= 1986:
            self._add_eid_al_fitr_day_two(name)
            if self._year <= 1994:
                self._add_eid_al_fitr_day_three(name)

        # Eid al-Adha.
        name = tr("عيد الأضحى")
        self._add_eid_al_adha_day(name)
        self._add_eid_al_adha_day_two(name)
        if 1986 <= self._year <= 1993:
            self._add_eid_al_adha_day_three(name)

    def _populate_bank_holidays(self):
        if self._year >= 2020:
            # Rafik Hariri Memorial Day.
            self._add_holiday_feb_14(tr("يوم ذكرى رفيق الحريري"))

            # Catholic Easter Monday.
            self._add_easter_monday(tr("اثنين الفصح عند الطوائف الكاثوليكية"))

            self._add_easter_monday(
                # Orthodox Easter Monday.
                tr("اثنين الفصح عند الطوائف الأرثوذكسية"),
                calendar=JULIAN_CALENDAR,
            )

    def _populate_government_holidays(self):
        if self._year >= 2020:
            # Rafik Hariri Memorial Day.
            self._add_holiday_feb_14(tr("يوم ذكرى رفيق الحريري"))

        if self._year >= 2021:
            # Anniversary of the tragedy of Beirut port explosion.
            self._add_holiday_aug_4(tr("ذكرى مأساة انفجار مرفأ بيروت"))


class LB(Lebanon):
    pass


class LBN(Lebanon):
    pass


class LebanonIslamicHolidays(_CustomIslamicHolidays):
    # https://web.archive.org/web/20250426220233/https://www.timeanddate.com/holidays/lebanon/first-day-ashura
    ASHURA_DATES_CONFIRMED_YEARS = (1978, 2024)
    ASHURA_DATES = {
        1978: (DEC, 11),
        1979: (NOV, 30),
        1981: (NOV, 8),
        1982: (OCT, 28),
        1983: (OCT, 17),
        1984: (OCT, 6),
        1985: (SEP, 25),
        1986: (SEP, 15),
        1987: (SEP, 4),
        1988: (AUG, 23),
        1989: (AUG, 13),
        1990: (AUG, 2),
        1991: (JUL, 22),
        1992: (JUL, 11),
        1995: (JUN, 9),
        1996: (MAY, 28),
        1997: (MAY, 18),
        1998: (MAY, 7),
        2003: (MAR, 14),
        2004: (MAR, 2),
        2010: (DEC, 17),
        2011: (DEC, 6),
        2013: (NOV, 14),
        2015: (OCT, 24),
        2016: (OCT, 12),
        2017: (OCT, 1),
        2018: (SEP, 21),
        2019: (SEP, 10),
        2020: (AUG, 30),
        2021: (AUG, 19),
        2022: (AUG, 9),
    }

    # https://web.archive.org/web/20250316072400/https://www.timeanddate.com/holidays/lebanon/eid-al-adha
    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (1978, 2025)
    EID_AL_ADHA_DATES = {
        1978: (NOV, 11),
        1979: (NOV, 1),
        1980: (OCT, 20),
        1981: (OCT, 9),
        1982: (SEP, 29),
        1983: (SEP, 18),
        1984: (SEP, 6),
        1985: (AUG, 27),
        1986: (AUG, 16),
        1987: (AUG, 6),
        1988: (JUL, 25),
        1989: (JUL, 14),
        1990: (JUL, 4),
        1991: (JUN, 23),
        1993: (JUN, 1),
        1994: (MAY, 21),
        1995: (MAY, 10),
        1996: (APR, 29),
        1997: (APR, 18),
        1998: (APR, 8),
        1999: (MAR, 28),
        2001: (MAR, 6),
        2002: (FEB, 23),
        2003: (FEB, 12),
        2004: (FEB, 2),
        2008: (DEC, 9),
        2009: (NOV, 28),
        2010: (NOV, 17),
        2011: (NOV, 7),
        2014: (OCT, 5),
        2015: (SEP, 24),
        2016: (SEP, 13),
        2017: (SEP, 2),
        2018: (AUG, 22),
        2019: (AUG, 12),
    }

    # https://web.archive.org/web/20250216042303/https://www.timeanddate.com/holidays/lebanon/eid-al-fitr
    EID_AL_FITR_DATES_CONFIRMED_YEARS = (1978, 2025)
    EID_AL_FITR_DATES = {
        1978: (SEP, 4),
        1979: (AUG, 25),
        1980: (AUG, 13),
        1981: (AUG, 2),
        1982: (JUL, 23),
        1983: (JUL, 12),
        1985: (JUN, 20),
        1986: (JUN, 9),
        1987: (MAY, 30),
        1988: (MAY, 18),
        1989: (MAY, 7),
        1990: (APR, 27),
        1991: (APR, 16),
        1993: (MAR, 25),
        1994: (MAR, 14),
        1995: (MAR, 3),
        1996: (FEB, 21),
        1997: (FEB, 9),
        1998: (JAN, 30),
        1999: (JAN, 19),
        2000: ((JAN, 8), (DEC, 28)),
        2001: (DEC, 17),
        2002: (DEC, 6),
        2003: (NOV, 26),
        2005: (NOV, 4),
        2006: (OCT, 24),
        2008: (OCT, 2),
        2009: (SEP, 21),
        2011: (AUG, 31),
        2014: (JUL, 29),
        2016: (JUL, 7),
        2017: (JUN, 26),
    }

    # https://web.archive.org/web/20250428034326/https://www.timeanddate.com/holidays/lebanon/muharram
    HIJRI_NEW_YEAR_DATES_CONFIRMED_YEARS = (1978, 2025)
    HIJRI_NEW_YEAR_DATES = {
        1978: (DEC, 2),
        1979: (NOV, 21),
        1981: (OCT, 30),
        1982: (OCT, 19),
        1983: (OCT, 8),
        1984: (SEP, 27),
        1985: (SEP, 16),
        1986: (SEP, 6),
        1987: (AUG, 26),
        1988: (AUG, 14),
        1989: (AUG, 4),
        1990: (JUL, 24),
        1991: (JUL, 13),
        1992: (JUL, 2),
        1995: (MAY, 31),
        1996: (MAY, 19),
        1997: (MAY, 9),
        1998: (APR, 28),
        2003: (MAR, 5),
        2004: (FEB, 22),
        2010: (DEC, 8),
        2011: (NOV, 27),
        2013: (NOV, 5),
        2016: (OCT, 3),
        2017: (SEP, 22),
    }

    # https://web.archive.org/web/20240908071444/https://www.timeanddate.com/holidays/lebanon/prophet-birthday
    MAWLID_DATES_CONFIRMED_YEARS = (1978, 2024)
    MAWLID_DATES = {
        1978: (FEB, 20),
        1979: (FEB, 10),
        1982: ((JAN, 8), (DEC, 28)),
        1983: (DEC, 17),
        1984: (DEC, 6),
        1985: (NOV, 25),
        1986: (NOV, 15),
        1987: (NOV, 4),
        1988: (OCT, 23),
        1989: (OCT, 13),
        1990: (OCT, 2),
        1991: (SEP, 21),
        1992: (SEP, 10),
        1993: (AUG, 30),
        1995: (AUG, 9),
        1996: (JUL, 28),
        1997: (JUL, 18),
        1998: (JUL, 7),
        2000: (JUN, 15),
        2003: (MAY, 14),
        2004: (MAY, 2),
        2006: (APR, 11),
        2011: (FEB, 16),
        2012: (FEB, 5),
        2014: (JAN, 14),
        2015: ((JAN, 3), (DEC, 24)),
        2016: (DEC, 12),
        2017: (DEC, 1),
    }
