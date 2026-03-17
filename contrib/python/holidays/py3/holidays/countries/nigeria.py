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
from holidays.groups import (
    ChristianHolidays,
    InternationalHolidays,
    IslamicHolidays,
    StaticHolidays,
)
from holidays.observed_holiday_base import ObservedHolidayBase, SAT_SUN_TO_NEXT_WORKDAY


class Nigeria(
    ObservedHolidayBase, ChristianHolidays, InternationalHolidays, IslamicHolidays, StaticHolidays
):
    """Nigeria holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Nigeria>
        * [Public Holidays Decree 1979](https://web.archive.org/web/20240616072641/https://archive.gazettes.africa/archive/ng/1979/ng-government-gazette-supplement-dated-1979-02-01-no-5.pdf)
        * [Public Holidays (Amendment) Decree 1984](https://web.archive.org/web/20240615185836/https://archive.gazettes.africa/archive/ng/1984/ng-government-gazette-supplement-dated-1984-08-30-no-52-part-a.pdf)
        * [Public Holidays Act (2004 Consolidated Version)](https://web.archive.org/web/20250822094939/https://placng.org/lawsofnigeria/view2.php?sn=467)
        * <https://web.archive.org/web/20250829024504/https://fmino.gov.ng/may-29th-in-nigerias-history/>
        * <https://web.archive.org/web/20250829084254/https://www.iita.org/wp-content/uploads/2017/01/The-Bulletin-23-27-May-2011-No.-2070.pdf>
        * <https://web.archive.org/web/20170618031224/http://pulse.ng/local/inauguration-day-fg-declares-may-29-public-holiday-id3795830.html>
        * <https://web.archive.org/web/20250829081956/https://dailypost.ng/2019/05/27/fg-declares-may-29-public-holiday/>
        * <https://web.archive.org/web/20230620060859/https://interior.gov.ng/fg-declares-monday-may-29th-2023-work-free-day-for-presidential-inauguration/>
        * <https://web.archive.org/web/20250829060151/https://statehouse.gov.ng/news/president-buhari-declares-june-12-the-new-democracy-day/>
        * <https://web.archive.org/web/20250829085227/https://www.nairaland.com/4548139/may-29-remains-inauguration-day>
        * <https://web.archive.org/web/20250829030023/https://www.timeanddate.com/holidays/nigeria/2025?hol=9>

    In-lieu holidays have been in effect since at least 2010:
        * <http://archive.today/2025.08.30-142719/https://www.vanguardngr.com/2010/05/fg-declares-may-31-public-holiday-for-democracy/>
    """

    country = "NG"
    default_language = "en_NG"
    # %s (estimated).
    estimated_label = tr("%s (estimated)")
    # %s (observed, estimated).
    observed_estimated_label = tr("%s (observed, estimated)")
    # %s (observed).
    observed_label = tr("%s (observed)")
    # Public Holidays Decree 1979, in effect from January 1st, 1979.
    start_year = 1979
    supported_languages = ("en_NG", "en_US")

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.

        In Nigeria, the dates of the Islamic calendar usually fall a day later than
        the corresponding dates in the Umm al-Qura calendar.
        """
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(
            self,
            cls=NigeriaIslamicHolidays,
            show_estimated=islamic_show_estimated,
            calendar_delta_days=+1,
        )
        StaticHolidays.__init__(self, NigeriaStaticHolidays)
        kwargs.setdefault("observed_rule", SAT_SUN_TO_NEXT_WORKDAY)
        kwargs.setdefault("observed_since", 2010)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        dts_observed = set()

        # New Year's Day.
        dts_observed.add(self._add_new_years_day(tr("New Year's Day")))

        # Good Friday.
        self._add_good_friday(tr("Good Friday"))

        # Easter Monday.
        self._add_easter_monday(tr("Easter Monday"))

        # Added via Public Holidays (Amendment) Decree 1984 on August 28th, 1984.
        if self._year >= 1985:
            # Workers' Day.
            dts_observed.add(self._add_labor_day(tr("Workers' Day")))

        # Added via Public Holidays (Amendment) Decree 2000.
        # Changed to June 12th for 2019 onwards on July 6th, 2018.
        if self._year >= 2000:
            # Democracy Day.
            name = tr("Democracy Day")
            dts_observed.add(
                self._add_holiday_jun_12(name)
                if self._year >= 2019
                else self._add_holiday_may_29(name)
            )

        if self._year >= 1999 and self._year % 4 == 3:
            # Presidential Inauguration Day.
            self._add_holiday_may_29(tr("Presidential Inauguration Day"))

        # National Day.
        dts_observed.add(self._add_holiday_oct_1(tr("National Day")))

        # Christmas Day.
        dts_observed.add(self._add_christmas_day(tr("Christmas Day")))

        # Boxing Day.
        dts_observed.add(self._add_christmas_day_two(tr("Boxing Day")))

        # Prophet's Birthday.
        dts_observed.update(self._add_mawlid_day(tr("Id el Maulud")))

        # Eid al-Fitr.
        dts_observed.update(self._add_eid_al_fitr_day(tr("Id el Fitr")))

        # Eid al-Fitr Holiday.
        dts_observed.update(self._add_eid_al_fitr_day_two(tr("Id el Fitr Holiday")))

        # Eid al-Adha.
        dts_observed.update(self._add_eid_al_adha_day(tr("Id el Kabir")))

        # Eid al-Adha Holiday.
        dts_observed.update(self._add_eid_al_adha_day_two(tr("Id el Kabir Holiday")))

        if self.observed:
            self._populate_observed(dts_observed)


class NG(Nigeria):
    pass


class NGA(Nigeria):
    pass


class NigeriaIslamicHolidays(_CustomIslamicHolidays):
    # https://web.archive.org/web/20250829051803/https://www.timeanddate.com/holidays/nigeria/id-el-kabir
    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (1979, 2025)
    EID_AL_ADHA_DATES = {
        1982: (SEP, 29),
        1987: (AUG, 6),
        1988: (JUL, 25),
        1990: (JUL, 4),
        1992: (JUN, 11),
        1996: (APR, 29),
        2000: (MAR, 16),
        2005: (JAN, 21),
        2006: ((JAN, 10), (DEC, 31)),
        2007: (DEC, 19),
        2012: (OCT, 26),
        2013: (OCT, 15),
        2014: (OCT, 6),
        2020: (JUL, 31),
        2021: (JUL, 20),
        2022: (JUL, 9),
        2023: (JUN, 28),
        2024: (JUN, 16),
        2025: (JUN, 6),
    }

    # https://web.archive.org/web/20250829052410/https://www.timeanddate.com/holidays/nigeria/id-el-fitr
    EID_AL_FITR_DATES_CONFIRMED_YEARS = (1979, 2025)
    EID_AL_FITR_DATES = {
        1979: (AUG, 25),
        1982: (JUL, 23),
        1984: (JUN, 30),
        1987: (MAY, 30),
        1988: (MAY, 18),
        1992: (APR, 4),
        1996: (FEB, 21),
        2000: ((JAN, 8), (DEC, 28)),
        2004: (NOV, 14),
        2007: (OCT, 12),
        2008: (SEP, 29),
        2010: (SEP, 9),
        2011: (AUG, 30),
        2013: (AUG, 8),
        2014: (JUL, 28),
        2015: (JUL, 17),
        2016: (JUL, 6),
        2018: (JUN, 15),
        2019: (JUN, 4),
        2020: (MAY, 24),
        2021: (MAY, 13),
        2022: (MAY, 2),
        2023: (APR, 21),
        2024: (APR, 10),
        2025: (MAR, 30),
    }

    # https://web.archive.org/web/20250829053435/https://www.timeanddate.com/holidays/nigeria/id-el-maulud
    # https://web.archive.org/web/20250829054319/https://pmnewsnigeria.com/2012/02/03/fg-declares-6-feb-public-holiday/
    MAWLID_DATES_CONFIRMED_YEARS = (1979, 2025)
    MAWLID_DATES = {
        1980: (JAN, 30),
        1981: (JAN, 18),
        1984: (DEC, 6),
        1989: (OCT, 13),
        1994: (AUG, 19),
        1997: (JUL, 18),
        1999: (JUN, 26),
        2001: (JUN, 4),
        2002: (MAY, 24),
        2005: (APR, 21),
        2007: (MAR, 31),
        2008: (MAR, 20),
        2009: (MAR, 9),
        2010: (FEB, 26),
        2012: (FEB, 6),
        2013: (JAN, 24),
        2014: (JAN, 13),
        2015: ((JAN, 2), (DEC, 24)),
        2018: (NOV, 20),
        2019: (NOV, 9),
        2020: (OCT, 29),
        2022: (OCT, 8),
        2023: (SEP, 27),
    }


class NigeriaStaticHolidays:
    """Nigeria special holidays.

    References:
        * <https://web.archive.org/web/20250829090324/https://www.pambazuka.org/governance/nigeria-holiday-blow-presidential-hopeful>
        * <https://web.archive.org/web/20250829084913/https://www.nbcnews.com/id/wbna36974655>
        * <https://web.archive.org/web/20250829081641/https://businessday.ng/uncategorized/article/2019-elections-fg-declares-friday-public-holiday-excludes-banksothers/>
        * <https://web.archive.org/web/20250829024209/https://statehouse.gov.ng/news/new-date-for-special-federal-executive-council-session-in-honour-of-president-muhammadu-buhari-to-be-announced/>
    """

    # Public Holiday for Elections.
    name_elections = tr("Public Holiday for Elections")

    special_public_holidays = {
        2007: (
            (APR, 12, name_elections),
            (APR, 13, name_elections),
        ),
        # Day of Mourning for President Umaru Yar'Adua.
        2010: (MAY, 6, tr("Day of Mourning for President Umaru Yar'Adua")),
        2019: (FEB, 22, name_elections),
        # Day of Mourning for President Muhammadu Buhari.
        2025: (JUL, 15, tr("Day of Mourning for President Muhammadu Buhari")),
    }
