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

from __future__ import annotations

from gettext import gettext as tr
from typing import TYPE_CHECKING

from holidays.calendars.gregorian import JAN, MAR, APR, MAY, JUN, SEP, DEC
from holidays.calendars.julian_revised import JULIAN_REVISED_CALENDAR
from holidays.constants import HALF_DAY, PUBLIC, SCHOOL
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.observed_holiday_base import ObservedHolidayBase, SAT_SUN_TO_NEXT_WORKDAY

if TYPE_CHECKING:
    from datetime import date


class Bulgaria(ObservedHolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays):
    """Bulgaria holidays.

    References:
        * [Labor Code](https://web.archive.org/web/20250402193136/https://lex.bg/laws/ldoc/1594373121)
        * [Labor Code changes - State Gazette, Issue 30, 13.04.1990](https://archive.org/details/30-1990)
        * [Labor Code changes - State Gazette, Issue 27, 05.04.1991](https://archive.org/details/27-1991_202511)
        * [Labor Code changes - State Gazette, Issue 104, 17.12.1991](https://archive.org/details/104-1991)
        * [Labor Code changes - State Gazette, Issue 88, 30.10.1992](https://archive.org/details/88-1992)
        * [Labor Code changes - State Gazette, Issue 2, 05.01.1996](https://archive.org/details/2-1996_202511)
        * [Labor Code changes - State Gazette, Issue 22, 24.02.1998](https://archive.org/details/22-1998)
        * [Labor Code changes - State Gazette, Issue 56, 19.05.1998](https://archive.org/details/56-1998)
        * [Labor Code changes - State Gazette, Issue 108, 15.09.1998](https://archive.org/details/108-1998)
        * [Labor Code changes - State Gazette, Issue 15, 23.02.2010](https://web.archive.org/web/20250515130122/https://dv.parliament.bg/DVWeb/showMaterialDV.jsp?idMat=29936)
        * [Labor Code changes - State Gazette, Issue 105, 30.12.2016](https://web.archive.org/web/20220509103925/https://dv.parliament.bg/DVWeb/showMaterialDV.jsp?idMat=110584)
        * [Labor Code changes - State Gazette, Issue 107, 18.12.2020](https://web.archive.org/web/20210410181539/https://dv.parliament.bg/DVWeb/showMaterialDV.jsp?idMat=154367)
        * [Bulgarian public holidays](https://web.archive.org/web/20240814165123/https://www.parliament.bg/bg/24)
        * [Working days and weekends calendar](https://web.archive.org/web/20250916112151/https://kik-info.com/spravochnik/calendar/2025/)
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Bulgaria>
    """

    country = "BG"
    default_language = "bg"
    # %s (observed).
    observed_label = tr("%s (почивен ден)")
    supported_categories = (HALF_DAY, PUBLIC, SCHOOL)
    supported_languages = ("bg", "en_US", "uk")
    # Labor Code changes - State Gazette, Issue 30, 13.04.1990.
    start_year = 1991

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self, JULIAN_REVISED_CALENDAR)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, BulgariaStaticHolidays)
        # Labor Code changes - State Gazette, Issue 105, 30.12.2016.
        kwargs.setdefault("observed_rule", SAT_SUN_TO_NEXT_WORKDAY)
        kwargs.setdefault("observed_since", 2017)
        super().__init__(*args, **kwargs)

    def _populate_observed(self, dts: set[date], *, multiple: bool = False) -> None:
        excluded_names = {
            # Holy Saturday.
            self.tr("Велика събота"),
            # Easter.
            self.tr("Великден"),
        }

        for dt in sorted(dts):
            for name in self.get_list(dt):
                if name not in excluded_names:
                    self._add_observed(dt, name)

    def _populate_public_holidays(self):
        dts_observed = set()

        # New Year's Day.
        dts_observed.add(self._add_new_years_day(tr("Нова година")))

        dts_observed.add(
            # Liberation Day.
            self._add_holiday_mar_3(tr("Ден на Освобождението на България от османско иго"))
        )

        # Labor Code changes - State Gazette, Issue 15, 23.02.2010.
        if self._year >= 2010:
            # Good Friday.
            self._add_good_friday(tr("Велики петък"))

            # Holy Saturday.
            self._add_holy_saturday(tr("Велика събота"))

        # Easter.
        name = tr("Великден")
        self._add_easter_sunday(name)
        # Labor Code changes - State Gazette, Issue 27, 05.04.1991.
        self._add_easter_monday(name)

        dts_observed.add(
            # Labor Day and International Workers' Solidarity Day.
            self._add_labor_day(tr("Ден на труда и на международната работническа солидарност"))
        )

        # Labor Code changes - State Gazette, Issue 56, 19.05.1998.
        if self._year >= 1999:
            dts_observed.add(
                # Saint George's Day, Day of the Bulgarian Army.
                self._add_holiday_may_6(tr("Гергьовден, Ден на храбростта и Българската армия"))
            )

        # Renamed by Labor Code changes - State Gazette, Issue 107, 18.12.2020.
        name = (
            # Day of the Holy Brothers Cyril and Methodius, Bulgarian Alphabet,
            # Enlightenment, Culture and Slavonic Literature.
            tr(
                "Ден на светите братя Кирил и Методий, на българската азбука, "
                "просвета и култура и на славянската книжовност"
            )
            if self._year >= 2021
            # Day of Bulgarian Enlightenment and Culture and Slavonic Alphabet.
            else tr("Ден на българската просвета и култура и на славянската писменост")
        )
        dts_observed.add(self._add_holiday_may_24(name))

        # Labor Code changes - State Gazette, Issue 22, 24.02.1998.
        if self._year >= 1998:
            # Unification Day.
            dts_observed.add(self._add_holiday_sep_6(tr("Ден на Съединението")))

        # Abandoned by Labor Code changes - State Gazette, Issue 104, 17.12.1991.
        if self._year <= 1991:
            # Freedom Day.
            self._add_holiday_sep_9(tr("Ден на свободата"))

        # Labor Code changes - State Gazette, Issue 108, 15.09.1998.
        if self._year >= 1998:
            # Independence Day.
            dts_observed.add(self._add_holiday_sep_22(tr("Ден на Независимостта на България")))

        # Labor Code changes - State Gazette, Issue 2, 05.01.1996.
        if self._year >= 1996:
            # Christmas Eve.
            dts_observed.add(self._add_christmas_eve(tr("Бъдни вечер")))

        # Labor Code changes - State Gazette, Issue 104, 17.12.1991.

        # Christmas Day.
        name = tr("Рождество Христово")
        dts_observed.add(self._add_christmas_day(name))
        dts_observed.add(self._add_christmas_day_two(name))

        if self.observed:
            self._populate_observed(dts_observed)

    def _populate_half_day_holidays(self):
        # Established as half-day by Labor Code changes - State Gazette, Issue 104, 17.12.1991.
        # Changed to full-day by Labor Code changes - State Gazette, Issue 2, 05.01.1996.
        if self._year <= 1995:
            # %s (from 2pm).
            begin_time_label = self.tr("%s (след 14 ч.)")

            # Christmas Eve.
            self._add_christmas_eve(begin_time_label % self.tr("Бъдни вечер"))

    def _populate_school_holidays(self):
        # Labor Code changes - State Gazette, Issue 88, 30.10.1992.
        if self._year >= 1992:
            # The Day of the People's Awakeners.
            self._add_holiday_nov_1(tr("Ден на народните будители"))


class BG(Bulgaria):
    pass


class BLG(Bulgaria):
    pass


class BulgariaStaticHolidays:
    """Bulgaria special holidays.

    Substituted holidays references:
        * [2004](https://web.archive.org/web/20251126005928/https://dv.parliament.bg/DVWeb/showMaterialDV.jsp?idMat=21156)
        * [2005](https://web.archive.org/web/20251126005738/https://dv.parliament.bg/DVWeb/showMaterialDV.jsp?idMat=14875)
        * [2006](https://web.archive.org/web/20251126005635/https://dv.parliament.bg/DVWeb/showMaterialDV.jsp?idMat=1142)
        * [2006 changes](https://web.archive.org/web/20251126005653/https://dv.parliament.bg/DVWeb/showMaterialDV.jsp?idMat=1246)
        * [2007](https://web.archive.org/web/20251126010247/https://dv.parliament.bg/DVWeb/showMaterialDV.jsp?idMat=3325)
        * [2007 changes](https://web.archive.org/web/20251126010247/https://dv.parliament.bg/DVWeb/showMaterialDV.jsp?idMat=3600)
        * [2008](https://web.archive.org/web/20251126010335/https://dv.parliament.bg/DVWeb/showMaterialDV.jsp?idMat=4686)
        * [2009](https://web.archive.org/web/20251126005722/https://dv.parliament.bg/DVWeb/showMaterialDV.jsp?idMat=12464)
        * [2009 changes](https://web.archive.org/web/20251126005900/https://dv.parliament.bg/DVWeb/showMaterialDV.jsp?idMat=16562)
        * [2009 changes](https://web.archive.org/web/20251126010145/https://dv.parliament.bg/DVWeb/showMaterialDV.jsp?idMat=27525)
        * [2010](https://web.archive.org/web/20251126010239/https://dv.parliament.bg/DVWeb/showMaterialDV.jsp?idMat=27993)
        * [2011](https://web.archive.org/web/20251126010309/https://dv.parliament.bg/DVWeb/showMaterialDV.jsp?idMat=41994)
        * [2012](https://web.archive.org/web/20251126010350/https://dv.parliament.bg/DVWeb/showMaterialDV.jsp?idMat=58790)
        * [2013](https://web.archive.org/web/20251126115613/https://dv.parliament.bg/DVWeb/showMaterialDV.jsp?idMat=70577)
        * [2013 changes](https://web.archive.org/web/20210410195503/https://dv.parliament.bg/DVWeb/showMaterialDV.jsp?idMat=74637)
        * [2013 changes](https://web.archive.org/web/20250908131607/https://dv.parliament.bg/DVWeb/showMaterialDV.jsp?idMat=80503)
        * [2014](https://web.archive.org/web/20231130064115/http://dv.parliament.bg/DVWeb/showMaterialDV.jsp?idMat=80502)
        * [2015](https://web.archive.org/web/20250523171159/https://dv.parliament.bg/DVWeb/showMaterialDV.jsp?idMat=89532)
        * [2016](https://web.archive.org/web/20250208071613/https://dv.parliament.bg/DVWeb/showMaterialDV.jsp?idMat=99143)

    Special holidays references:
        * [Resolution 220 of Mar 26, 2004](https://web.archive.org/web/20251126010024/https://dv.parliament.bg/DVWeb/showMaterialDV.jsp?idMat=21351)
        * [Resolution 722 of Oct 18, 2006](https://web.archive.org/web/20251126010114/https://dv.parliament.bg/DVWeb/showMaterialDV.jsp?idMat=2739)
        * [Resolution 808 of Nov 19, 2025](https://web.archive.org/web/20251126010045/https://dv.parliament.bg/DVWeb/showMaterialDV.jsp?idMat=239075)
    """

    # Substituted date format.
    substituted_date_format = tr("%d.%m.%Y")

    # Day off (substituted from %s).
    substituted_label = tr("Почивен ден (прехвърлен от %s)")

    # Official Holiday.
    official_holiday = tr("Официален празник")

    # Non-working day.
    non_working_day = tr("Неприсъствен ден")

    special_public_holidays = {
        2004: (
            (APR, 2, official_holiday),
            (MAY, 7, MAY, 15),
        ),
        2005: (
            (MAR, 4, MAR, 12),
            (MAY, 23, MAY, 28),
            (SEP, 5, SEP, 10),
            (SEP, 23, SEP, 17),
        ),
        2006: (JAN, 2, JAN, 28),
        2007: (
            (JAN, 2, official_holiday),
            (APR, 30, APR, 21),
            (MAY, 25, JUN, 2),
            (SEP, 7, SEP, 15),
            (DEC, 31, DEC, 15),
        ),
        2008: (
            (MAY, 2, MAY, 10),
            (MAY, 5, MAY, 17),
            (DEC, 31, DEC, 20),
        ),
        2009: (
            (JAN, 2, JAN, 10),
            (MAR, 2, MAR, 14),
            (MAY, 4, MAY, 16),
            (MAY, 5, MAY, 30),
            (SEP, 21, SEP, 26),
            (DEC, 31, DEC, 19),
        ),
        2010: (
            (MAY, 7, MAY, 15),
            (DEC, 31, DEC, 11),
        ),
        2011: (
            (MAR, 4, MAR, 19),
            (MAY, 23, MAY, 28),
            (SEP, 5, SEP, 3),
            (SEP, 23, SEP, 17),
        ),
        2012: (
            (JAN, 2, JAN, 21),
            (APR, 30, APR, 21),
            (MAY, 25, MAY, 19),
            (SEP, 7, SEP, 29),
            (DEC, 31, DEC, 15),
        ),
        2013: (
            (MAY, 2, MAY, 18),
            (DEC, 23, DEC, 21),
            (DEC, 31, DEC, 14),
        ),
        2014: (
            (MAY, 2, MAY, 10),
            (MAY, 5, MAY, 31),
            (DEC, 31, DEC, 13),
        ),
        2015: (
            (JAN, 2, JAN, 24),
            (MAR, 2, MAR, 21),
            (SEP, 21, SEP, 12),
            (DEC, 31, DEC, 12),
        ),
        2016: (
            (MAR, 4, MAR, 12),
            (MAY, 23, MAY, 14),
            (SEP, 5, SEP, 10),
            (SEP, 23, SEP, 17),
        ),
        2025: (DEC, 31, non_working_day),
        2026: (JAN, 2, non_working_day),
    }
