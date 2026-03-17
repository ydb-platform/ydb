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

from datetime import date
from gettext import gettext as tr

from holidays.calendars import _CustomIslamicHolidays
from holidays.calendars.gregorian import JAN, MAY, JUL, AUG, SEP, FRI, SAT
from holidays.calendars.julian import JULIAN_CALENDAR
from holidays.groups import ChristianHolidays, IslamicHolidays
from holidays.holiday_base import HolidayBase


class Sudan(HolidayBase, ChristianHolidays, IslamicHolidays):
    """Sudan holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Sudan>
        * <https://web.archive.org/web/20250820070831/https://www.sudanembassy.org.uk/public-holidays/>
        * [Christian Holidays 2011-2018](https://web.archive.org/web/20250827155208/https://evangelicalfocus.com/world/5014/christmas-celebrations-mark-progress-of-religious-freedom-in-sudan)
    """

    country = "SD"
    default_language = "ar_SD"
    # %s (estimated).
    estimated_label = tr("%s (المقدرة)")
    supported_languages = ("ar_SD", "en_US")
    start_year = 1985
    weekend = {FRI, SAT}

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        ChristianHolidays.__init__(self)
        IslamicHolidays.__init__(
            self, cls=SudanIslamicHolidays, show_estimated=islamic_show_estimated
        )
        super().__init__(*args, **kwargs)

    def _get_weekend(self, dt: date) -> set[int]:
        # The resting days are Friday and Saturday since January 26th, 2008.
        # https://sudantribune.com/article/25544
        return {FRI, SAT} if dt >= date(2008, JAN, 26) else {FRI}

    def _populate_public_holidays(self):
        # Independence Day.
        self._add_holiday_jan_1(tr("عيد الإستقلال"))

        # Christian public holidays were suspended 2011–2018 and reinstated in 2019.
        if self._year <= 2010 or self._year >= 2019:
            # Coptic Christmas Day.
            self._add_christmas_day(tr("عيد الميلاد المجيد"), calendar=JULIAN_CALENDAR)

            # Coptic Easter.
            self._add_easter_sunday(tr("عيد الفصح القبطي"), calendar=JULIAN_CALENDAR)

            # Christmas Day.
            self._add_christmas_day(tr("عيد الميلاد"))

        # Islamic New Year.
        self._add_islamic_new_year_day(tr("رأس السنة الهجرية"))

        # Prophet's Birthday.
        self._add_mawlid_day(tr("المولد النبوي الشريف"))

        # Eid al-Fitr.
        name = tr("عيد الفطر المبارك")
        self._add_eid_al_fitr_day(name)
        self._add_eid_al_fitr_day_two(name)
        self._add_eid_al_fitr_day_three(name)

        if self._year >= 2020:
            self._add_eid_al_fitr_day_four(name)

        # Eid al-Adha.
        name = tr("عيد الأضحى المبارك")
        self._add_arafah_day(name)
        self._add_eid_al_adha_day(name)
        self._add_eid_al_adha_day_two(name)
        self._add_eid_al_adha_day_three(name)
        self._add_eid_al_adha_day_four(name)


class SD(Sudan):
    pass


class SDN(Sudan):
    pass


class SudanIslamicHolidays(_CustomIslamicHolidays):
    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2020, 2025)
    EID_AL_ADHA_DATES = {
        2022: (JUL, 10),
    }

    EID_AL_FITR_DATES_CONFIRMED_YEARS = (2020, 2025)
    EID_AL_FITR_DATES = {
        2022: (MAY, 1),
    }

    HIJRI_NEW_YEAR_DATES_CONFIRMED_YEARS = (2020, 2025)
    HIJRI_NEW_YEAR_DATES = {
        2021: (AUG, 11),
    }

    MAWLID_DATES_CONFIRMED_YEARS = (2020, 2025)
    MAWLID_DATES = {
        2023: (SEP, 28),
    }
