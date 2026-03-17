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

from holidays.calendars import _CustomIslamicHolidays
from holidays.calendars.gregorian import FEB, MAR, JUN, JUL, AUG, SEP, OCT, NOV, DEC
from holidays.groups import ChristianHolidays, InternationalHolidays, IslamicHolidays
from holidays.holiday_base import HolidayBase


class Gabon(HolidayBase, ChristianHolidays, InternationalHolidays, IslamicHolidays):
    """Gabon holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Gabon>
        * <https://web.archive.org/web/20250414021427/https://www.timeanddate.com/holidays/gabon/>
        * <https://web.archive.org/web/20241206141530/https://www.officeholidays.com/countries/gabon>
        * <https://web.archive.org/web/20231211163448/https://www.travail.gouv.ga/402-evenements/489-liste-des-jours-feries/>
    """

    country = "GA"
    # %s (estimated).
    estimated_label = "%s (estimated)"
    # On 17 August 1960, Gabon gained independence from France.
    start_year = 1961

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
            self, cls=GabonIslamicHolidays, show_estimated=islamic_show_estimated
        )
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day("New Year's Day")

        # Easter Monday.
        self._add_easter_monday("Easter Monday")

        # Women's Rights Day.
        if self._year >= 2015:
            self._add_holiday_apr_17("Women's Rights Day")

        # Labour Day.
        self._add_labor_day("Labour Day")

        # Ascension Day.
        self._add_ascension_thursday("Ascension Day")

        # Whit Monday.
        self._add_whit_monday("Whit Monday")

        # Assumption Day.
        self._add_assumption_of_mary_day("Assumption Day")

        # Independence Day.
        self._add_holiday_aug_16("Independence Day")
        self._add_holiday_aug_17("Independence Day Holiday")

        # All Saints' Day.
        self._add_all_saints_day("All Saints' Day")

        # Christmas Day.
        self._add_christmas_day("Christmas Day")

        # Eid al-Fitr.
        self._add_eid_al_fitr_day("Eid al-Fitr")

        # Eid al-Adha.
        self._add_eid_al_adha_day("Eid al-Adha")


class GA(Gabon):
    pass


class GAB(Gabon):
    pass


class GabonIslamicHolidays(_CustomIslamicHolidays):
    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2001, 2023)
    EID_AL_ADHA_DATES = {
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
    }

    EID_AL_FITR_DATES_CONFIRMED_YEARS = (2001, 2024)
    EID_AL_FITR_DATES = {
        2001: (DEC, 17),
        2002: (DEC, 6),
        2003: (NOV, 26),
        2005: (NOV, 4),
        2006: (OCT, 24),
        2008: (OCT, 2),
        2009: (SEP, 21),
        2011: (AUG, 31),
        2014: (JUL, 29),
        2015: (JUL, 18),
        2016: (JUL, 7),
        2017: (JUN, 26),
    }
