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

from holidays.calendars.gregorian import MAY
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.holiday_base import HolidayBase


class Lesotho(HolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays):
    """Lesotho holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Lesotho>
        * <https://web.archive.org/web/20220508094900/https://www.ilo.org/dyn/travail/docs/2093/Public%20Holidays%20Act%201995.pdf>
        * <https://web.archive.org/web/20250317192347/https://www.timeanddate.com/holidays/lesotho/>
    """

    country = "LS"
    start_year = 1996

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, LesothoStaticHolidays)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day("New Year's Day")

        # Moshoeshoe's Day.
        self._add_holiday_mar_11("Moshoeshoe's Day")

        if self._year <= 2002:
            # Heroes Day.
            self._add_holiday_apr_4("Heroes Day")

        if self._year >= 2003:
            # Africa/Heroes Day.
            self._add_africa_day("Africa/Heroes Day")

        # Good Friday.
        self._add_good_friday("Good Friday")

        # Easter Monday.
        self._add_easter_monday("Easter Monday")

        # Workers' Day.
        self._add_labor_day("Workers' Day")

        # Ascension Day.
        self._add_ascension_thursday("Ascension Day")

        # https://en.wikipedia.org/wiki/Letsie_III
        # King's Birthday.
        name = "King's Birthday"
        if self._year >= 1998:
            self._add_holiday_jul_17(name)
        else:
            self._add_holiday_may_2(name)

        # Independence Day.
        self._add_holiday_oct_4("Independence Day")

        # Christmas Day.
        self._add_christmas_day("Christmas Day")

        # Boxing Day.
        self._add_christmas_day_two("Boxing Day")


class LS(Lesotho):
    pass


class LSO(Lesotho):
    pass


class LesothoStaticHolidays:
    special_public_holidays = {
        2002: (MAY, 25, "Africa Day"),
    }
