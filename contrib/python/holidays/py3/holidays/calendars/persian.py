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

from holidays.calendars.gregorian import _timedelta


class _Persian:
    """
    Persian calendar (Solar Hijri) for 1901-2100 years.

    https://en.wikipedia.org/wiki/Solar_Hijri_calendar
    """

    START_YEAR = 1901
    END_YEAR = 2100

    def is_leap_year(self, year: int) -> bool:
        """
        Is Persian year that begins in the specified Gregorian year a leap year.
        """
        return (year % 33) in {3, 7, 11, 16, 20, 24, 28, 32}

    def new_year_date(self, year: int) -> date | None:
        """
        Return Gregorian date of Persian new year (1 Farvardin) in a given Gregorian year.
        """
        if year < _Persian.START_YEAR or year > _Persian.END_YEAR:
            return None

        day = 21
        if (
            (year % 4 == 1 and year >= 2029)
            or (year % 4 == 2 and year >= 2062)
            or (year % 4 == 3 and year >= 2095)
            or (year % 4 == 0 and 1996 <= year <= 2096)
        ):
            day = 20
        elif (year % 4 == 2 and year <= 1926) or (year % 4 == 3 and year <= 1959):
            day = 22
        return date(year, 3, day)

    def persian_to_gregorian(self, year: int, j_month: int, j_day: int) -> date | None:
        """
        Return Gregorian date of Persian day and month in a given Gregorian year.
        """
        start_date = self.new_year_date(year)
        if not start_date:
            return None

        m = j_month - 1
        delta = (31 * m if m < 6 else 186 + 30 * (m - 6)) + j_day - 1
        return _timedelta(start_date, delta)
