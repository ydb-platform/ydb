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


class _Mandaean:
    """
    Mandaean calendar for 1901-2100 years.
    https://en.wikipedia.org/wiki/Mandaean_calendar
    """

    # Begin of 445271 Mandaean year.
    START_DATE = date(1901, 8, 16)
    START_YEAR = 1901
    END_YEAR = 2100

    def new_year_date(self, year: int) -> date | None:
        """
        Return Gregorian date of Mandaean new year (1 Dowla) in a given Gregorian year.
        """
        if year < _Mandaean.START_YEAR or year > _Mandaean.END_YEAR:
            return None

        return _timedelta(_Mandaean.START_DATE, 365 * (year - _Mandaean.START_YEAR))

    def mandaean_to_gregorian(self, year: int, month: int, day: int) -> date | None:
        """
        Return Gregorian date of Mandaean day and month of the year that begins in a given
        Gregorian year.
        Extra 5 days inserted after 8th month are considered as 13th month.
        """
        start_date = self.new_year_date(year)
        if not start_date:
            return None

        if not (1 <= month <= 13) or not (1 <= day <= 30) or (month == 13 and not (1 <= day <= 5)):
            return None

        if month < 9:
            delta = 30 * (month - 1)
        elif month == 13:
            delta = 30 * 8
        else:
            delta = 30 * (month - 1) + 5

        return _timedelta(start_date, delta + day - 1)
