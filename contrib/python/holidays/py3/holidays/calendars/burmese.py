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
from functools import cache

from holidays.calendars.gregorian import _timedelta

MO = 1954168.050623  # Beginning of 0 ME for MMT.
SY = 1577917828.0 / 4320000.0  # Solar year (365.2587565 days).


class _BurmeseLunisolar:
    """Burmese Lunisolar calendar.

    References:
        * [Algorithm, Program and Calculation of Myanmar Calendar](https://web.archive.org/web/20250510011425/http://cool-emerald.blogspot.com/2013/06/algorithm-program-and-calculation-of.html)
    """

    START_DATE = date(1939, 3, 20)  # 1 Late Tagu 1301 Burmese Era.
    START_YEAR = 1939  # 1301 Burmese Era.
    END_YEAR = 2100  # 1463 Burmese Era.

    LITTLE_WATAT_YEARS_GREGORIAN = {
        1939,
        1948,
        1955,
        1958,
        1966,
        1972,
        1974,
        1982,
        1988,
        1993,
        1999,
        2004,
        2012,
        2018,
        2020,
        2029,
        2034,
        2039,
        2045,
        2050,
        2056,
        2061,
        2069,
        2075,
        2077,
        2086,
        2091,
        2096,
    }

    BIG_WATAT_YEARS_GREGORIAN = {
        1942,
        1945,
        1950,
        1953,
        1961,
        1964,
        1969,
        1977,
        1980,
        1985,
        1991,
        1996,
        2001,
        2007,
        2010,
        2015,
        2023,
        2026,
        2031,
        2037,
        2042,
        2048,
        2053,
        2058,
        2064,
        2067,
        2072,
        2080,
        2083,
        2088,
        2094,
        2099,
    }

    @staticmethod
    def jdn_to_gregorian(jdn: int) -> date:
        """Convert Julian Day Number to Gregorian date.

        Args:
            jdn: Julian Day Number.

        Returns:
            Gregorian date.
        """
        j = jdn - 1721119
        y, j = divmod(4 * j - 1, 146097)
        d = j // 4
        j, d = divmod(4 * d + 3, 1461)
        d = (d + 4) // 4
        m, d = divmod(5 * d - 3, 153)
        d = (d + 5) // 5
        y = 100 * y + j
        if m < 10:
            m += 3
        else:
            m -= 9
            y += 1

        return date(y, m, d)

    @cache
    def _get_start_date(self, year: int) -> date | None:
        if year < self.START_YEAR or year > self.END_YEAR:
            return None

        delta_days = 354 * (year - self.START_YEAR)
        for iter_year in range(self.START_YEAR, year):
            if iter_year in self.LITTLE_WATAT_YEARS_GREGORIAN:
                delta_days += 30
            elif iter_year in self.BIG_WATAT_YEARS_GREGORIAN:
                delta_days += 31

        return _timedelta(self.START_DATE, delta_days)

    def thingyan_dates(self, year: int) -> tuple[date | None, date | None]:
        """Calculate key dates of Thingyan (Myanmar New Year festival) - Akya day
            and Atat day.

        Args:
            year:
                Gregorian year.

        Returns:
            Akya day date, Atat day date.

        """
        if year < self.START_YEAR or year > self.END_YEAR:
            return None, None

        ja = SY * (year - 638) + MO
        jk = (ja - 2.169918982) if year >= 1950 else (ja - 2.1675)

        return self.jdn_to_gregorian(round(jk)), self.jdn_to_gregorian(round(ja))

    def kason_full_moon_date(self, year: int) -> date | None:
        """Calculate the Gregorian date of Full Moon Day of Kason.

        15th day of 2nd month (Kason).

        To calculate, we use the following time delta:

        * All years:
            29 [1] + 15 - 1 = 43.

        Args:
            year:
                Gregorian year.

        Returns:
            Gregorian date of Full Moon Day of Kason.
            `None` if the Gregorian year is out of supported range.
        """
        if not (start_date := self._get_start_date(year)):
            return None

        return _timedelta(start_date, +43)

    def waso_full_moon_date(self, year: int) -> date | None:
        """Calculate the Gregorian date of Full Moon Day of Waso.

        15th day of 4th month (Waso).

        To calculate, we use the following time delta:

        * All years:
            Year_length - 266 [4-12] + 15 - 1 = Year_length - 252.

        Args:
            year:
                Gregorian year.

        Returns:
            Gregorian date of Full Moon Day of Waso.
            `None` if the Gregorian year is out of supported range.
        """
        if not (next_year_start_date := self._get_start_date(year + 1)):
            return None

        return _timedelta(next_year_start_date, -252)

    def thadingyut_full_moon_date(self, year: int) -> date | None:
        """Calculate the Gregorian date of Full Moon Day of Thadingyut.

        15th day of 7th month (Thadingyut).

        To calculate, we use the following time delta:

        * All years:
            Year_length - 177 [7-12] + 15 - 1 = Year_length - 163.

        Args:
            year:
                Gregorian year.

        Returns:
            Gregorian date of Full Moon Day of Thadingyut.
            `None` if the Gregorian year is out of supported range.
        """
        if not (next_year_start_date := self._get_start_date(year + 1)):
            return None

        return _timedelta(next_year_start_date, -163)

    def tazaungmon_waxing_moon_date(self, year: int) -> date | None:
        """Calculate the Gregorian date of 1st Waxing Day of Tazaungmon.

        1st day of 8th month (Tazaungmon).

        To calculate, we use the following time delta:

        * All years:
            Year_length - 148 [8-12] + 1 - 1 = Year_length - 148.

        Args:
            year:
                Gregorian year.

        Returns:
            Gregorian date of 1st Waxing Day of Tazaungmon.
            `None` if the Gregorian year is out of supported range.
        """
        if not (next_year_start_date := self._get_start_date(year + 1)):
            return None

        return _timedelta(next_year_start_date, -148)

    def tazaungmon_full_moon_date(self, year: int) -> date | None:
        """Calculate the Gregorian date of Full Moon Day of Tazaungmon.

        15th day of 8th month (Tazaungmon).

        To calculate, we use the following time delta:

        * All years:
            Year_length - 148 [8-12] + 15 - 1 = Year_length - 134.

        Args:
            year:
                Gregorian year.

        Returns:
            Gregorian date of Full Moon Day of Tazaungmon.
            `None` if the Gregorian year is out of supported range.
        """
        if not (next_year_start_date := self._get_start_date(year + 1)):
            return None

        return _timedelta(next_year_start_date, -134)

    def pyatho_waxing_moon_date(self, year: int) -> date | None:
        """Calculate the Gregorian date of 1st Waxing Day of Pyatho.

        1st day of 10th month (Pyatho).

        To calculate, we use the following time delta:

        * All years:
            Year_length - 89 [10-12] + 1 - 1 = Year_length - 89.

        Args:
            year:
                Gregorian year.

        Returns:
            Gregorian date of 1st Waxing Day of Pyatho.
            `None` if the Gregorian year is out of supported range.
        """
        if not (next_year_start_date := self._get_start_date(year + 1)):
            return None

        return _timedelta(next_year_start_date, -89)

    def tabaung_full_moon_date(self, year: int) -> date | None:
        """Calculate the Gregorian date of Full Moon Day of Tabaung.

        15th day of 12th month (Tabaung).

        To calculate, we use the following time delta:

        * All years:
            Year_length - 30 [12] + 15 - 1 = Year_length - 16.

        Args:
            year:
                Gregorian year.

        Returns:
            Gregorian date of Full Moon Day of Tabaung.
            `None` if the Gregorian year is out of supported range.
        """
        if not (next_year_start_date := self._get_start_date(year + 1)):
            return None

        return _timedelta(next_year_start_date, -16)
