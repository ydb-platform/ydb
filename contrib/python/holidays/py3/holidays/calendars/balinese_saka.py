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

from holidays.calendars.gregorian import MAR, APR

NYEPI = "NYEPI"


class _BalineseSakaLunar:
    """
    Balinese Saka lunar calendar.

    The Balinese saka calendar is one of two calendars used on the Indonesian island
    of Bali. Unlike the 210-day pawukon calendar, it is based on the phases of the Moon,
    and is approximately the same length as the tropical year (solar year, Gregorian year).
    https://en.wikipedia.org/wiki/Balinese_saka_calendar
    """

    NYEPI_DATES = {
        1983: (MAR, 15),
        1984: (MAR, 4),
        1985: (MAR, 22),
        1986: (MAR, 12),
        1987: (MAR, 31),
        1988: (MAR, 19),
        1989: (MAR, 9),
        1990: (MAR, 27),
        1991: (MAR, 17),
        1992: (MAR, 5),
        1993: (MAR, 24),
        1994: (MAR, 12),
        1995: (APR, 1),
        1996: (MAR, 21),
        1997: (APR, 9),
        1998: (MAR, 29),
        1999: (MAR, 18),
        2000: (APR, 4),
        2001: (MAR, 25),
        2002: (APR, 13),
        2003: (APR, 2),
        2004: (MAR, 22),
        2005: (MAR, 11),
        2006: (MAR, 30),
        2007: (MAR, 19),
        2008: (MAR, 7),
        2009: (MAR, 26),
        2010: (MAR, 16),
        2011: (MAR, 5),
        2012: (MAR, 23),
        2013: (MAR, 12),
        2014: (MAR, 31),
        2015: (MAR, 21),
        2016: (MAR, 9),
        2017: (MAR, 28),
        2018: (MAR, 17),
        2019: (MAR, 7),
        2020: (MAR, 25),
        2021: (MAR, 14),
        2022: (MAR, 3),
        2023: (MAR, 22),
        2024: (MAR, 11),
        2025: (MAR, 29),
        2026: (MAR, 19),
        2027: (MAR, 8),
        2028: (MAR, 26),
        2029: (MAR, 15),
        2030: (MAR, 5),
        2031: (MAR, 24),
        2032: (MAR, 12),
        2033: (MAR, 31),
        2034: (MAR, 20),
        2035: (MAR, 10),
        2036: (MAR, 28),
        2037: (MAR, 17),
        2038: (MAR, 6),
        2039: (MAR, 25),
        2040: (MAR, 14),
        2041: (MAR, 3),
        2042: (MAR, 22),
        2043: (MAR, 11),
        2044: (MAR, 29),
        2045: (MAR, 19),
        2046: (MAR, 8),
        2047: (MAR, 27),
        2048: (MAR, 15),
        2049: (MAR, 5),
        2050: (MAR, 24),
    }

    def _get_holiday(self, holiday: str, year: int) -> date | None:
        dt = getattr(self, f"{holiday}_DATES", {}).get(year, ())
        return date(year, *dt) if dt else None

    def nyepi_date(self, year: int) -> date | None:
        """
        Data References:
            * [1983-2025](https://id.wikipedia.org/wiki/Indonesia_dalam_tahun_1983)
            * [2020-2050](https://web.archive.org/web/20240718011857/https://www.balitrips.com/balinese-temples-ceremony)
        """
        return self._get_holiday(NYEPI, year)
