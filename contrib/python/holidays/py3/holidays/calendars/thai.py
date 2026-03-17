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

KHMER_CALENDAR = "KHMER_CALENDAR"
THAI_CALENDAR = "THAI_CALENDAR"


class _ThaiLunisolar:
    """Thai Lunar Calendar Holidays.

    Works from 1913 (B.E. 2456/2455) onwards until 2157 (B.E. 2700), as we only have
    Thai year-type data for cross-checking through that period.

    ## The basics of the Thai Lunar Calendar:

    3-year types for calendar intercalation:
        * Pakatimat (Normal Year):
            Consists of 12 months, totaling 354 days.
        * Athikawan (Extra-Day Year):
            Adds one extra day to the 7th month, totaling 355 days for synodic month correction.
        * Athikamat (Extra-Month Year):
            Adds one extra 8th month, totaling 384 days for sidereal year correction.

    Months alternate between 30 (even months) and 29 (odd months) days.

    The waxing phase (Full Moon) lasts 15 days, while the waning phase (New Moon) lasts
    14 days for odd months (except Month 7 in Athikawan years), 15 days for even months.

    The second "Month 8" for Athikamat years is called "Month 8.8"
    (read as "the latter 8th month"), with all observed holidays
    delayed from the usual calendar by 30 days.

    Implemented Thai Lunar Calendar holiday methods:
        * Magha Puja / Makha Bucha / Meak Bochea:
            15th Waxing Day (Full Moon) of Month 3 (On Month 4 for Athikamat Years).
            `KHMER_CALENDAR` always falls on Month 3.

        * Vesak / Visakha Bucha / Visaka Bochea:
            15th Waxing Day (Full Moon) of Month 6 (On Month 7 for Athikamat Years).
            `KHMER_CALENDAR` always falls on Month 6.

        * Cambodian Royal Ploughing Ceremony / Preah Neangkol:
            4th Waning Day of Month 6 (On Month 7 for Athikamat Years).
            Defaults to `KHMER_CALENDAR` (its sole user).

        * Buddha's Cremation Day / Atthami Bucha:
            8th Waning Day of Month 6 (On Month 7 for Athikamat Years).
            `KHMER_CALENDAR` always falls on Month 6.

        * Asalha Puja / Asarnha Bucha:
            15th Waxing Day (Full Moon) of Month 8 (On Month 8.8 for Athikamat Years).

        * Buddhist Lent Day / Wan Khao Phansa:
            1st Waning Day of Month 8 (On Month 8.8 for Athikamat Years).

        * Boun Haw Khao Padapdin / Boon Khao Padap Din:
            14th Waning Day (New Moon) of Month 9.

        * Boun Haw Khao Salark / Boon Khao Sak:
            15th Waxing Day (Full Moon) of Month 10.

        * Pchum Ben / Prachum Bandar:
            15th Waning Day (New Moon) of Month 10.

        * Ok Boun Suang Huea / Vientiane Boat Racing Festival:
            1st Waning Day (New Moon) of Month 11.

        * Loy Krathong / Boun That Louang / Bon Om Touk:
            15th Waxing Day (Full Moon) of Month 12.

    Other Thai Lunar Calendar holidays:
        * Thai Royal Ploughing Ceremony / Raeknakhwan:
            Court astrologers choose the auspicious dates based on the Thai Lunar Calendar,
            but these dates do not follow a predictable pattern.
            See the specific section in `thailand.py` for more details.

        * End of Buddhist Lent Day / Ok Phansa:
            15th Waxing Day (Full Moon) of Month 11
            (Currently calculated based on Asalha Puja / Asarnha Bucha method).

    Notes:
        The following code is based on Ninenik Narkdee's PHP implementation,
        and we're thankful for his work.

    References:
        * <https://web.archive.org/web/20241016080156/https://www.ninenik.com/แนวทางฟังก์ชั่น_php_อย่างง่ายกับการหาวันข้างขึ้นข้างแรม-1021.html>
        * <https://web.archive.org/web/20250119171416/https://www.myhora.com/ปฏิทิน/ปฏิทิน-พ.ศ.2560.aspx>
        * <https://web.archive.org/web/20250302100842/https://calendar.kunthet.com/#/>
        * [2025 Khmer Lunar Calendar](https://web.archive.org/web/20250921045847/https://static1.squarespace.com/static/67722f65f2908e531b5f326d/t/678a87776503cc3f8bc538ac/1737131904673/2025Calendar-compressed.pdf)

    Example:

        >>> from holidays.calendars.thai import _ThaiLunisolar
        >>> thls = _ThaiLunisolar()
        >>> print(thls.visakha_bucha_date(2010))
        2010-05-28

    """

    # Athikawan (Extra-Day Year) list goes from 1914-2157 C.E.
    # Copied off from 1757-2157 (B.E. 2300-2700) Thai Lunar Calendar
    ATHIKAWAN_YEARS_GREGORIAN = {
        1914,
        1917,
        1925,
        1929,
        1933,
        1936,
        1945,
        1949,
        1952,
        1957,
        1963,
        1970,
        1973,
        1979,
        1987,
        1990,
        1997,
        2000,
        2006,
        2009,
        2016,
        2020,
        2025,
        2032,
        2035,
        2043,
        2046,
        2052,
        2055,
        2058,
        2067,
        2071,
        2076,
        2083,
        2086,
        2092,
        2097,
        2103,
        2109,
        2111,
        2117,
        2121,
        2126,
        2133,
        2136,
        2142,
        2147,
        2153,
    }

    # Athikamat (Extra-Month Year) list goes from 1914-2157 C.E.:
    # Copied off from 1757-2157 (B.E. 2300-2700) Thai Lunar Calendar
    # Approx formula as follows: (common_era-78)-0.45222)%2.7118886 < 1
    ATHIKAMAT_YEARS_GREGORIAN = {
        1915,
        1918,
        1920,
        1923,
        1926,
        1928,
        1931,
        1934,
        1937,
        1939,
        1942,
        1944,
        1947,
        1950,
        1953,
        1956,
        1958,
        1961,
        1964,
        1966,
        1969,
        1972,
        1975,
        1977,
        1980,
        1983,
        1985,
        1988,
        1991,
        1994,
        1996,
        1999,
        2002,
        2004,
        2007,
        2010,
        2012,
        2015,
        2018,
        2021,
        2023,
        2026,
        2029,
        2031,
        2034,
        2037,
        2040,
        2042,
        2045,
        2048,
        2050,
        2053,
        2056,
        2059,
        2062,
        2064,
        2066,
        2069,
        2072,
        2074,
        2077,
        2080,
        2082,
        2085,
        2088,
        2091,
        2094,
        2096,
        2099,
        2101,
        2104,
        2107,
        2112,
        2114,
        2116,
        2119,
        2122,
        2124,
        2127,
        2130,
        2132,
        2135,
        2138,
        2141,
        2144,
        2146,
        2149,
        2151,
        2154,
        2157,
    }

    # While Buddhist Holy Days have been observed since the 1900s
    #   Thailand's Public Holiday Act wasn't codified until 1914 (B.E. 2457)
    #   and that our array only goes up to B.E. 2700; We'll thus only populate
    #   the data for 1914-2157 (B.E. 2457-2700).
    # Sources: หนังสือเวียนกรมการปกครอง กระทรวงมหาดไทย มท 0310.1/ว4 5 ก.พ. 2539
    START_DATE = date(1913, 11, 28)
    START_YEAR = 1914
    END_YEAR = 2157

    def __init__(self, calendar=THAI_CALENDAR) -> None:
        self.__verify_calendar(calendar)
        self.__calendar = calendar

    @staticmethod
    def __is_khmer_calendar(calendar) -> bool:
        """Check if the given calendar is the Khmer calendar.

        Args:
            calendar:
                The calendar identifier to check.

        Returns:
            True if the calendar is `KHMER_CALENDAR`, False otherwise.
        """
        return calendar == KHMER_CALENDAR

    @staticmethod
    def __verify_calendar(calendar) -> None:
        """Verify calendar type."""
        if calendar not in {KHMER_CALENDAR, THAI_CALENDAR}:
            raise ValueError(
                f"Unknown calendar name: {calendar}. Use `KHMER_CALENDAR` or `THAI_CALENDAR`."
            )

    @cache
    def _get_start_date(self, year: int) -> date | None:
        """Calculate the start date of that particular Thai Lunar Calendar Year.

        This usually falls in November or December of the previous Gregorian
        year in question. Should the year be outside of working scope
        (1914-2157: B.E 2457-2700), this will returns None instead.

        Args:
            year:
                The Gregorian year.

        Returns:
             The start date of Thai Lunar Calendar for a Gregorian year.
        """
        if year < _ThaiLunisolar.START_YEAR or year > _ThaiLunisolar.END_YEAR:
            return None

        delta_days = 354 * (year - _ThaiLunisolar.START_YEAR)
        for iter_year in range(_ThaiLunisolar.START_YEAR, year):
            if iter_year in _ThaiLunisolar.ATHIKAMAT_YEARS_GREGORIAN:
                delta_days += 30
            elif iter_year in _ThaiLunisolar.ATHIKAWAN_YEARS_GREGORIAN:
                delta_days += 1

        return _timedelta(_ThaiLunisolar.START_DATE, delta_days)

    @cache
    def buddhist_sabbath_dates(self, year: int) -> set[date]:
        """Return all Buddhist Sabbath (Uposatha) days in a given Gregorian year.

        This function works independently of the calendar system in use,
        whether `THAI_CALENDAR` or `KHMER_CALENDAR`.

        Args:
            year:
                The Gregorian year.

        Returns:
            A set of `date` objects representing all Buddhist Sabbath days in the specified
            Gregorian year. Returns an empty set if the year is outside the supported range.
        """
        start_date = self._get_start_date(year)
        if not start_date:
            return set()

        # Initializes Thai lunar month lengths.
        months = [29, 30] * 6
        if year in _ThaiLunisolar.ATHIKAMAT_YEARS_GREGORIAN:
            months.insert(7, 30)
        elif year in _ThaiLunisolar.ATHIKAWAN_YEARS_GREGORIAN:
            months[6] += 1
        # Includes first two months of the next Thai lunar year to ensure all Buddhist Sabbaths
        # in the Gregorian year are captured.
        months.extend([29, 30])

        buddhist_sabbaths: set[date] = set()
        day_cursor = start_date
        for month_days in months:
            if day_cursor.year > year:
                break
            # Buddhist Sabbaths: 8 Waxing, 15 Waxing, 8 Waning, 14/15 Waning.
            for offset in (7, 14, 22, month_days - 1):
                buddhist_sabbath = _timedelta(day_cursor, offset)
                if buddhist_sabbath.year == year:
                    buddhist_sabbaths.add(buddhist_sabbath)
                elif buddhist_sabbath.year > year:
                    break
            day_cursor = _timedelta(day_cursor, month_days)

        return buddhist_sabbaths

    def makha_bucha_date(self, year: int, calendar=None) -> date | None:
        """Calculate the estimated Gregorian date of Makha Bucha.

        Also known as "Magha Puja", "Makha Buxha" and "Meak Bochea".
        This coincides with the 15th Waxing Day of Month 3
        in Thai Lunar Calendar, or Month 4 in Athikamat years.

        `KHMER_CALENDAR` will always use Month 3 regardless of year type.

        To calculate, we use the following time delta:

        * Athikamat:
            15th Waxing Day of Month 4 or 29[1] + 30[2] + 29[3] + 15[4] -1 = 102

        * Athikawan:
            15th Waxing Day of Month 3 or 29[1] + 30[2] + 15[3] -1 = 73

        * Pakatimat:
            15th Waxing Day of Month 3 or 29[1] + 30[2] + 15[3] -1 = 73

        Args:
            year:
                The Gregorian year.

            calendar:
                Calendar type, this defaults to THAI_CALENDAR.

        Returns:
            Estimated Gregorian date of Makha Bucha.
            Returns None if the Gregorian year input is invalid.
        """
        calendar = calendar or self.__calendar
        self.__verify_calendar(calendar)

        start_date = self._get_start_date(year)
        if not start_date:
            return None

        return _timedelta(
            start_date,
            +102
            if (
                year in _ThaiLunisolar.ATHIKAMAT_YEARS_GREGORIAN
                and not self.__is_khmer_calendar(calendar)
            )
            else +73,
        )

    def visakha_bucha_date(self, year: int, calendar=None) -> date | None:
        """Calculate the estimated Gregorian date of Visakha Bucha.

        Also known as "Vesak" and "Buddha Day". This coincides with
        the 15th Waxing Day of Month 6 in Thai Lunar Calendar, or Month 7 in Athikamat years.

        `KHMER_CALENDAR` will always use Month 6 regardless of year type.

        To calculate, we use use the following time delta:

        * Athikamat:
            15th Waxing Day of Month 7 or 177[1-6] + 15[7] -1 = 191

        * Athikawan:
            15th Waxing Day of Month 6 or 147[1-5] + 15[6] -1 = 161

        * Pakatimat:
            15th Waxing Day of Month 6 or 147[1-5] + 15[6] -1 = 161

        Args:
            year:
                The Gregorian year.

            calendar:
                Calendar type, this defaults to THAI_CALENDAR.

        Returns:
            Estimated Gregorian date of Visakha Bucha.
            Returns None if the Gregorian year input is invalid.
        """
        calendar = calendar or self.__calendar
        self.__verify_calendar(calendar)

        start_date = self._get_start_date(year)
        if not start_date:
            return None

        return _timedelta(
            start_date,
            +191
            if (
                year in _ThaiLunisolar.ATHIKAMAT_YEARS_GREGORIAN
                and not self.__is_khmer_calendar(calendar)
            )
            else +161,
        )

    def preah_neangkoal_date(self, year: int) -> date | None:
        """Calculate the estimated Gregorian date of Preah Neangkoal.

        Also known as "Cambodian Royal Ploughing Ceremony". This always
        coincides with the 4th Waning Day of Month 6 in Khmer Lunar Calendar.

        To calculate, we use use the following time delta:

        * Athikamat:
            4th Waning Day of Month 6 (Khmer Lunar Calendar) or 177[1-5] + 19[6] -1 = 165

        * Athikawan:
            4th Waning Day of Month 6 or 147[1-5] + 19[6] -1 = 165

        * Pakatimat:
            4th Waning Day of Month 6 or 147[1-5] + 19[6] -1 = 165

        Or as in simpler terms: "Visakha Bucha" (Khmer Lunar Calendar) +4.

        Args:
            year:
                The Gregorian year.

        Returns:
            Estimated Gregorian date of Preah Neangkoal.
        """
        start_date = self._get_start_date(year)
        if not start_date:
            return None

        return _timedelta(start_date, +165)

    def atthami_bucha_date(self, year: int, calendar=None) -> date | None:
        """Calculate the estimated Gregorian date of Atthami Bucha.

        Also known as "Buddha's Cremation Day". This coincides with
        the 8th Waning Day of Month 6 in Thai Lunar Calendar, or Month 7 in Athikamat years.

        `KHMER_CALENDAR` will always use Month 6 regardless of year type.

        To calculate, we use use the following time delta:

        * Athikamat:
            8th Waning Day of  Month 7 or 177[1-6] + 23[7] -1 = 199

        * Athikawan:
            8th Waning Day of  Month 6 or 147[1-5] + 23[6] -1 = 169

        * Pakatimat:
            8th Waning Day of  Month 6 or 147[1-5] + 23[6] -1 = 169

        Or as in simpler terms: "Visakha Bucha" +8

        Args:
            year:
                The Gregorian year.

            calendar:
                Calendar type, this defaults to THAI_CALENDAR.

        Returns:
            Estimated Gregorian date of Atthami Bucha.
            Returns None if the Gregorian year input is invalid.
        """
        calendar = calendar or self.__calendar
        self.__verify_calendar(calendar)

        start_date = self._get_start_date(year)
        if not start_date:
            return None

        return _timedelta(
            start_date,
            +199
            if (
                year in _ThaiLunisolar.ATHIKAMAT_YEARS_GREGORIAN
                and not self.__is_khmer_calendar(calendar)
            )
            else +169,
        )

    def asarnha_bucha_date(self, year: int) -> date | None:
        """Calculate the estimated Gregorian date of Asarnha Bucha.

        Also known as "Asalha Puja". This coincides with
        the 15th Waxing Day of Month 8 in Thai Lunar Calendar,
        or Month 8.8 in Athikamat years.

        Lao Start of Buddhist Lent start on this day (1-day earlier than Thai and Khmer ones).

        To calculate, we use the following time delta:

        * Athikamat:
            15th Waxing Day of Month 8/8 or 177[1-6] + 29[7] + 30[8] + 15[8.8] -1 = 250

        * Athikawan:
            15th Waxing Day of Month 8 or 177[1-6] + 30[7] + 15[8] -1 = 221

        * Pakatimat:
            15th Waxing Day of Month 8 or 177[1-6] + 29[7] + 15[8] -1 = 220

        Args:
            year:
                The Gregorian year.

        Returns:
            Estimated Gregorian date of Asarnha Bucha.
            Returns None if the Gregorian year input is invalid.
        """
        start_date = self._get_start_date(year)
        if not start_date:
            return None

        if year in _ThaiLunisolar.ATHIKAMAT_YEARS_GREGORIAN:
            delta_days = +250
        elif year in _ThaiLunisolar.ATHIKAWAN_YEARS_GREGORIAN:
            delta_days = +221
        else:
            delta_days = +220
        return _timedelta(start_date, delta_days)

    def khao_phansa_date(self, year: int) -> date | None:
        """Calculate the estimated Gregorian date of Khao Phansa.

        Also known as "(Start of) Buddhist Lent" and "Start of Vassa".
        This coincides with the 1st Waning Day of Month 8
        in Thai Lunar Calendar, or Month 8.8 in Athikamat years.

        To calculate, we use use the following time delta:

        * Athikamat:
            1st Waning Day of Month 8.8 or 177[1-6] + 29[7] + 30[8] + 16[8.8] -1 = 251

        * Athikawan:
            1st Waning Day of Month 8 or 177[1-6] + 30[7] + 16[8] -1 = 222

        * Pakatimat:
            1st Waning Day of Month 8 or 177[1-6] + 29[7] + 16[8] -1 = 221

        Or as in simpler terms: "Asarnha Bucha" +1

        Args:
            year:
                The Gregorian year.

        Returns:
            Estimated Gregorian date of Khao Phansa.
            Returns None if the Gregorian year input is invalid.
        """
        start_date = self._get_start_date(year)
        if not start_date:
            return None

        if year in _ThaiLunisolar.ATHIKAMAT_YEARS_GREGORIAN:
            delta_days = +251
        elif year in _ThaiLunisolar.ATHIKAWAN_YEARS_GREGORIAN:
            delta_days = +222
        else:
            delta_days = +221
        return _timedelta(start_date, delta_days)

    def boun_haw_khao_padapdin_date(self, year: int) -> date | None:
        """Calculate the estimated Gregorian date of Boun Haw Khao Padapdin.

        Also known as "Boon Khao Padap Din".
        This coincides with the 14th Waning Day of Month 9 in Thai Lunar Calendar.

        To calculate, we use use the following time delta:

        * Athikamat:
            14th Waning Day of Month 9 or 236[1-8] + 30[8.8] + 29[9] -1 = 294

        * Athikawan:
            14th Waning Day of Month 9 or 236[1-8] + 1[7] + 29[9] -1 = 265

        * Pakatimat:
            14th Waning Day of Month 9 or 236[1-8] + 29[9] -1 = 264

        Args:
            year:
                The Gregorian year.

        Returns:
            Estimated Gregorian date of Boun Haw Khao Padapdin.
            Returns None if the Gregorian year input is invalid.
        """
        start_date = self._get_start_date(year)
        if not start_date:
            return None

        if year in _ThaiLunisolar.ATHIKAMAT_YEARS_GREGORIAN:
            delta_days = +294
        elif year in _ThaiLunisolar.ATHIKAWAN_YEARS_GREGORIAN:
            delta_days = +265
        else:
            delta_days = +264
        return _timedelta(start_date, delta_days)

    def boun_haw_khao_salark_date(self, year: int) -> date | None:
        """Calculate the estimated Gregorian date of Boun Haw Khao Salark.

        Also known as "Boon Khao Sak".
        This coincides with the 15th Waxing Day of Month 10 in Thai Lunar Calendar.

        To calculate, we use use the following time delta:

        * Athikamat:
            15th Waxing Day of Month 10 or 265[1-9] + 30[8.8] + 15[10] -1 = 309

        * Athikawan:
            15th Waxing Day of Month 10 or 265[1-9] + 1[7] + 15[10] -1 = 280

        * Pakatimat:
            15th Waxing Day of Month 10 or 265[1-9] + 15[10] -1 = 279

        Args:
            year:
                The Gregorian year.

        Returns:
            Estimated Gregorian date of Boun Haw Khao Salark.
            Returns None if the Gregorian year input is invalid.
        """
        start_date = self._get_start_date(year)
        if not start_date:
            return None

        if year in _ThaiLunisolar.ATHIKAMAT_YEARS_GREGORIAN:
            delta_days = +309
        elif year in _ThaiLunisolar.ATHIKAWAN_YEARS_GREGORIAN:
            delta_days = +280
        else:
            delta_days = +279
        return _timedelta(start_date, delta_days)

    def pchum_ben_date(self, year: int) -> date | None:
        """Calculate the estimated Gregorian date of Pchum Ben.

        Also known as "Prachum Bandar".
        This coincides with the 15th Waning Day of Month 10 in Thai Lunar Calendar.

        To calculate, we use use the following time delta:

        * Athikamat:
            15th Waning Day of Month 10 or 265[1-9] + 30[8.8] + 30[10] -1 = 324

        * Athikawan:
            15th Waning Day of Month 10 or 265[1-9] + 1[7] + 30[10] -1 = 295

        * Pakatimat:
            15th Waning Day of Month 10 or 265[1-9] + 30[10] -1 = 294

        Args:
            year:
                The Gregorian year.

        Returns:
            Estimated Gregorian date of Pchum Ben.
            Returns None if the Gregorian year input is invalid.
        """
        start_date = self._get_start_date(year)
        if not start_date:
            return None

        if year in _ThaiLunisolar.ATHIKAMAT_YEARS_GREGORIAN:
            delta_days = +324
        elif year in _ThaiLunisolar.ATHIKAWAN_YEARS_GREGORIAN:
            delta_days = +295
        else:
            delta_days = +294
        return _timedelta(start_date, delta_days)

    def ok_phansa_date(self, year: int) -> date | None:
        """Calculate the estimated Gregorian date of Ok Phansa.

        Also known as "End of Buddhist Lent" and "End of Vassa".
        This coincides with the 15th Waxing Day of Month 11 in Thai Lunar Calendar.

        To calculate, we use use the following time delta:

        * Athikamat:
            15th Waxing Day of Month 11 or 295[1-10] + 30[8.8] + 15[11] -1 = 339

        * Athikawan:
            15th Waxing Day of Month 11 or 295[1-10] + 1[7] + 15[11] -1 = 310

        * Pakatimat:
            15th Waxing Day of Month 11 or 295[1-10] + 15[11] -1 = 309

        Args:
            year:
                The Gregorian year.

        Returns:
            Estimated Gregorian date of Ok Phansa.
            Returns None if the Gregorian year input is invalid.
        """
        start_date = self._get_start_date(year)
        if not start_date:
            return None

        if year in _ThaiLunisolar.ATHIKAMAT_YEARS_GREGORIAN:
            delta_days = +339
        elif year in _ThaiLunisolar.ATHIKAWAN_YEARS_GREGORIAN:
            delta_days = +310
        else:
            delta_days = +309
        return _timedelta(start_date, delta_days)

    def boun_suang_heua_date(self, year: int) -> date | None:
        """Calculate the estimated Gregorian date of Ok Boun Suang Huea.

        Boun Suang Huea Nakhone Luang Prabang, also known as "Vientiane Boat Racing Festival".
        This coincides with the 1st Waning Day of Month 11 in Thai Lunar Calendar.

        To calculate, we use use the following time delta:

        * Athikamat:
            1st Waning Day of Month 11 or 295[1-10] + 30[8.8] + 16[11] -1 = 340

        * Athikawan:
            1st Waning Day of Month 11 or 295[1-10] + 1[7] + 16[11] -1 = 311

        * Pakatimat:
            1st Waning Day of Month 11 or 295[1-10] + 16[11] -1 = 310

        Args:
            year:
                The Gregorian year.

        Returns:
            Estimated Gregorian date of Boun Suang Huea.
            Returns None if the Gregorian year input is invalid.
        """
        start_date = self._get_start_date(year)
        if not start_date:
            return None

        if year in _ThaiLunisolar.ATHIKAMAT_YEARS_GREGORIAN:
            delta_days = +340
        elif year in _ThaiLunisolar.ATHIKAWAN_YEARS_GREGORIAN:
            delta_days = +311
        else:
            delta_days = +310
        return _timedelta(start_date, delta_days)

    def loy_krathong_date(self, year: int) -> date | None:
        """Calculate the estimated Gregorian date of Loy Krathong.

        Also known as "Boun That Louang" and "Bon Om Touk".
        This coincides with the 15th Waxing Day of Month 12 in Thai Lunar Calendar.

        To calculate, we use use the following time delta:

        * Athikamat:
            15th Waxing Day of Month 12 or 324[1-11] + 30[8.8] + 15[11] -1 = 368

        * Athikawan:
            15th Waxing Day of Month 12 or 324[1-11] + 1[7] + 15[11] -1 = 339

        * Pakatimat:
            15th Waxing Day of Month 12 or 324[1-11] + 15[11] -1 = 338

        Args:
            year:
                The Gregorian year.

        Returns:
            Estimated Gregorian date of Loy Krathong.
            Returns None if the Gregorian year input is invalid.
        """
        start_date = self._get_start_date(year)
        if not start_date:
            return None

        if year in _ThaiLunisolar.ATHIKAMAT_YEARS_GREGORIAN:
            delta_days = +368
        elif year in _ThaiLunisolar.ATHIKAWAN_YEARS_GREGORIAN:
            delta_days = +339
        else:
            delta_days = +338
        return _timedelta(start_date, delta_days)
