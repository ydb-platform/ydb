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

from collections.abc import Iterable
from datetime import date

from holidays.calendars.gregorian import _timedelta
from holidays.calendars.hebrew import _HebrewLunisolar


class HebrewCalendarHolidays:
    """
    Hebrew lunisolar calendar holidays.
    """

    def __init__(self) -> None:
        self._hebrew_calendar = _HebrewLunisolar()

    def _add_hebrew_calendar_holiday(
        self, name: str, holiday_date: date, days_delta: int | Iterable[int] = 0
    ) -> set[date]:
        """
        Add Hebrew calendar holiday.
        """
        return {
            dt
            for delta in ((days_delta,) if isinstance(days_delta, int) else days_delta)
            if (dt := self._add_holiday(name, _timedelta(holiday_date, delta)))
        }

    def _add_hanukkah(self, name: str, days_delta: int | Iterable[int] = 0) -> set[date]:
        """
        Add Hanukkah.
        In some Gregorian years, there may be two Hanukkah dates.

        Hanukkah is a Jewish festival commemorating the recovery of Jerusalem
        and subsequent rededication of the Second Temple.
        https://en.wikipedia.org/wiki/Hanukkah
        """
        return {
            dt
            for hanukkah_dt in self._hebrew_calendar.hanukkah_date(self._year)
            for dt in self._add_hebrew_calendar_holiday(name, hanukkah_dt, days_delta)
        }

    def _add_lag_baomer(self, name: str, days_delta: int | Iterable[int] = 0) -> set[date]:
        """
        Add Lag BaOmer.

        Lag BaOmer, also Lag B'Omer or Lag LaOmer, is a Jewish religious holiday celebrated
        on the 33rd day of the Counting of the Omer, which occurs on the 18th day of
        the Hebrew month of Iyar.
        https://en.wikipedia.org/wiki/Lag_BaOmer
        """
        return self._add_hebrew_calendar_holiday(
            name,
            self._hebrew_calendar.lag_baomer_date(self._year),  # type: ignore[arg-type]
            days_delta,
        )

    def _add_passover(self, name: str, days_delta: int | Iterable[int] = 0) -> set[date]:
        """
        Add Passover.

        Passover, also called Pesach, is a major Jewish holiday and one of the Three Pilgrimage
        Festivals. It celebrates the Exodus of the Israelites from slavery in Egypt.
        https://en.wikipedia.org/wiki/Passover
        """
        return self._add_hebrew_calendar_holiday(
            name,
            self._hebrew_calendar.passover_date(self._year),  # type: ignore[arg-type]
            days_delta,
        )

    def _add_purim(self, name: str) -> set[date]:
        """
        Add Purim.

        Purim is a Jewish holiday that commemorates the saving of the Jewish people
        from annihilation at the hands of an official of the Achaemenid Empire named Haman,
        as it is recounted in the Book of Esther.
        https://en.wikipedia.org/wiki/Purim
        """
        return self._add_hebrew_calendar_holiday(
            name,
            self._hebrew_calendar.purim_date(self._year),  # type: ignore[arg-type]
        )

    def _add_rosh_hashanah(self, name: str, days_delta: int | Iterable[int] = 0) -> set[date]:
        """
        Add Rosh Hashanah.

        Rosh Hashanah is the New Year in Judaism.
        https://en.wikipedia.org/wiki/Rosh_Hashanah
        """
        return self._add_hebrew_calendar_holiday(
            name,
            self._hebrew_calendar.rosh_hashanah_date(self._year),  # type: ignore[arg-type]
            days_delta,
        )

    def _add_shavuot(self, name: str) -> set[date]:
        """
        Add Shavuot.

        Shavuot, or Shvues, is a Jewish holiday, one of the biblically ordained
        Three Pilgrimage Festivals. It occurs on the sixth day of the Hebrew month of Sivan.
        https://en.wikipedia.org/wiki/Shavuot
        """
        return self._add_hebrew_calendar_holiday(
            name,
            self._hebrew_calendar.shavuot_date(self._year),  # type: ignore[arg-type]
        )

    def _add_sukkot(self, name: str, days_delta: int | Iterable[int] = 0) -> set[date]:
        """
        Add Sukkot.

        Sukkot, also known as the Feast of Tabernacles or Feast of Booths, is a Torah-commanded
        holiday celebrated for seven days, beginning on the 15th day of the month of Tishrei.
        https://en.wikipedia.org/wiki/Sukkot
        """
        return self._add_hebrew_calendar_holiday(
            name,
            self._hebrew_calendar.sukkot_date(self._year),  # type: ignore[arg-type]
            days_delta,
        )

    def _add_yom_kippur(self, name: str, days_delta: int | Iterable[int] = 0) -> set[date]:
        """
        Add Yom Kippur.

        Yom Kippur (Day of Atonement) is the holiest day of the year in Judaism.
        It occurs annually on the 10th of Tishrei.
        https://en.wikipedia.org/wiki/Yom_Kippur
        """
        return self._add_hebrew_calendar_holiday(
            name,
            self._hebrew_calendar.yom_kippur_date(self._year),  # type: ignore[arg-type]
            days_delta,
        )
