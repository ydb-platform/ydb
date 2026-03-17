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

from holidays.calendars.mongolian import _MongolianLunisolar
from holidays.groups.eastern import EasternCalendarHolidays


class MongolianCalendarHolidays(EasternCalendarHolidays):
    """
    Mongolian lunisolar calendar holidays.
    """

    def __init__(self, cls=None, *, show_estimated=False) -> None:
        self._mongolian_calendar = cls() if cls else _MongolianLunisolar()
        self._mongolian_calendar_show_estimated = show_estimated

    def _add_mongolian_calendar_holiday(
        self, name: str, dt_estimated: tuple[date | None, bool], days_delta: int = 0
    ) -> date | None:
        """
        Add Mongolian calendar holiday.

        Adds customizable estimation label to holiday name if holiday date
        is an estimation.
        """

        return self._add_eastern_calendar_holiday(
            name,
            dt_estimated,
            show_estimated=self._mongolian_calendar_show_estimated,
            days_delta=days_delta,
        )

    def _add_buddha_day(self, name) -> date | None:
        """
        Add Buddha Day.

        Buddha Day is celebrated on the 15th day of the early summer month
        in the Mongolian lunisolar calendar.
        This holiday honors the birth, enlightenment, and death of Buddha.
        https://en.wikipedia.org/wiki/Vesak (general Buddhist context)
        """
        return self._add_mongolian_calendar_holiday(
            name, self._mongolian_calendar.buddha_day_date(self._year)
        )

    def _add_genghis_khan_day(self, name) -> date | None:
        """
        Add Genghis Khan's Birthday.

        Genghis Khan's Birthday is observed on the 1st day of the early winter month
        according to the Mongolian lunisolar calendar.
        It commemorates the birth of the founder of the Mongol Empire.
        https://en.wikipedia.org/wiki/Genghis_Khan
        """
        return self._add_mongolian_calendar_holiday(
            name, self._mongolian_calendar.genghis_khan_day_date(self._year)
        )

    def _add_tsagaan_sar(self, name) -> date | None:
        """
        Add Tsagaan Sar (Mongolian Lunar New Year).

        Tsagaan Sar, or White Moon Festival, marks the beginning of the Mongolian Lunar New Year.
        It usually falls on the first day of the first month of the Mongolian lunisolar calendar.
        https://en.wikipedia.org/wiki/Tsagaan_Sar
        """
        return self._add_mongolian_calendar_holiday(
            name, self._mongolian_calendar.tsagaan_sar_date(self._year)
        )

    def _add_tsagaan_sar_day_2(self, name) -> date | None:
        """
        Add Tsagaan Sar Day 2 (Mongolian Lunar New Year).

        Tsagaan Sar, or White Moon Festival, marks the beginning of the Mongolian Lunar New Year.
        It usually falls on the first day of the first month of the Mongolian lunisolar calendar.
        https://en.wikipedia.org/wiki/Tsagaan_Sar
        """
        return self._add_mongolian_calendar_holiday(
            name, self._mongolian_calendar.tsagaan_sar_date(self._year), days_delta=+1
        )

    def _add_tsagaan_sar_day_3(self, name) -> date | None:
        """
        Add Tsagaan Sar Day 3 (Mongolian Lunar New Year).

        Tsagaan Sar, or White Moon Festival, marks the beginning of the Mongolian Lunar New Year.
        It usually falls on the first day of the first month of the Mongolian lunisolar calendar.
        https://en.wikipedia.org/wiki/Tsagaan_Sar
        """
        return self._add_mongolian_calendar_holiday(
            name, self._mongolian_calendar.tsagaan_sar_date(self._year), days_delta=+2
        )
