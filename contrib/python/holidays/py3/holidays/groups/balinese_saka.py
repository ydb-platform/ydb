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

from holidays.calendars.balinese_saka import _BalineseSakaLunar


class BalineseSakaCalendarHolidays:
    """
    Balinese Saka lunar calendar holidays.
    """

    def __init__(self) -> None:
        self._balinese_saka_calendar = _BalineseSakaLunar()

    def _add_balinese_saka_calendar_holiday(self, name: str, dt: date | None) -> date | None:
        """
        Add Balinese Saka calendar holiday.
        """
        if dt is None:
            return None
        return self._add_holiday(name, dt)

    def _add_nyepi(self, name) -> date | None:
        """
        Add Nyepi (Day following the 9th of Dark Moon (Tilem)).

        Nyepi is a Balinese "Day of Silence" that is commemorated every
        Isakawarsa (Saka new year) according to the Balinese calendar.
        https://en.wikipedia.org/wiki/Nyepi
        """
        return self._add_balinese_saka_calendar_holiday(
            name, self._balinese_saka_calendar.nyepi_date(self._year)
        )
