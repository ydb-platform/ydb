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

from holidays.calendars.buddhist import _BuddhistLunisolar
from holidays.groups.eastern import EasternCalendarHolidays


class BuddhistCalendarHolidays(EasternCalendarHolidays):
    """
    Buddhist lunisolar calendar holidays.
    """

    def __init__(self, cls=None, *, show_estimated=False) -> None:
        self._buddhist_calendar = cls() if cls else _BuddhistLunisolar()
        self._buddhist_calendar_show_estimated = show_estimated

    def _add_buddhist_calendar_holiday(
        self, name: str, dt_estimated: tuple[date | None, bool]
    ) -> date | None:
        """
        Add Buddhist calendar holiday.

        Adds customizable estimation label to holiday name if holiday date
        is an estimation.
        """
        return self._add_eastern_calendar_holiday(
            name, dt_estimated, show_estimated=self._buddhist_calendar_show_estimated
        )

    def _add_vesak(self, name) -> date | None:
        """
        Add Vesak (15th day of the 4th lunar month).

        Vesak for Thailand, Laos, Singapore and Indonesia.
        https://en.wikipedia.org/wiki/Vesak
        """
        return self._add_buddhist_calendar_holiday(
            name, self._buddhist_calendar.vesak_date(self._year)
        )

    def _add_vesak_may(self, name) -> date | None:
        """
        Add Vesak (on the day of the first full moon in May
        in the Gregorian calendar).

        Vesak for Sri Lanka, Nepal, India, Bangladesh and Malaysia.
        https://en.wikipedia.org/wiki/Vesak
        """
        return self._add_buddhist_calendar_holiday(
            name, self._buddhist_calendar.vesak_may_date(self._year)
        )
