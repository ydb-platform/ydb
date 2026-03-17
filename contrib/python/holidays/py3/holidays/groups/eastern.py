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


class EasternCalendarHolidays:
    """
    Eastern calendar holidays base class.
    """

    def _add_eastern_calendar_holiday(
        self,
        name: str,
        dt_estimated: tuple[date | None, bool],
        *,
        show_estimated: bool = True,
        days_delta: int = 0,
    ) -> date | None:
        """
        Add Eastern (Buddhist, Chinese, Hindu, Islamic, Mongolian) calendar holiday.

        Adds customizable estimation label to holiday name if holiday date is an estimation.
        """
        dt, is_estimated = dt_estimated
        if days_delta and dt:
            dt = _timedelta(dt, days_delta)

        return (
            self._add_holiday(
                self.tr(self.estimated_label) % self.tr(name)
                if is_estimated and show_estimated
                else name,
                dt,
            )
            if dt
            else None
        )

    def _add_eastern_calendar_holiday_set(
        self,
        name: str,
        dts_estimated: Iterable[tuple[date, bool]],
        *,
        show_estimated: bool = True,
        days_delta: int = 0,
    ) -> set[date]:
        """
        Add Eastern (Buddhist, Chinese, Hindu, Islamic, Mongolian) calendar holidays.

        Adds customizable estimation label to holiday name if holiday date is an estimation.
        """

        return {
            dt
            for dt_estimated in dts_estimated
            if (
                dt := self._add_eastern_calendar_holiday(
                    name, dt_estimated, show_estimated=show_estimated, days_delta=days_delta
                )
            )
        }
