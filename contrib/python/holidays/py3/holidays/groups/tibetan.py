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

from holidays.calendars.tibetan import _TibetanLunisolar
from holidays.groups.eastern import EasternCalendarHolidays


class TibetanCalendarHolidays(EasternCalendarHolidays):
    """
    Tibetan lunisolar calendar holidays.
    """

    def __init__(self, cls=None, *, show_estimated=False) -> None:
        self._tibetan_calendar = cls() if cls else _TibetanLunisolar()
        self._tibetan_calendar_show_estimated = show_estimated

    def _add_tibetan_calendar_holiday(
        self, name: str, dt_estimated: tuple[date | None, bool], days_delta: int = 0
    ) -> date | None:
        """
        Add Tibetan calendar holiday.

        Adds customizable estimation label to holiday name if holiday date
        is an estimation.
        """
        return self._add_eastern_calendar_holiday(
            name,
            dt_estimated,
            show_estimated=self._tibetan_calendar_show_estimated,
            days_delta=days_delta,
        )

    def _add_blessed_rainy_day(self, name) -> date | None:
        """
        Add Blessed Rainy Day (September Equinox of Autumn Calendar).
        """
        return self._add_tibetan_calendar_holiday(
            name, self._tibetan_calendar.blessed_rainy_day_date(self._year)
        )

    def _add_birth_of_guru_rinpoche(self, name) -> date | None:
        """
        Add Birth of Guru Rinpoche (10th day of the 5th lunar month).
        """
        return self._add_tibetan_calendar_holiday(
            name, self._tibetan_calendar.birth_of_guru_rinpoche_date(self._year)
        )

    def _add_buddha_first_sermon(self, name) -> date | None:
        """
        Add Buddha First Sermon (4th day of the 6th lunar month).
        """
        return self._add_tibetan_calendar_holiday(
            name, self._tibetan_calendar.buddha_first_sermon_date(self._year)
        )

    def _add_buddha_parinirvana(self, name) -> date | None:
        """
        Add Buddha Parinirvana (15th day of the 4th lunar month).
        """
        return self._add_tibetan_calendar_holiday(
            name, self._tibetan_calendar.buddha_parinirvana_date(self._year)
        )

    def _add_day_of_offering(self, name) -> date | None:
        """
        Add Day of Offering (1st day of the 12th lunar month).
        """
        return self._add_tibetan_calendar_holiday(
            name, self._tibetan_calendar.day_of_offering_date(self._year)
        )

    def _add_death_of_zhabdrung(self, name) -> date | None:
        """
        Add Death of Zhabdrung (10th day of the 3rd lunar month).
        """
        return self._add_tibetan_calendar_holiday(
            name, self._tibetan_calendar.death_of_zhabdrung_date(self._year)
        )

    def _add_descending_day_of_lord_buddha(self, name) -> date | None:
        """
        Add Descending Day of Lord Buddha (22nd day of the 9th lunar month).
        """
        return self._add_tibetan_calendar_holiday(
            name, self._tibetan_calendar.descending_day_of_lord_buddha_date(self._year)
        )

    def _add_losar(self, name) -> date | None:
        """
        Add Losar (1st day of the 1st lunar month).
        """
        return self._add_tibetan_calendar_holiday(
            name, self._tibetan_calendar.losar_date(self._year)
        )

    def _add_losar_day_two(self, name) -> date | None:
        """
        Add Losar Day Two.
        """
        return self._add_tibetan_calendar_holiday(
            name, self._tibetan_calendar.losar_date(self._year), days_delta=+1
        )

    def _add_thimphu_drubchen_day(self, name) -> date | None:
        """
        Add Thimphu Drubchen (6th day of the 8th lunar month).
        """
        return self._add_tibetan_calendar_holiday(
            name, self._tibetan_calendar.thimphu_drubchen_date(self._year), days_delta=+1
        )

    def _add_thimphu_tshechu_day(self, name) -> date | None:
        """
        Add Thimphu Tshechu (10th day of the 8th lunar month).
        """
        return self._add_tibetan_calendar_holiday(
            name, self._tibetan_calendar.thimphu_tshechu_date(self._year)
        )

    def _add_thimphu_tshechu_day_two(self, name) -> date | None:
        """
        Add Thimphu Tshechu Day 2.
        """
        return self._add_tibetan_calendar_holiday(
            name, self._tibetan_calendar.thimphu_tshechu_date(self._year), days_delta=+1
        )

    def _add_thimphu_tshechu_day_three(self, name) -> date | None:
        """
        Add Thimphu Tshechu Day 3.
        """
        return self._add_tibetan_calendar_holiday(
            name, self._tibetan_calendar.thimphu_tshechu_date(self._year), days_delta=+2
        )

    def _add_tibetan_winter_solstice(self, name) -> date | None:
        """
        Add Winter Solstice Day.
        """
        return self._add_tibetan_calendar_holiday(
            name, self._tibetan_calendar.tibetan_winter_solstice_date(self._year)
        )
