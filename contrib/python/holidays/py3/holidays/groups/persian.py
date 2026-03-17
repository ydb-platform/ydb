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
from holidays.calendars.persian import _Persian


class PersianCalendarHolidays:
    """
    Persian (Solar Hijri) calendar holidays.
    """

    def __init__(self) -> None:
        self._persian_calendar = _Persian()

    def _add_death_of_khomeini_day(self, name: str) -> date | None:
        """
        Add Death of Ruhollah Khomeini Day (14th day of the 3rd month).

        Ayatollah Ruhollah Khomeini was an Iranian revolutionary, politician and religious leader.
        https://en.wikipedia.org/wiki/Ruhollah_Khomeini
        """
        return self._add_persian_calendar_holiday(
            name, self._persian_calendar.persian_to_gregorian(self._year, 3, 14)
        )

    def _add_islamic_emirat_victory_day(self, name: str) -> date | None:
        """
        Add Islamic Emirate Victory Day (24th day of the 5th month).

        Anniversary of the Taliban forces arrival in Kabul.
        https://en.wikipedia.org/wiki/Fall_of_Kabul_(2021)
        """
        return self._add_persian_calendar_holiday(
            name, self._persian_calendar.persian_to_gregorian(self._year, 5, 24)
        )

    def _add_islamic_republic_day(self, name: str) -> date | None:
        """
        Add Islamic Republic Day (12th day of the 1st month).

        Iranian Islamic Republic Day is a national and a public holiday in Iran. It marks the day
        that the results of the March 1979 Iranian Islamic Republic referendum were announced.
        https://en.wikipedia.org/wiki/Islamic_Republic_Day
        """
        return self._add_persian_calendar_holiday(
            name, self._persian_calendar.persian_to_gregorian(self._year, 1, 12)
        )

    def _add_islamic_revolution_day(self, name: str) -> date | None:
        """
        Add Islamic Revolution Day (22nd day of the 11th month).

        The anniversary of the Iranian Revolution commemorates the protests that led to
        the downfall of the Pahlavi dynasty and the installation of the Islamic Revolutionary
        which is headed by Imam Khomeini.
        https://en.wikipedia.org/wiki/Anniversary_of_Islamic_Revolution
        """
        return self._add_persian_calendar_holiday(
            name, self._persian_calendar.persian_to_gregorian(self._year - 1, 11, 22)
        )

    def _add_khordad_uprising_day(self, name: str) -> date | None:
        """
        Add 15 Khordad uprising Day (15th day of the 3rd month).

        The demonstrations of June 5 and 6, also called the events of June 1963 or (using
        the Iranian calendar) the 15 Khordad uprising were protests in Iran against the arrest
        of Ayatollah Ruhollah Khomeini.
        https://en.wikipedia.org/wiki/June_5,_1963_demonstrations_in_Iran
        """
        return self._add_persian_calendar_holiday(
            name, self._persian_calendar.persian_to_gregorian(self._year, 3, 15)
        )

    def _add_last_day_of_year(self, name: str) -> date | None:
        """
        If previous year is a leap year, its 12th month (Esfand) has 30 days,
        and this 30th day is a holiday.
        """
        if self._persian_calendar.is_leap_year(self._year - 1):
            return self._add_persian_calendar_holiday(
                name, self._persian_calendar.new_year_date(self._year), days_delta=-1
            )
        else:
            return None

    def _add_natures_day(self, name: str) -> date | None:
        """
        Add Nature's Day, or Sizdah Bedar (13th day of the 1st month).

        Nature's Day is an Iranian festival, during which people spend time picnicking outdoors.
        https://en.wikipedia.org/wiki/Sizdah_Be-dar
        """
        return self._add_persian_calendar_holiday(
            name, self._persian_calendar.persian_to_gregorian(self._year, 1, 13)
        )

    def _add_nowruz_day(self, name: str) -> date | None:
        """
        Add Nowruz Day (1st day of the 1st month).

        Nowruz (Iranian or Persian New Year) is a festival based on the Iranian Solar Hijri
        calendar, on the spring equinox.
        https://en.wikipedia.org/wiki/Nowruz
        """
        return self._add_persian_calendar_holiday(
            name, self._persian_calendar.new_year_date(self._year)
        )

    def _add_nowruz_day_two(self, name: str) -> date | None:
        """
        Add Nowruz Day Two.
        """
        return self._add_persian_calendar_holiday(
            name, self._persian_calendar.new_year_date(self._year), days_delta=+1
        )

    def _add_nowruz_day_three(self, name: str) -> date | None:
        """
        Add Nowruz Day Three.
        """
        return self._add_persian_calendar_holiday(
            name, self._persian_calendar.new_year_date(self._year), days_delta=+2
        )

    def _add_nowruz_day_four(self, name: str) -> date | None:
        """
        Add Nowruz Day Four.
        """
        return self._add_persian_calendar_holiday(
            name, self._persian_calendar.new_year_date(self._year), days_delta=+3
        )

    def _add_oil_nationalization_day(self, name: str) -> date | None:
        """
        Add Iranian Oil Industry Nationalization Day (29th day of the 12th month).

        The nationalization of the Iranian oil industry resulted from a movement in the Iranian
        parliament (Majlis) to seize control of Iran's oil industry.
        https://en.wikipedia.org/wiki/Nationalization_of_the_Iranian_oil_industry
        """
        return self._add_persian_calendar_holiday(
            name, self._persian_calendar.persian_to_gregorian(self._year - 1, 12, 29)
        )

    def _add_persian_calendar_holiday(
        self, name: str, dt: date | None, days_delta: int = 0
    ) -> date | None:
        """
        Add Persian calendar holiday.
        """
        if dt is None:
            return None
        if days_delta != 0:
            dt = _timedelta(dt, days_delta)
        return self._add_holiday(name, dt)
