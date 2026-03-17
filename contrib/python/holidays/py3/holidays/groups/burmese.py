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

from holidays.calendars.burmese import _BurmeseLunisolar
from holidays.calendars.gregorian import _timedelta


class BurmeseCalendarHolidays:
    """Burmese lunisolar calendar holidays."""

    def __init__(self) -> None:
        self._burmese_calendar = _BurmeseLunisolar()

    def _add_burmese_calendar_holiday(
        self, name: str, dt: date | None = None, days_delta: int = 0
    ) -> date | None:
        """Add Burmese calendar holiday."""

        if dt is None:
            return None

        if days_delta:
            dt = _timedelta(dt, days_delta)

        return self._add_holiday(name, dt)

    def _add_karen_new_year(self, name: str) -> set[date]:
        """Add Karen New Year holiday.

        The Karen New Year, also known as the Kayin New Year, is one of the major holidays
        celebrated by the Karen people. The Karen New Year falls on the first day of Pyatho,
        the tenth month in the Burmese calendar.
        https://en.wikipedia.org/wiki/Karen_New_Year

        Args:
            name:
                Holiday name.

        Returns:
            Set of dates of added holiday, empty if there is no holiday date for the current year.
        """
        return {
            dt
            for y in (self._year - 1, self._year)
            if (
                dt := self._add_burmese_calendar_holiday(
                    name, self._burmese_calendar.pyatho_waxing_moon_date(y)
                )
            )
        }

    def _add_kason_full_moon_day(self, name: str) -> date | None:
        """Add Full Moon Day of Kason holiday.

        Vesak is known as the Full Moon Day of Kason, which is the second month
        in the traditional Burmese calendar.
        https://en.wikipedia.org/wiki/Vesak#In_Myanmar_(Burma)

        Args:
            name:
                Holiday name.

        Returns:
            Date of added holiday, `None` if there is no holiday date for the current year.
        """
        return self._add_burmese_calendar_holiday(
            name, self._burmese_calendar.kason_full_moon_date(self._year)
        )

    def _add_myanmar_diwali(self, name: str) -> date | None:
        """Add Myanmar Diwali holiday.

        Diwali (Deepavali, Festival of Lights) is one of the most important festivals
        in Indian religions. In Myanmar, it is celebrated on first day of Tazaungmon,
        the eighth month of the Burmese calendar.
        https://en.wikipedia.org/wiki/Diwali

        Args:
            name:
                Holiday name.

        Returns:
            Date of added holiday, `None` if there is no holiday date for the current year.
        """
        return self._add_burmese_calendar_holiday(
            name, self._burmese_calendar.tazaungmon_waxing_moon_date(self._year)
        )

    def _add_myanmar_national_day(self, name: str) -> date | None:
        """Add Myanmar National Day holiday.

        National Day is a public holiday in Myanmar, marking the anniversary of the first
        university student strike at Rangoon University in 1920. The date is based on
        the traditional Burmese calendar, occurring on the 10th day following
        the full moon of Tazaungmon.
        https://en.wikipedia.org/wiki/National_Day_(Myanmar)

        Args:
            name:
                Holiday name.

        Returns:
            Date of added holiday, `None` if there is no holiday date for the current year.
        """
        return self._add_burmese_calendar_holiday(
            name, self._burmese_calendar.tazaungmon_full_moon_date(self._year), days_delta=+10
        )

    def _add_myanmar_new_year(
        self,
        name: str,
        extra_days_before: int = 0,
        extra_days_after: int = 0,
    ) -> set[date]:
        """Add Myanmar New Year (Thingyan, Water Festival).

        Thingyan, also known as the Myanmar New Year, is a festival that usually occurs
        in middle of April. Thingyan marks the transition from the old year to the new one,
        based on the traditional Myanmar lunisolar calendar. The festival usually spans
        four to five days.

        https://en.wikipedia.org/wiki/Thingyan.

        Args:
            name:
                Holiday name.

            extra_days_before:
                Number of additional holiday days preceding Akya.

            extra_days_after:
                Whether to add additional holiday days following Atat.

        Returns:
            Set of dates of added holiday, empty if there is no holiday date for the current year.
        """
        akya, atat = self._burmese_calendar.thingyan_dates(self._year)
        if akya is None or atat is None:
            return set()

        # Default length is from Thingyan Akyo (akya - 1) to New Year Day (atat + 1).
        # Optional additional days:
        # pre-Akya days (1 day in 2007-2016, 4 days in 2022-2023),
        # post-Atat days (3 or 4 days).
        begin = -1 - extra_days_before
        end = extra_days_after or (atat - akya).days + 1

        return {
            dt
            for delta in range(begin, end)
            if (dt := self._add_holiday(name, _timedelta(akya, delta)))
        }

    def _add_tabaung_full_moon_day(self, name: str) -> date | None:
        """Add Full Moon Day of Tabaung holiday.

        Māgha Pūjā (also written as Makha Bucha Day, Meak Bochea) is a Buddhist festival
        celebrated on the full moon day of Tabaung in Myanmar.
        https://en.wikipedia.org/wiki/Māgha_Pūjā#Myanmar_(Burma)

        Args:
            name:
                Holiday name.

        Returns:
            Date of added holiday, `None` if there is no holiday date for the current year.
        """
        return self._add_burmese_calendar_holiday(
            name, self._burmese_calendar.tabaung_full_moon_date(self._year - 1)
        )

    def _add_tazaungmon_full_moon_day(self, name: str) -> date | None:
        """Add Full Moon Day of Tazaungmon holiday.

        The Tazaungdaing Festival, also known as the Festival of Lights, held on the full moon day
        of Tazaungmon, the eighth month of the Burmese calendar.
        https://en.wikipedia.org/wiki/Tazaungdaing_festival

        Args:
            name:
                Holiday name.

        Returns:
            Date of added holiday, `None` if there is no holiday date for the current year.
        """
        return self._add_burmese_calendar_holiday(
            name, self._burmese_calendar.tazaungmon_full_moon_date(self._year)
        )

    def _add_thadingyut_full_moon_eve(self, name: str) -> date | None:
        """Add Pre-Full Moon Day of Thadingyut holiday.

        The Thadingyut Festival, also known as the Lighting Festival of Myanmar, is held
        on the full moon day of the Burmese lunar month of Thadingyut.
        https://en.wikipedia.org/wiki/Thadingyut_Festival

        Args:
            name:
                Holiday name.

        Returns:
            Date of added holiday, `None` if there is no holiday date for the current year.
        """
        return self._add_burmese_calendar_holiday(
            name, self._burmese_calendar.thadingyut_full_moon_date(self._year), days_delta=-1
        )

    def _add_thadingyut_full_moon_day(self, name: str) -> date | None:
        """Add Full Moon Day of Thadingyut holiday.

        The Thadingyut Festival, also known as the Lighting Festival of Myanmar, is held
        on the full moon day of the Burmese lunar month of Thadingyut.
        https://en.wikipedia.org/wiki/Thadingyut_Festival

        Args:
            name:
                Holiday name.

        Returns:
            Date of added holiday, `None` if there is no holiday date for the current year.
        """
        return self._add_burmese_calendar_holiday(
            name, self._burmese_calendar.thadingyut_full_moon_date(self._year)
        )

    def _add_thadingyut_full_moon_day_two(self, name: str) -> date | None:
        """Add Post-Full Moon Day of Thadingyut holiday.

        The Thadingyut Festival, also known as the Lighting Festival of Myanmar, is held
        on the full moon day of the Burmese lunar month of Thadingyut.
        https://en.wikipedia.org/wiki/Thadingyut_Festival

        Args:
            name:
                Holiday name.

        Returns:
            Date of added holiday, `None` if there is no holiday date for the current year.
        """
        return self._add_burmese_calendar_holiday(
            name, self._burmese_calendar.thadingyut_full_moon_date(self._year), days_delta=+1
        )

    def _add_waso_full_moon_day(self, name: str) -> date | None:
        """Add Full Moon Day of Waso holiday.

        Vassa is the three-month annual retreat observed by Theravada Buddhists.
        Vassa lasts for three lunar months, from the Burmese month of Waso to
        the Burmese month of Thadingyut.
        https://en.wikipedia.org/wiki/Vassa

        Args:
            name:
                Holiday name.

        Returns:
            Date of added holiday, `None` if there is no holiday date for the current year.
        """
        return self._add_burmese_calendar_holiday(
            name, self._burmese_calendar.waso_full_moon_date(self._year)
        )
