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
from holidays.calendars.mandaean import _Mandaean


class MandaeanHolidays:
    """
    Mandaean holidays.
    """

    def __init__(self) -> None:
        self._mandaean = _Mandaean()

    @property
    def _dehwa_hanina(self):
        return self._mandaean.mandaean_to_gregorian(self._year - 1, 9, 23)

    @property
    def _great_feast(self):
        return self._mandaean.mandaean_to_gregorian(self._year, 1, 6)

    @property
    def _parwanaya(self):
        return self._mandaean.mandaean_to_gregorian(self._year - 1, 13, 1)

    def _add_mandaean_holiday(self, name: str, dt: date | None) -> date | None:
        """
        Add a Mandaean holiday.
        """
        if dt is None:
            return None
        return self._add_holiday(name, dt)

    def _add_parwanaya_day(self, name) -> date | None:
        """
        Add Parwanaya (Feast of Creation).
        A major Mandaean holiday celebrated over five days after the 8th month.
        The five intercalary days are considered as the 13th month.
        Marks the creation of the world and renewal of life.
        https://en.wikipedia.org/wiki/Parwanaya
        """
        return self._add_mandaean_holiday(name, self._parwanaya)

    def _add_parwanaya_day_two(self, name) -> date | None:
        return self._add_mandaean_holiday(name, _timedelta(self._parwanaya, +1))

    def _add_parwanaya_day_three(self, name) -> date | None:
        return self._add_mandaean_holiday(name, _timedelta(self._parwanaya, +2))

    def _add_parwanaya_day_four(self, name) -> date | None:
        return self._add_mandaean_holiday(name, _timedelta(self._parwanaya, +3))

    def _add_parwanaya_day_five(self, name) -> date | None:
        return self._add_mandaean_holiday(name, _timedelta(self._parwanaya, +4))

    def _add_dehwa_daimana_day(self, name) -> date | None:
        """
        Add Dehwa Daimana (Birthday of Prophet Yahya).
        Celebrated on 1st day of Mandaean 11th month.
        https://en.wikipedia.org/wiki/Dehwa_Daimana
        """
        return self._add_mandaean_holiday(
            name, self._mandaean.mandaean_to_gregorian(self._year - 1, 11, 1)
        )

    def _add_dehwa_hanina_day(self, name) -> date | None:
        """
        Add Dehwa Hanina (Little Feast / Day of Victory).
        Commemorates spiritual triumph and deliverance.
        Celebrated on 23rd day of the 9th Mandaean month.
        https://en.wikipedia.org/wiki/Dehwa_Hanina
        """
        return self._add_mandaean_holiday(name, self._dehwa_hanina)

    def _add_dehwa_hanina_day_two(self, name) -> date | None:
        return self._add_mandaean_holiday(name, _timedelta(self._dehwa_hanina, +1))

    def _add_great_feast_day(self, name) -> date | None:
        """
        Add Great Feast (Feast of the Great Shishlam).
        Celebrated on the 6th day of the first month of the Mandaean calendar.
        https://en.wikipedia.org/wiki/Feast_of_the_Great_Shishlam
        """
        return self._add_mandaean_holiday(name, self._great_feast)

    def _add_great_feast_day_two(self, name) -> date | None:
        return self._add_mandaean_holiday(name, _timedelta(self._great_feast, +1))

    def _add_great_feast_day_three(self, name) -> date | None:
        return self._add_mandaean_holiday(name, _timedelta(self._great_feast, +2))

    def _add_great_feast_day_four(self, name) -> date | None:
        return self._add_mandaean_holiday(name, _timedelta(self._great_feast, +3))
