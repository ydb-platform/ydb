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

from holidays.groups import ChristianHolidays, InternationalHolidays
from holidays.holiday_base import HolidayBase


class Liberia(HolidayBase, ChristianHolidays, InternationalHolidays):
    """Liberia holidays.

    References:
        * [Patriotic and Cultural Observances Law 1956](http://archive.today/2026.01.22-053103/http://www.liberlii.org/lr/legis/codes/pacolt25lcolr654/)

    Inauguration Day is celebrated on the first Monday of January in the year after the
    Presidential elections.
    """

    country = "LR"
    start_year = 1957

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day("New Year's Day")

        if self._year in {1960, 1964, 1968, 1972, 1976, 1986, 1998, 2006, 2012, 2018, 2024}:
            # Inauguration Day.
            self._add_holiday_1st_mon_of_jan("Inauguration Day")

        # Armed Forces Day.
        self._add_holiday_feb_11("Armed Forces Day")

        # Decoration Day.
        self._add_holiday_2nd_wed_of_mar("Decoration Day")

        # J. J. Roberts Memorial Birthday.
        self._add_holiday_mar_15("J. J. Roberts Memorial Birthday")

        # Fasting and Prayer Day.
        self._add_holiday_2nd_fri_of_apr("Fasting and Prayer Day")

        if self._year >= 1960:
            # National Unification and Integration Day.
            self._add_holiday_may_14("National Unification and Integration Day")

        # Independence Day.
        self._add_holiday_jul_26("Independence Day")

        # National Flag Day.
        self._add_holiday_aug_24("National Flag Day")

        # Thanksgiving Day.
        self._add_holiday_1st_thu_of_nov("Thanksgiving Day")

        # Tubman Administration Goodwill Day.
        self._add_holiday_nov_29("Tubman Administration Goodwill Day")

        # Christmas Day.
        self._add_christmas_day("Christmas Day")


class LR(Liberia):
    pass


class LBR(Liberia):
    pass
