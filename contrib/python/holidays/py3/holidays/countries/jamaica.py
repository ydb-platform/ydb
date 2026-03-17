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
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    SUN_TO_NEXT_MON,
    SUN_TO_NEXT_TUE,
    SAT_SUN_TO_NEXT_MON,
)


class Jamaica(ObservedHolidayBase, ChristianHolidays, InternationalHolidays):
    """Jamaica holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Jamaica>
        * <https://web.archive.org/web/20241110125301/https://www.mlss.gov.jm/wp-content/uploads/2017/11/The-Holidays-Public-General-Act.pdf>
    """

    country = "JM"
    observed_label = "%s (observed)"

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        kwargs.setdefault("observed_rule", SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day
        self._add_observed(self._add_new_years_day("New Year's Day"))

        # Ash Wednesday
        self._add_ash_wednesday("Ash Wednesday")

        # Good Friday
        self._add_good_friday("Good Friday")

        # Easter Monday
        self._add_easter_monday("Easter Monday")

        # National Labour Day
        self._add_observed(
            self._add_holiday_may_23("National Labour Day"), rule=SAT_SUN_TO_NEXT_MON
        )

        # Emancipation Day
        if self._year >= 1998:
            self._add_observed(self._add_holiday_aug_1("Emancipation Day"))

        # Independence Day
        self._add_observed(self._add_holiday_aug_6("Independence Day"))

        # National Heroes Day
        self._add_holiday_3rd_mon_of_oct("National Heroes Day")

        # Christmas Day
        self._add_observed(self._add_christmas_day("Christmas Day"), rule=SUN_TO_NEXT_TUE)

        # Boxing Day
        self._add_observed(self._add_christmas_day_two("Boxing Day"))


class JM(Jamaica):
    pass


class JAM(Jamaica):
    pass
