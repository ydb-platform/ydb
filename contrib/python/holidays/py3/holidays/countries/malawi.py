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
    SAT_SUN_TO_NEXT_MON,
    SAT_SUN_TO_NEXT_MON_TUE,
)


class Malawi(ObservedHolidayBase, ChristianHolidays, InternationalHolidays):
    """Malawi holidays.

    References:
        * <https://web.archive.org/web/20250414071022/https://www.officeholidays.com/countries/malawi>
        * <https://web.archive.org/web/20250213080059/https://www.timeanddate.com/holidays/malawi/>
    """

    country = "MW"
    observed_label = "%s (observed)"
    start_year = 2000

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        kwargs.setdefault("observed_rule", SAT_SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        self._add_observed(self._add_new_years_day("New Year's Day"))

        self._add_observed(self._add_holiday_jan_15("John Chilembwe Day"))

        self._add_observed(self._add_holiday_mar_3("Martyrs Day"))

        self._add_good_friday("Good Friday")

        self._add_easter_monday("Easter Monday")

        self._add_observed(self._add_labor_day("Labour Day"))

        self._add_observed(self._add_holiday_may_14("Kamuzu Day"))

        self._add_observed(self._add_holiday_jul_6("Independence Day"))

        self._add_observed(self._add_holiday_oct_15("Mother's Day"))

        self._add_observed(self._add_christmas_day("Christmas Day"), rule=SAT_SUN_TO_NEXT_MON_TUE)

        self._add_observed(self._add_christmas_day_two("Boxing Day"), rule=SAT_SUN_TO_NEXT_MON_TUE)


class MW(Malawi):
    pass


class MWI(Malawi):
    pass
