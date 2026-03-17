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

from holidays.calendars.gregorian import JAN, JUL
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    MON_TO_NEXT_TUE,
    SUN_TO_NEXT_MON,
    SUN_TO_NEXT_TUE,
)


class Barbados(ObservedHolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays):
    """Barbados holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Barbados>
        * <https://web.archive.org/web/20250415140739/https://www.timeanddate.com/holidays/barbados>
        * [Public Holidays Act Cap.352](https://web.archive.org/web/20250427133553/https://barbadosparliament-laws.com/en/showdoc/cs/352)
        * <https://web.archive.org/web/20240629135051/https://labour.gov.bb/pdf/Library/Other%20Docs/Public%20Holidays%20for%20the%20Year%202018.pdf>
        * <https://web.archive.org/web/20240630134528/https://labour.gov.bb/wp-content/uploads/2020/04/Public-Holidays-for-the-Year-2021.pdf>
        * <https://web.archive.org/web/20241104070357/https://gisbarbados.gov.bb/download/public-holidays-for-2022/>
        * <https://web.archive.org/web/20230803161905/https://gisbarbados.gov.bb/download/public-holidays-for-2023/>
    """

    country = "BB"
    observed_label = "%s (observed)"
    # Public Holidays Act Cap.352, 1968-12-30
    start_year = 1969

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, BarbadosStaticHolidays)
        kwargs.setdefault("observed_rule", SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day
        self._add_observed(self._add_new_years_day("New Year's Day"))

        # Errol Barrow Day
        if self._year >= 1989:
            self._add_observed(self._add_holiday_jan_21("Errol Barrow Day"))

        # Good Friday
        self._add_good_friday("Good Friday")

        # Easter Monday
        self._add_easter_monday("Easter Monday")

        # National Heroes Day
        if self._year >= 1998:
            self._add_observed(self._add_holiday_apr_28("National Heroes Day"))

        # May Day
        self._add_observed(self._add_labor_day("May Day"))

        # Whit Monday
        self._add_whit_monday("Whit Monday")

        # Emancipation Day
        name = "Emancipation Day"
        # If Aug 1 is Kadooment Day (i.e. Monday), observed on Tuesday.
        self._add_observed(self._add_holiday_aug_1(name), rule=SUN_TO_NEXT_TUE + MON_TO_NEXT_TUE)

        # Kadooment Day
        self._add_holiday_1st_mon_of_aug("Kadooment Day")

        # Independence Day
        self._add_observed(self._add_holiday_nov_30("Independence Day"))

        # Christmas
        self._add_observed(self._add_christmas_day("Christmas Day"), rule=SUN_TO_NEXT_TUE)

        # Boxing Day
        self._add_observed(self._add_christmas_day_two("Boxing Day"))


class BB(Barbados):
    pass


class BRB(Barbados):
    pass


class BarbadosStaticHolidays:
    special_public_holidays = {
        2021: (
            (JAN, 4, "Public Holiday"),
            (JAN, 5, "Public Holiday"),
        ),
        # One off 50th Anniversary of CARICOM Holiday.
        # See https://web.archive.org/web/20240805050828/https://gisbarbados.gov.bb/blog/one-off-bank-holiday-for-caricoms-50th-anniversary-celebrations/
        2023: (JUL, 31, "50th Anniversary of CARICOM Holiday"),
    }
