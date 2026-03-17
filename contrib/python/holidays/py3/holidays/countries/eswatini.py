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

import warnings

from holidays.calendars.gregorian import JAN, DEC
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.observed_holiday_base import ObservedHolidayBase, SUN_TO_NEXT_MON, SUN_TO_NEXT_TUE


class Eswatini(ObservedHolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays):
    """Eswatini holidays.

    References:
        * <https://web.archive.org/web/20171109031840/http://www.swazilii.org:80/sz/legislation/act/1938/71>
        * <https://web.archive.org/web/20250413193851/https://www.officeholidays.com/countries/swaziland>
    """

    country = "SZ"
    observed_label = "%s (observed)"
    start_year = 1939

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, cls=EswatiniStaticHolidays)
        kwargs.setdefault("observed_rule", SUN_TO_NEXT_MON)
        kwargs.setdefault("observed_since", 2021)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        self._add_observed(self._add_new_years_day("New Year's Day"))

        self._add_good_friday("Good Friday")

        self._add_easter_monday("Easter Monday")

        self._add_ascension_thursday("Ascension Day")

        if self._year >= 1987:
            self._add_observed(
                apr_19 := self._add_holiday_apr_19("King's Birthday"),
                rule=SUN_TO_NEXT_TUE if apr_19 == self._easter_sunday else SUN_TO_NEXT_MON,
            )

        if self._year >= 1969:
            self._add_observed(
                apr_25 := self._add_holiday_apr_25("National Flag Day"),
                rule=SUN_TO_NEXT_TUE if apr_25 == self._easter_sunday else SUN_TO_NEXT_MON,
            )

        self._add_observed(self._add_labor_day("Worker's Day"))

        if self._year >= 1983:
            self._add_observed(self._add_holiday_jul_22("Birthday of Late King Sobhuza"))

        self._add_observed(self._add_holiday_sep_6("Independence Day"))

        self._add_observed(self._add_christmas_day("Christmas Day"), rule=SUN_TO_NEXT_TUE)

        self._add_observed(self._add_christmas_day_two("Boxing Day"))


class Swaziland(Eswatini):
    def __init__(self, *args, **kwargs) -> None:
        warnings.warn("Swaziland is deprecated, use Eswatini instead.", DeprecationWarning)

        super().__init__(*args, **kwargs)


class SZ(Eswatini):
    pass


class SZW(Eswatini):
    pass


class EswatiniStaticHolidays:
    special_public_holidays = {
        # https://web.archive.org/web/20250413193906/https://mg.co.za/article/1999-12-09-swaziland-declares-bank-holidays/
        1999: (DEC, 31, "Y2K changeover"),
        2000: (JAN, 3, "Y2K changeover"),
    }
