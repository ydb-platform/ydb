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

from holidays.calendars.gregorian import MAR, JUL, AUG, SEP
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.observed_holiday_base import ObservedHolidayBase, SUN_TO_NEXT_MON


class Zambia(ObservedHolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays):
    """Zambia holidays.

    References:
        * <https://web.archive.org/web/20241206135307/https://www.officeholidays.com/countries/zambia>
        * <https://web.archive.org/web/20250408170158/https://www.timeanddate.com/holidays/zambia/>
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Zambia>
        * <https://web.archive.org/web/20250329191121/http://www.parliament.gov.zm/sites/default/files/documents/acts/Public%20Holidays%20Act.pdf>
    """

    country = "ZM"
    observed_label = "%s (observed)"
    start_year = 1965

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, ZambiaStaticHolidays)
        kwargs.setdefault("observed_rule", SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_observed(self._add_new_years_day("New Year's Day"))

        if self._year >= 1991:
            self._add_observed(
                # International Women's Day.
                self._add_womens_day("International Women's Day")
            )

        # Youth Day.
        self._add_observed(self._add_holiday_mar_12("Youth Day"))

        # Good Friday.
        self._add_good_friday("Good Friday")

        # Holy Saturday.
        self._add_holy_saturday("Holy Saturday")

        # Easter Monday.
        self._add_easter_monday("Easter Monday")

        if self._year >= 2022:
            # Kenneth Kaunda Day.
            self._add_observed(self._add_holiday_apr_28("Kenneth Kaunda Day"))

        # Labour Day.
        self._add_observed(self._add_labor_day("Labour Day"))

        # Africa Freedom Day.
        self._add_observed(self._add_africa_day("Africa Freedom Day"))

        # Heroes' Day.
        self._add_holiday_1st_mon_of_jul("Heroes' Day")

        # Unity Day.
        self._add_holiday_1_day_past_1st_mon_of_jul("Unity Day")

        # Farmers' Day.
        self._add_holiday_1st_mon_of_aug("Farmers' Day")

        if self._year >= 2015:
            # National Prayer Day.
            self._add_observed(self._add_holiday_oct_18("National Prayer Day"))

        # Independence Day.
        self._add_observed(self._add_holiday_oct_24("Independence Day"))

        # Christmas Day.
        self._add_observed(self._add_christmas_day("Christmas Day"))


class ZM(Zambia):
    pass


class ZMB(Zambia):
    pass


class ZambiaStaticHolidays:
    special_public_holidays = {
        2016: (
            (AUG, 11, "General elections and referendum"),
            (SEP, 13, "Inauguration ceremony of President-elect and Vice President-elect"),
        ),
        2018: (
            (MAR, 9, "Public holiday"),
            (JUL, 26, "Lusaka mayoral and other local government elections"),
        ),
        2021: (
            (JUL, 2, "Memorial service for Kenneth Kaunda"),
            (JUL, 7, "Funeral of Kenneth Kaunda"),
            (AUG, 12, "General elections"),
            (AUG, 13, "Counting in general elections"),
            (AUG, 24, "Presidential inauguration"),
        ),
        2022: (MAR, 18, "Funeral of Rupiah Banda"),
    }
