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

from holidays.calendars.gregorian import JUL, OCT
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.observed_holiday_base import ObservedHolidayBase, SUN_TO_NEXT_MON, MON_TO_NEXT_TUE


class Vanuatu(ObservedHolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays):
    """Vanuatu holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Vanuatu>
        * <https://web.archive.org/web/20250419100625/https://www.timeanddate.com/holidays/vanuatu>
        * <https://web.archive.org/web/20250419062251/https://gov.vu/index.php/events/holidays>
    """

    country = "VU"
    observed_label = "%s (observed)"
    # On 30 July 1980, Vanuatu gained independence from Britain and France.
    start_year = 1981

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, VanuatuStaticHolidays)
        kwargs.setdefault("observed_rule", SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Years Day.
        self._add_observed(self._add_new_years_day("New Year's Day"))

        if self._year >= 1999:
            # Father Lini Day.
            self._add_observed(self._add_holiday_feb_21("Father Lini Day"))

        # Custom Chief's Day.
        self._add_observed(self._add_holiday_mar_5("Custom Chief's Day"))

        # Good Friday.
        self._add_good_friday("Good Friday")

        # Easter Monday.
        self._add_easter_monday("Easter Monday")

        # Labour Day.
        self._add_observed(self._add_labor_day("Labour Day"))

        # Ascension Day.
        self._add_ascension_thursday("Ascension Day")

        # Children's Day.
        self._add_observed(self._add_holiday_jul_24("Children's Day"))

        # Independence Day.
        self._add_observed(self._add_holiday_jul_30("Independence Day"))

        # Assumption Day.
        self._add_observed(self._add_assumption_of_mary_day("Assumption Day"))

        # Constitution Day.
        self._add_observed(self._add_holiday_oct_5("Constitution Day"))

        # Unity Day.
        self._add_observed(self._add_holiday_nov_29("Unity Day"))

        # Christmas Day.
        self._add_christmas_day("Christmas Day")

        # Family Day.
        self._add_observed(
            self._add_christmas_day_two("Family Day"), rule=SUN_TO_NEXT_MON + MON_TO_NEXT_TUE
        )


class VU(Vanuatu):
    pass


class VTU(Vanuatu):
    pass


class VanuatuStaticHolidays:
    independence_anniversary = "40th Independence Anniversary"

    special_public_holidays = {
        2020: (
            (JUL, 23, independence_anniversary),
            (JUL, 27, independence_anniversary),
            (JUL, 28, independence_anniversary),
            (JUL, 29, independence_anniversary),
            (JUL, 31, independence_anniversary),
        ),
        2022: (OCT, 13, "Election Day"),
    }
