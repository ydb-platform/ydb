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

from holidays.groups import ChristianHolidays, InternationalHolidays, IslamicHolidays
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    SAT_SUN_TO_NEXT_MON,
    SAT_SUN_TO_NEXT_MON_TUE,
)


class Ghana(ObservedHolidayBase, ChristianHolidays, InternationalHolidays, IslamicHolidays):
    """Ghana holidays.

    References:
        * <https://web.archive.org/web/20250114152956/https://www.mint.gov.gh/statutory-public-holidays/>
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Ghana>
    """

    country = "GH"
    estimated_label = "%s (estimated)"
    observed_label = "%s (observed)"
    observed_estimated_label = "%s (observed, estimated)"
    start_year = 1957

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.
        """
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(self, show_estimated=islamic_show_estimated)
        kwargs.setdefault("observed_rule", SAT_SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day
        self._add_observed(self._add_new_years_day("New Year's Day"))

        # Constitution Day
        if self._year >= 2019:
            self._add_observed(self._add_holiday_jan_7("Constitution Day"))

        # Independence Day
        self._add_observed(self._add_holiday_mar_6("Independence Day"))

        # Good Friday
        self._add_good_friday("Good Friday")

        # Easter Monday
        self._add_easter_monday("Easter Monday")

        # May Day(Workers' Day)
        self._add_observed(self._add_labor_day("May Day"))

        # Eid al-Fitr
        for dt in self._add_eid_al_fitr_day("Eid ul-Fitr"):
            self._add_observed(dt)

        # Eid al-Adha
        for dt in self._add_eid_al_adha_day("Eid ul-Adha"):
            self._add_observed(dt)

        # Founders' Day
        if self._year >= 2019:
            self._add_observed(self._add_holiday_aug_4("Founders' Day"))

        # Kwame Nkrumah Memorial Day / Founder's Day
        if self._year >= 2009:
            self._add_observed(
                self._add_holiday_sep_21(
                    "Kwame Nkrumah Memorial Day" if self._year >= 2019 else "Founder's Day"
                )
            )

        # Farmer's Day
        self._add_holiday_1st_fri_of_dec("Farmer's Day")

        # Christmas Day
        self._add_observed(self._add_christmas_day("Christmas Day"), rule=SAT_SUN_TO_NEXT_MON_TUE)

        # Boxing Day
        self._add_observed(self._add_christmas_day_two("Boxing Day"), rule=SAT_SUN_TO_NEXT_MON_TUE)


class GH(Ghana):
    pass


class GHA(Ghana):
    pass
