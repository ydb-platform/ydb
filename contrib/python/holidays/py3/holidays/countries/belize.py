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
    TUE_WED_THU_TO_PREV_MON,
    FRI_SUN_TO_NEXT_MON,
)


class Belize(ObservedHolidayBase, ChristianHolidays, InternationalHolidays):
    """Belize holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Belize>
        * <https://web.archive.org/web/20250427131247/https://www.belizelaw.org/web/lawadmin/PDF%20files/cap289.pdf>
        * <https://web.archive.org/web/20250421081044/https://www.pressoffice.gov.bz/public-and-bank-holidays-2022-updated>
        * <https://web.archive.org/web/20250318103939/https://www.pressoffice.gov.bz/government-of-belize-establishes-new-public-and-bank-holidays/>
    """

    country = "BZ"
    observed_label = "%s (observed)"
    # Belize was granted independence on 21.09.1981.
    start_year = 1982

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        # Chapter 289 of the laws of Belize states that if the holiday falls
        # on a Sunday or a Friday, the following Monday is observed as public
        # holiday; further, if the holiday falls on a Tuesday, Wednesday or
        # Thursday, the preceding Monday is observed as public holiday
        kwargs.setdefault("observed_rule", SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._move_holiday(self._add_new_years_day("New Year's Day"))

        if self._year >= 2021:
            # George Price Day.
            self._move_holiday(self._add_holiday_jan_15("George Price Day"))

        # National Heroes and Benefactors Day.
        self._move_holiday(
            self._add_holiday_mar_9("National Heroes and Benefactors Day"),
            rule=TUE_WED_THU_TO_PREV_MON + FRI_SUN_TO_NEXT_MON,
        )

        # Good Friday.
        self._add_good_friday("Good Friday")

        # Holy Saturday.
        self._add_holy_saturday("Holy Saturday")

        # Easter Monday.
        self._add_easter_monday("Easter Monday")

        # Labour Day.
        self._move_holiday(self._add_labor_day("Labour Day"))

        if self._year <= 2021:
            # Commonwealth Day.
            self._move_holiday(
                self._add_holiday_may_24("Commonwealth Day"),
                rule=TUE_WED_THU_TO_PREV_MON + FRI_SUN_TO_NEXT_MON,
            )

        if self._year >= 2021:
            # Emancipation Day.
            self._move_holiday(
                self._add_holiday_aug_1("Emancipation Day"),
                rule=TUE_WED_THU_TO_PREV_MON + FRI_SUN_TO_NEXT_MON,
            )

        # Saint George's Caye Day.
        self._move_holiday(self._add_holiday_sep_10("Saint George's Caye Day"))

        # Independence Day.
        self._move_holiday(self._add_holiday_sep_21("Independence Day"))

        # Indigenous Peoples' Resistance Day / Pan American Day.
        name = "Indigenous Peoples' Resistance Day" if self._year >= 2021 else "Pan American Day"
        self._move_holiday(
            self._add_columbus_day(name), rule=TUE_WED_THU_TO_PREV_MON + FRI_SUN_TO_NEXT_MON
        )

        # Garifuna Settlement Day.
        self._move_holiday(self._add_holiday_nov_19("Garifuna Settlement Day"))

        # Christmas Day.
        self._add_christmas_day("Christmas Day")

        # Boxing Day.
        self._move_holiday(self._add_christmas_day_two("Boxing Day"))


class BZ(Belize):
    pass


class BLZ(Belize):
    pass
