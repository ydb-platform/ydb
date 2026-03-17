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

from gettext import gettext as tr

from holidays.calendars.gregorian import MAY, SEP
from holidays.constants import GOVERNMENT, PUBLIC, WORKDAY
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    SAT_TO_NEXT_MON,
    SAT_TO_NEXT_TUE,
    SUN_TO_NEXT_TUE,
    SUN_TO_NEXT_WED,
    SAT_SUN_TO_NEXT_MON,
)


class FalklandIslands(
    ObservedHolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays
):
    """Falkland Islands holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_the_Falkland_Islands>
        * [January 31st, 2023 Amendment](https://web.archive.org/web/20250704040815/https://www.falklands.gov.fk/legalservices/statute-law-commissioner/gazettes-supplements/2023/supplements-2023?task=download.send&id=140)
        * <https://web.archive.org/web/20241201174202/https://www.timeanddate.com/holidays/falkland-islands/2025>
        * [Falkland Day](https://web.archive.org/web/20250325025508/https://www.daysoftheyear.com/days/falklands-day/)
        * [Peat Cutting Day](https://en.wikipedia.org/wiki/Peat_Cutting_Monday)
        * [HM The King's Birthday](https://web.archive.org/web/20230329063159/https://en.mercopress.com/2022/11/03/falklands-appoints-14-november-as-a-public-holiday-to-celebrate-birthday-of-king-charles-iii)
        * [Governor's Office Stanley](https://web.archive.org/web/20250704040807/https://www.gov.uk/world/organisations/governors-office-stanley/office/governors-office-stanley)
        * [2020-2023](https://web.archive.org/web/20220805134148/http://www.falklands.gov.fk/policy/jdownloads/Reports%20&%20Publications/Public%20Health,%20Social%20and%20Community%20Publications/Falkland%20Islands%20Public%20Holidays%202020-2023.pdf)
        * [2023-2026](https://web.archive.org/web/20250402143330/https://www.falklands.gov.fk/policy/downloads?task=download.send&id=186:falkland-islands-public-holidays-2023-2026&catid=12)
    """

    country = "FK"
    default_language = "en_GB"
    # %s observed.
    observed_label = tr("%s (observed)")
    # Falkland War's Conclusion.
    start_year = 1983
    supported_categories = (GOVERNMENT, PUBLIC, WORKDAY)
    supported_languages = ("en_GB", "en_US")

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, cls=FalklandIslandsStaticHolidays)
        kwargs.setdefault("observed_rule", SAT_SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_observed(self._add_new_years_day(tr("New Year's Day")))

        # Good Friday.
        self._add_good_friday(tr("Good Friday"))

        if self._year <= 2022:
            # Queen's Birthday.
            self._add_observed(self._add_holiday_apr_21(tr("HM The Queen's Birthday")))

        # Liberation Day.
        self._add_observed(self._add_holiday_jun_14(tr("Liberation Day")))

        if self._year <= 2001:
            # Falkland Day.
            self._add_observed(self._add_holiday_aug_14(tr("Falkland Day")))
        else:
            # Peat Cutting Day.
            self._add_holiday_1st_mon_of_oct(tr("Peat Cutting Day"))

        if self._year >= 2022:
            # King's Birthday.
            self._add_observed(self._add_holiday_nov_14(tr("HM The King's Birthday")))

        self._add_observed(
            # Christmas Day.
            self._add_christmas_day(tr("Christmas Day")),
            rule=SAT_TO_NEXT_TUE + SUN_TO_NEXT_WED,
        )

        self._add_observed(
            # Boxing Day.
            self._add_christmas_day_two(tr("Boxing Day")),
            rule=SAT_TO_NEXT_MON + SUN_TO_NEXT_WED,
        )

        self._add_observed(
            # Christmas Holiday.
            self._add_christmas_day_three(tr("Christmas Holiday")),
            rule=SAT_TO_NEXT_MON + SUN_TO_NEXT_TUE,
        )

    def _populate_government_holidays(self):
        # Government Holiday.
        name = tr("Government Holiday")
        last_workday = self._add_holiday(
            name, self._get_next_workday(self._next_year_new_years_day, -1)
        )
        self._add_holiday(name, self._get_next_workday(last_workday, -1))

    def _populate_workday_holidays(self):
        # Margaret Thatcher Day.
        self._add_holiday_jan_10(tr("Margaret Thatcher Day"))

        if self._year >= 2002:
            # Falkland Day.
            self._add_holiday_aug_14(tr("Falkland Day"))


class FK(FalklandIslands):
    pass


class FLK(FalklandIslands):
    pass


class FalklandIslandsStaticHolidays:
    """Falkland Islands special holidays.

    References:
        * <https://web.archive.org/web/20231004165250/https://en.mercopress.com/2022/09/14/falklands-public-holiday-and-national-two-minute-s-silence-for-queen-elizabeth-ii>
    """

    special_public_holidays = {
        # Queen Elizabeth II's Funeral.
        2022: (SEP, 19, tr("Queen Elizabeth II's Funeral")),
        # King Charles III's Coronation.
        2023: (MAY, 8, tr("HM The King's Coronation")),
    }
