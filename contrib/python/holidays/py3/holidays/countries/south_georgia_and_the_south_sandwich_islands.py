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

from holidays.calendars.gregorian import APR, MAY, JUN, SEP, OCT, NOV
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    MON_TO_NEXT_TUE,
    SAT_SUN_TO_NEXT_MON,
    SAT_SUN_TO_NEXT_MON_TUE,
)


class SouthGeorgiaAndTheSouthSandwichIslands(
    ObservedHolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays
):
    """South Georgia and the South Sandwich Islands holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_South_Georgia_and_the_South_Sandwich_Islands>
        * [2012-2013](https://web.archive.org/web/20240810165616/https://laws.gov.gs/wp-content/uploads/2022/05/2012.pdf)
        * [2014](https://web.archive.org/web/20250726194445/https://laws.gov.gs/wp-content/uploads/2022/05/2013.pdf)
        * [2015](https://web.archive.org/web/20240810161237/https://laws.gov.gs/wp-content/uploads/2022/05/2014.pdf)
        * [2016](https://web.archive.org/web/20250726193915/https://laws.gov.gs/wp-content/uploads/2022/05/2015.pdf)
        * [2017](https://web.archive.org/web/20250726193700/https://laws.gov.gs/wp-content/uploads/2022/05/2016.pdf)
        * [2018](https://web.archive.org/web/20240810175520/https://laws.gov.gs/wp-content/uploads/2022/05/2017.pdf)
        * [2019](https://web.archive.org/web/20250726193031/https://laws.gov.gs/wp-content/uploads/2022/05/2018.pdf)
        * [2020](https://web.archive.org/web/20250726192724/https://laws.gov.gs/wp-content/uploads/2022/05/2019.pdf)
        * [2021](https://web.archive.org/web/20210911002738/https://www.gov.gs/docsarchive/Legislation/SGSSI%20Gazette%20No%202%20dated%2023%20December%202020.pdf)
        * [2022](https://web.archive.org/web/20250727082227/https://laws.gov.gs/wp-content/uploads/2022/05/2021.pdf)
        * [2023](https://web.archive.org/web/20240810165140/https://laws.gov.gs/wp-content/uploads/2024/03/Gazette2023.pdf)
        * [2024](https://web.archive.org/web/20240810163638/https://laws.gov.gs/wp-content/uploads/2024/02/SGSSI-Gazette-No-1-dated-27-February-2024.pdf)
        * [2025](https://web.archive.org/web/20250404025158/https://laws.gov.gs/wp-content/uploads/2025/01/SGSSI-Gazette-No-1-dated-30-January-2025.pdf)
    """

    country = "GS"
    default_language = "en_GS"
    # %s (observed).
    observed_label = tr("%s (observed)")
    supported_languages = ("en_GS", "en_US")
    start_year = 2012

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, SouthGeorgiaAndTheSouthSandwichIslandsStaticHolidays)
        kwargs.setdefault("observed_rule", SAT_SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_observed(self._add_new_years_day(tr("New Year's Day")))

        # Possession Day.
        self._add_observed(self._add_holiday_jan_17(tr("Possession Day")))

        # Good Friday.
        self._add_good_friday(tr("Good Friday"))

        if self._year <= 2019:
            # Easter Monday.
            self._add_easter_monday(tr("Easter Monday"))

        if 2018 <= self._year <= 2022:
            # The Queen's Birthday.
            dt = self._add_holiday_apr_21(tr("The Queen's Birthday"))
            if self._year not in {2019, 2020}:
                self._add_observed(dt)

        # Liberation Day.
        name = tr("Liberation Day")
        self._add_observed(
            self._add_holiday_apr_25(name)
            if self._year >= 2016
            else self._add_holiday_apr_26(name)
        )

        if self._year >= 2015:
            # Shackleton Day.
            dt = self._add_holiday_may_20(tr("Shackleton Day"))
            if self._year != 2023:
                self._add_observed(dt)

        # Mid-winter Day.
        dt = self._add_holiday_jun_21(tr("Mid-winter Day"))
        if self._year != 2025:
            self._add_observed(dt)

        if self._year <= 2021:
            if self._year >= 2015:
                # Toothfish Day.
                self._add_observed(self._add_holiday_sep_4(tr("Toothfish Day")))
            else:
                # Toothfish (End of Season) Day.
                dt = self._add_holiday_sep_14(tr("Toothfish (end of season) Day"))
                if self._year != 2013:
                    self._add_observed(dt)

        if self._year >= 2020:
            # Environment Day.
            dt = self._add_holiday_oct_30(tr("Environment Day"))
            if self._year != 2021:
                self._add_observed(dt)

        # Placed before Christmas Day for proper observed calculation.
        self._add_observed(
            # Boxing Day.
            self._add_christmas_day_two(tr("Boxing Day")),
            rule=SAT_SUN_TO_NEXT_MON_TUE + MON_TO_NEXT_TUE,
        )

        # Christmas Day.
        self._add_observed(self._add_christmas_day(tr("Christmas Day")))


class GS(SouthGeorgiaAndTheSouthSandwichIslands):
    pass


class SGS(SouthGeorgiaAndTheSouthSandwichIslands):
    pass


class SouthGeorgiaAndTheSouthSandwichIslandsStaticHolidays:
    """South Georgia and the South Sandwich Islands special holidays."""

    # The Queen's Platinum Jubilee.
    queens_platinum_jubilee = tr("The Queen's Platinum Jubilee")

    special_public_holidays = {
        2022: (
            (JUN, 2, queens_platinum_jubilee),
            (JUN, 3, queens_platinum_jubilee),
        ),
        2023: (
            # King Charles III's Coronation.
            (MAY, 8, tr("Coronation of King Charles III")),
            # King's Birthday.
            (NOV, 14, tr("King's Birthday")),
        ),
    }

    special_public_holidays_observed = {
        2013: (SEP, 13, tr("Toothfish (end of season) Day")),
        2019: (APR, 23, tr("The Queen's Birthday")),
        2021: (OCT, 29, tr("Environment Day")),
        2023: (MAY, 19, tr("Shackleton Day")),
        2025: (JUN, 20, tr("Mid-winter Day")),
    }
