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

from holidays.calendars.gregorian import SEP
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    SAT_SUN_TO_NEXT_MON,
    SAT_SUN_TO_NEXT_MON_TUE,
    SUN_TO_NEXT_MON,
)


class NorfolkIsland(ObservedHolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays):
    """Norfolk Island holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Norfolk_Island>
        * <https://web.archive.org/web/20250409233237/https://www.infrastructure.gov.au/territories-regions-cities/territories/norfolk-island/travel-information>
        * [2020](https://web.archive.org/web/20220704121150/http://norfolkislander.com/images/2019_08_30_Gazette_No._38.pdf)
        * [2021](https://web.archive.org/web/20250710234146/http://www.norfolkislander.com/images/2020_07_31_Gazette_No._35.pdf)
        * [2022](https://web.archive.org/web/20250328071352/https://www.nirc.gov.au/files/assets/public/v/1/your-council/documents/nirc-gazettes/2021_07_09_gazette_no_29.pdf)
        * [2023](https://web.archive.org/web/20250328071155/https://www.nirc.gov.au/files/assets/public/v/1/your-council/documents/nirc-gazettes/2022_07_21_gazette_no_29.pdf)
        * [2024](https://web.archive.org/web/20250328070948/https://www.nirc.gov.au/files/assets/public/v/1/your-council/documents/nirc-gazettes/2023_07_13_gazette_no_40.pdf)
        * [2025](https://web.archive.org/web/20250711000525/https://www.nirc.gov.au/files/assets/public/v/1/your-council/documents/nirc-gazettes/2024_07_05_gazette_no_25.pdf)
        * [2026](https://web.archive.org/web/20250713192750/https://www.nirc.gov.au/files/assets/public/v/1/your-council/documents/nirc-gazettes/2025/2025-07-11-gazette-no-26.pdf)
    """

    country = "NF"
    default_language = "en_NF"
    # %s (observed).
    observed_label = tr("%s (observed)")
    supported_languages = ("en_NF", "en_US")
    start_year = 2016

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, NorfolkIslandStaticHolidays)
        kwargs.setdefault("observed_rule", SAT_SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_observed(self._add_new_years_day(tr("New Year's Day")))

        # Australia Day.
        self._add_observed(self._add_holiday_jan_26(tr("Australia Day")))

        # Foundation Day.
        self._add_observed(self._add_holiday_mar_6(tr("Foundation Day")), rule=SUN_TO_NEXT_MON)

        # Good Friday.
        self._add_good_friday(tr("Good Friday"))

        # Easter Monday.
        self._add_easter_monday(tr("Easter Monday"))

        # ANZAC Day.
        self._add_anzac_day(tr("ANZAC Day"))

        # Bounty Day.
        self._add_observed(bounty_day := self._add_holiday_jun_8(tr("Bounty Day")))

        # Sovereign's Birthday.
        name = (
            # King's Birthday.
            tr("King's Birthday")
            if self._year >= 2023
            # Queen's Birthday.
            else tr("Queen's Birthday")
        )
        # If Sovereign's Birthday falls on the same day as Bounty Day (observed),
        # it is moved to the next Monday.
        if self._is_saturday(bounty_day):
            self._add_holiday_2_days_past_3rd_sat_of_jun(name)
        else:
            self._add_holiday_2_days_past_2nd_sat_of_jun(name)

        # Show Day.
        self._add_holiday_2nd_mon_of_oct(tr("Show Day"))

        # Thanksgiving Day.
        self._add_holiday_last_wed_of_nov(tr("Thanksgiving Day"))

        self._add_observed(
            # Christmas Day.
            self._add_christmas_day(tr("Christmas Day")),
            rule=SAT_SUN_TO_NEXT_MON_TUE,
        )

        self._add_observed(
            # Boxing Day.
            self._add_christmas_day_two(tr("Boxing Day")),
            rule=SAT_SUN_TO_NEXT_MON_TUE,
        )


class NF(NorfolkIsland):
    pass


class NFK(NorfolkIsland):
    pass


class NorfolkIslandStaticHolidays:
    """Norfolk Island special holidays.

    References:
        * [National Day of Mourning 2022](https://web.archive.org/web/20250711012623/https://www.infrastructure.gov.au/territories-regions-cities/territories/norfolk-island/media-releases/national-day-of-mourning-for-her-majesty-the-queen)
    """

    special_public_holidays = {
        # National Day of Mourning for Queen Elizabeth II.
        2022: (SEP, 22, tr("National Day of Mourning for Queen Elizabeth II")),
    }
