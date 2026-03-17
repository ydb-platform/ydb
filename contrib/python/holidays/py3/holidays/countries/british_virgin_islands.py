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

from holidays.calendars.gregorian import MAY, JUN, OCT, DEC
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    SAT_TO_PREV_FRI,
    SUN_TO_NEXT_MON,
    SAT_SUN_TO_NEXT_MON,
    SAT_SUN_TO_NEXT_MON_TUE,
)


class BritishVirginIslands(
    ObservedHolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays
):
    """British Virgin Islands holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_the_British_Virgin_Islands>
        * [Public Holidays (Amendment) Act, 2021](https://web.archive.org/web/20250522154946/https://laws.gov.vg/Laws/public-holidays-amendment-act-2021)
        * <https://web.archive.org/web/20250401075439/https://www.timeanddate.com/holidays/british-virgin-islands/>
        * [2015](https://web.archive.org/web/20250317055722/https://www.bvi.gov.vg/media-centre/2015-public-holidays)
        * [2016](https://web.archive.org/web/20250323113905/https://www.bvi.gov.vg/media-centre/2016-public-holidays)
        * [2017](https://web.archive.org/web/20250515155409/https://bvi.gov.vg/media-centre/2017-public-holidays)
        * [2018](https://web.archive.org/web/20250318095807/https://www.bvi.gov.vg/media-centre/2018-public-holidays)
        * [2019](https://web.archive.org/web/20250315162108/https://www.bvi.gov.vg/media-centre/2019-public-holidays)
        * [2020](https://web.archive.org/web/20250111172241/https://www.bvi.gov.vg/media-centre/2020-public-holidays)
        * [2021](https://web.archive.org/web/20250417222246/http://www.bvi.gov.vg/media-centre/2021-public-holidays-revised)
        * [2022](https://web.archive.org/web/20250420005523/https://www.bvi.gov.vg/media-centre/revised-2022-public-holidays)
        * [2023](https://web.archive.org/web/20250111170511/https://bvi.gov.vg/media-centre/updatedofficialholidays20231)
        * [2024](https://web.archive.org/web/20250502194703/https://www.bvi.gov.vg/media-centre/cabinet-approves-2024-public-holidays)
        * [2025](https://web.archive.org/web/20250124122752/https://bvi.gov.vg/media-centre/cabinet-approves-2025-holidays)
    """

    country = "VG"
    default_language = "en_VG"
    # %s (observed).
    observed_label = tr("%s (observed)")
    supported_languages = ("en_US", "en_VG")
    start_year = 1967

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, BritishVirginIslandsStaticHolidays)
        kwargs.setdefault("observed_rule", SAT_SUN_TO_NEXT_MON)
        kwargs.setdefault("observed_since", 2000)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_observed(self._add_new_years_day(tr("New Year's Day")))

        self._add_holiday_1st_mon_before_mar_7(
            # Lavity Stoutt's Birthday.
            tr("The Anniversary of the Birth of Hamilton Lavity Stoutt")
        )

        if self._year <= 2020:
            # Commonwealth Day.
            self._add_holiday_2nd_mon_of_mar(tr("Commonwealth Day"))

        # Good Friday.
        self._add_good_friday(tr("Good Friday"))

        # Easter Monday.
        self._add_easter_monday(tr("Easter Monday"))

        # Whit Monday.
        self._add_whit_monday(tr("Whit Monday"))

        # Sovereign's Birthday.
        name = tr("Sovereign's Birthday")
        if self._year >= 2019:
            sovereign_birthday_dates = {
                2019: (JUN, 7),
                2023: (JUN, 16),
            }
            if dt := sovereign_birthday_dates.get(self._year):
                self._add_holiday(name, dt)
            else:
                self._add_holiday_2nd_fri_of_jun(name)
        else:
            self._add_holiday_2nd_sat_of_jun(name)

        name = (
            # Virgin Islands Day.
            tr("Virgin Islands Day")
            if self._year >= 2021
            # Territory Day.
            else tr("Territory Day")
            if self._year >= 1978
            # Colony Day.
            else tr("Colony Day")
        )
        territory_day_dates = {
            2015: (JUN, 29),
            2020: (JUN, 29),
        }
        if dt := territory_day_dates.get(self._year):
            self._add_holiday(name, dt)
        else:
            if self._year >= 2021:
                self._add_holiday_1st_mon_of_jul(name)
            else:
                self._add_observed(
                    self._add_holiday_jul_1(name), rule=SAT_TO_PREV_FRI + SUN_TO_NEXT_MON
                )

        self._add_holiday_1st_mon_of_aug(
            # Emancipation Monday.
            tr("Emancipation Monday")
            if self._year >= 2021
            # Festival Monday.
            else tr("Festival Monday")
        )

        self._add_holiday_1_day_past_1st_mon_of_aug(
            # Emancipation Tuesday.
            tr("Emancipation Tuesday")
            if self._year >= 2021
            # Festival Tuesday.
            else tr("Festival Tuesday")
        )

        self._add_holiday_2_days_past_1st_mon_of_aug(
            # Emancipation Wednesday.
            tr("Emancipation Wednesday")
            if self._year >= 2021
            # Festival Wednesday.
            else tr("Festival Wednesday")
        )

        if self._year <= 2020:
            # Saint Ursula's Day.
            name = tr("Saint Ursula's Day")
            saint_ursula_dates = {
                2015: (OCT, 19),
                2020: (OCT, 23),
            }
            if dt := saint_ursula_dates.get(self._year):
                self._add_holiday(name, dt)
            else:
                self._add_observed(
                    self._add_holiday_oct_21(name), rule=SAT_TO_PREV_FRI + SUN_TO_NEXT_MON
                )

        if self._year >= 2021:
            # Heroes and Foreparents Day.
            self._add_holiday_3rd_mon_of_oct(tr("Heroes and Foreparents Day"))

            # The Great March of 1949 and Restoration Day.
            self._add_holiday_4th_mon_of_nov(tr("The Great March of 1949 and Restoration Day"))

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


class VG(BritishVirginIslands):
    pass


class VGB(BritishVirginIslands):
    pass


class BritishVirginIslandsStaticHolidays:
    """British Virgin Islands special holidays.

    References:
        * [2019](https://web.archive.org/web/20250417134346/https://www.bvi.gov.vg/media-centre/public-holiday-be-observed-funeral-late-honourable-ralph-t-o-neal-obe)
        * [2022](https://web.archive.org/web/20250315174707/https://www.bvi.gov.vg/media-centre/public-holiday-observe-her-majesty-s-platinum-jubilee)
        * [2023](https://web.archive.org/web/20250505073654/https://bvi.gov.vg/media-centre/may-8-and-june-16-declared-public-holidays)
    """

    special_public_holidays = {
        # State Funeral of Honourable Ralph T. O'Neal.
        2019: (DEC, 11, tr("State Funeral of Honourable Ralph T. O'Neal")),
        # Platinum Jubilee of Elizabeth II.
        2022: (JUN, 3, tr("Queen Elizabeth II's Platinum Jubilee")),
        # Coronation of Charles III.
        2023: (MAY, 8, tr("The King's Coronation")),
    }
