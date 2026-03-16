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

from holidays.calendars.gregorian import APR, MAY, JUN, JUL, SEP, DEC
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    MON_TO_NEXT_TUE,
    SAT_TO_NEXT_MON,
    SAT_SUN_TO_NEXT_MON,
    SAT_SUN_TO_NEXT_MON_TUE,
)


class Montserrat(ObservedHolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays):
    """Montserrat holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Montserrat>
        * [Public holidays Act, 2017](https://web.archive.org/web/20240619074948/http://agc.gov.ms/wp-content/uploads/2011/10/Act-No.-3-of-2017-Public-Holidays-Amendment-Act-20171.pdf)
        * [Public holidays Act, 2019](https://web.archive.org/web/20241025070627/https://www.gov.ms/wp-content/uploads/2020/06/Public-Holidays-Act.pdf)
        * [National Day of Prayer and Thanksgiving, 2021](https://web.archive.org/web/20220809144840/https://www.gov.ms/2021/07/16/wednesday-july-21-2021-public-holiday-national-day-of-prayer-thanksgiving/)
        * [National Day of Prayer and Thanksgiving, 2022](https://web.archive.org/web/20220816124619/https://www.gov.ms/2022/07/19/public-holiday-for-national-day-of-prayer-and-thanksgiving-to-be-held-on-july-20-2022/)
        * [National Day of Prayer and Thanksgiving](https://web.archive.org/web/20240711221313/https://www.gov.ms/wp-content/uploads/2023/05/SRO-No.-15-of-2023-Public-Holidays-Amendment-of-Schedule-Order.pdf)
        * [Queen's Platinum Jubilee, 2022](https://web.archive.org/web/20250711160439/https://www.gov.ms/tag/public-holidays/)
        * [Montserrat Public holidays, 2022](https://web.archive.org/web/20220809030551/https://www.gov.ms/wp-content/uploads/2021/12/Public-Holidays-2022-1.jpeg)
        * [Montserrat Public holidays, 2023](https://web.archive.org/web/20241126232715/https://www.gov.ms/wp-content/uploads/2023/02/Public-Holidays-Montserrat-2023_page-0001.jpg)
        * [Montserrat Public holidays, 2024](https://web.archive.org/web/20240421112540/https://www.gov.ms/wp-content/uploads/2023/12/Public-Holidays-Montserrat-2024.docx.pdf)
        * [King's Birthday, 2024](https://web.archive.org/web/20250711202228/https://parliament.ms/wp-content/uploads/2024/09/SRO-No.-18-of-2024-PROCOLAMATION-APPOINTING-MONDAY-17-JUNE-2024-AS-A-PUBLIC-HOLIDAY.pdf)
        * [Montserrat Public holidays, 2025](https://web.archive.org/web/20250711160324/https://www.gov.ms/wp-content/uploads/2025/01/2025-Public-Holidays-on-Montserrat.pdf)
    """

    country = "MS"
    default_language = "en_MS"
    # %s (observed).
    observed_label = "%s (observed)"
    # Public holidays Act, 2017.
    start_year = 2017
    supported_languages = ("en_MS", "en_US")

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, MontserratStaticHolidays)
        kwargs.setdefault("observed_rule", SAT_SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        self._add_observed(
            # New Year's Day.
            self._add_new_years_day(tr("New Year's Day")),
            rule=SAT_SUN_TO_NEXT_MON_TUE + MON_TO_NEXT_TUE,
        )

        # Saint Patrick's Day.
        self._add_observed(self._add_saint_patricks_day(tr("Saint Patrick's Day")))

        # Good Friday.
        self._add_good_friday(tr("Good Friday"))

        # Easter Monday.
        self._add_easter_monday(tr("Easter Monday"))

        # Labor Day.
        self._add_holiday_1st_mon_of_may(tr("Labour Day"))

        name = (
            # King's Birthday.
            tr("King's Birthday")
            if self._year >= 2023
            # Queen's Birthday.
            else tr("Queen's Birthday")
        )
        sovereign_birthday_dts = {
            2022: (JUN, 2),
            2023: (JUN, 19),
            2024: (JUN, 17),
        }
        sovereign_birthday_dt = (
            self._add_holiday(name, dt)
            if (dt := sovereign_birthday_dts.get(self._year))
            else self._add_holiday_2_days_past_2nd_sat_of_jun(name)
        )

        # Whit Monday.
        name = tr("Whit Monday")
        whit_monday_dt = self._add_whit_monday(name)
        if whit_monday_dt == sovereign_birthday_dt:
            self._add_observed(whit_monday_dt, name=name, rule=MON_TO_NEXT_TUE)

        if self._year >= 2021:
            day_of_prayer_dts = {
                2021: (JUL, 21),
                2022: (JUL, 20),
            }
            name = (
                # National Day of Prayer and Thanksgiving.
                tr("National Day of Prayer and Thanksgiving")
                if self._year <= 2023
                # Day of Prayer and Thanksgiving.
                else tr("Day of Prayer and Thanksgiving")
            )
            if dt := day_of_prayer_dts.get(self._year):
                self._add_holiday(name, dt)
            else:
                self._add_holiday_2nd_wed_of_jul(name)

        # Emancipation Day.
        self._add_holiday_1st_mon_of_aug(tr("Emancipation Day"))

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

        # Festival Day.
        name = tr("Festival Day")
        self._add_new_years_eve(name)
        self._add_observed((self._year - 1, DEC, 31), name, rule=SAT_TO_NEXT_MON)


class MS(Montserrat):
    pass


class MSR(Montserrat):
    pass


class MontserratStaticHolidays(StaticHolidays):
    """Montserrat special holidays.

    References:
        * [September 14th, 2018](https://web.archive.org/web/20220810224300/https://www.gov.ms/wp-content/uploads/2020/11/SRO.-No.-35-of-2018-Proclamation-Declaring-Friday-14-September-2018-as-a-public-holiday.pdf)
        * [May 10th, 2019](https://web.archive.org/web/20240626113529/https://parliament.ms/wp-content/uploads/2022/02/SRO-No-13-of-2019-Proclamation-Declaring-Friday-10-May-2019-as-a-publi._.pdf)
        * [July 15th, 2020](https://web.archive.org/web/20220810225456/https://www.gov.ms/wp-content/uploads/2020/08/SRO.-No.-40-of-2020-Proclamation-Declaring-Wednesday-15-July-2020-as-a-Public-Holiday.pdf)
        * [National Day of Mourning](https://web.archive.org/web/20240617072858/https://www.parliament.ms/wp-content/uploads/2022/09/SRO-No.43-of-2022-Proclamation-Appointing-Monday-19-September-2022-a-Public-Holiday.pdf)
        * [Coronation of King Charles III](https://web.archive.org/web/20241126232715/https://www.gov.ms/wp-content/uploads/2023/02/Public-Holidays-Montserrat-2023_page-0001.jpg)
    """

    # Special Public Holiday.
    name = tr("Special Public Holiday")

    special_public_holidays = {
        2018: (SEP, 14, name),
        2019: (MAY, 10, name),
        2020: (JUL, 15, name),
        2022: (
            # Platinum Jubilee of Elizabeth II.
            (JUN, 3, tr("Platinum Jubilee of Elizabeth II")),
            # National Day of Mourning.
            (SEP, 19, tr("National Day of Mourning")),
        ),
        2023: (
            (APR, 12, name),
            # Coronation of King Charles III.
            (MAY, 6, tr("Coronation of King Charles III")),
        ),
    }
