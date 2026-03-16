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

from holidays.calendars import _CustomHinduHolidays, _CustomIslamicHolidays
from holidays.calendars.gregorian import SEP, OCT, NOV, DEC
from holidays.constants import PUBLIC, WORKDAY
from holidays.groups import (
    ChristianHolidays,
    HinduCalendarHolidays,
    InternationalHolidays,
    IslamicHolidays,
)
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    SAT_SUN_TO_NEXT_MON,
    SAT_SUN_TO_NEXT_MON_TUE,
    ALL_TO_NEAREST_MON,
)


class Fiji(
    ObservedHolidayBase,
    ChristianHolidays,
    HinduCalendarHolidays,
    InternationalHolidays,
    IslamicHolidays,
):
    """Fiji holidays.

    References:
        * <https://web.archive.org/web/20250421221329/https://www.laws.gov.fj/Acts/DisplayAct/2910>
        * <https://web.archive.org/web/20250421155009/https://laws.gov.fj/LawsAsMade>
        * <https://web.archive.org/web/20250318092311/https://www.fiji.gov.fj/About-Fiji/Public-Holidays>
        * <https://web.archive.org/web/20250413132828/https://www.timeanddate.com/holidays/fiji/>
        * <https://en.wikipedia.org/wiki/List_of_festivals_in_Fiji>
        * <https://web.archive.org/web/20250427183802/https://www.rnz.co.nz/international/pacific-news/249514/new-public-holiday-for-fiji>
        * <https://web.archive.org/web/20250427183723/https://www.fijitimes.com.fj/constitution-day-public-holiday-removed-cabinet/>
        * <https://web.archive.org/web/20150626160414/http://fijivillage.com:80/news/National-Sports-Day-celebrated-5krs29/>
        * <https://web.archive.org/web/20240624074852/https://www.fijivillage.com/news/Cabinet-approves-Ratu-Sir-Lala-Sukuna-Day-and-Girmit-Day-and-removes-Constitution-Day-as-a-public-holiday-f48r5x/>

    Official Fiji Public Holidays Calendar:
         * [2016](https://web.archive.org/web/20160520212352/http://www.fiji.gov.fj:80/Media-Center/Press-Releases/GOVERNMENT-APPROVES-2016-PUBLIC-HOLIDAYS.aspx?)
         * [2017](https://web.archive.org/web/20250319202817/https://www.fiji.gov.fj/Media-Centre/News/GOVERNMENT-APPROVES-2017-PUBLIC-HOLIDAYS)
         * [2018](https://web.archive.org/web/20180727205733/http://www.employment.gov.fj/images/Laws/Press%20Release%20-%20Government%20Approves%202018%20Public%20Holidays.pdf)
         * [2019](https://web.archive.org/web/20191018023027/https://www.fiji.gov.fj/About-Fiji/Public-Holidays)
         * [2020](https://web.archive.org/web/20210103183942/https://www.fiji.gov.fj/About-Fiji/Public-Holidays)
         * [2021-2022](https://web.archive.org/web/20221223004409/https://www.fiji.gov.fj/About-Fiji/Public-Holidays)
         * [2023](https://web.archive.org/web/20231129154609/https://www.fiji.gov.fj/About-Fiji/Public-Holidays)
         * [2024](https://web.archive.org/web/20250121185434/https://www.fiji.gov.fj/About-Fiji/Public-Holidays)
         * [2025](https://web.archive.org/web/20250318092311/https://www.fiji.gov.fj/About-Fiji/Public-Holidays)
    """

    country = "FJ"
    supported_categories = (PUBLIC, WORKDAY)
    # %s (estimated).
    estimated_label = "%s (estimated)"
    # %s (observed).
    observed_label = "%s (observed)"
    # %s (observed, estimated).
    observed_estimated_label = "%s (observed, estimated)"
    # Act No. 13 of 2015
    start_year = 2016

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.
        """
        ChristianHolidays.__init__(self)
        HinduCalendarHolidays.__init__(self, cls=FijiHinduHolidays, show_estimated=True)
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(
            self, cls=FijiIslamicHolidays, show_estimated=islamic_show_estimated
        )
        kwargs.setdefault("observed_rule", SAT_SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_observed(self._add_new_years_day("New Year's Day"))

        # Good Friday.
        self._add_good_friday("Good Friday")

        # Easter Saturday.
        self._add_holy_saturday("Easter Saturday")

        # Easter Monday.
        self._add_easter_monday("Easter Monday")

        # National Sports Day.
        if self._year <= 2018:
            self._add_holiday_last_fri_of_jun("National Sports Day")

        if self._year <= 2022:
            # Constitution Day.
            self._add_observed(self._add_holiday_sep_7("Constitution Day"))

        if self._year >= 2023:
            self._move_holiday_forced(
                # Girmit Day.
                self._add_holiday_may_14("Girmit Day"),
                rule=ALL_TO_NEAREST_MON,
            )

        # Ratu Sir Lala Sukuna Day.
        name = "Ratu Sir Lala Sukuna Day"
        if self._year == 2023:
            self._add_holiday_last_mon_of_may(name)
        elif self._year >= 2024:
            self._add_holiday_last_fri_of_may(name)

        # Fiji Day.
        self._add_holiday_oct_10("Fiji Day")

        # Diwali.
        self._add_observed(self._add_diwali_india("Diwali"))

        # Christmas Day.
        self._add_observed(self._add_christmas_day("Christmas Day"), rule=SAT_SUN_TO_NEXT_MON_TUE)

        # Boxing Day.
        self._add_observed(self._add_christmas_day_two("Boxing Day"), rule=SAT_SUN_TO_NEXT_MON_TUE)

        # Prophet Mohammed's Birthday.
        for dt in self._add_mawlid_day("Prophet Mohammed's Birthday"):
            self._add_observed(dt)

    def _populate_workday_holidays(self):
        if self._year >= 2023:
            # Constitution Day.
            self._add_holiday_sep_7("Constitution Day")


class FJ(Fiji):
    pass


class FJI(Fiji):
    pass


class FijiHinduHolidays(_CustomHinduHolidays):
    # https://web.archive.org/web/20250413132828/https://web.archive.org/web/20240724121605/https://www.timeanddate.com/holidays/fiji/diwali
    DIWALI_INDIA_DATES = {
        2016: (OCT, 31),
        2017: (OCT, 19),
        2018: (NOV, 7),
        2019: (OCT, 28),
        2020: (NOV, 14),
        2021: (NOV, 4),
        2022: (OCT, 25),
        2023: (NOV, 13),
        2024: (NOV, 1),
        2025: (OCT, 21),
    }


class FijiIslamicHolidays(_CustomIslamicHolidays):
    MAWLID_DATES_CONFIRMED_YEARS = (2016, 2025)
    MAWLID_DATES = {
        2016: (DEC, 12),
        2017: (DEC, 2),
        2018: (NOV, 19),
        2020: (OCT, 31),
        2022: (OCT, 7),  # looks like observed on FRI
        2023: (SEP, 30),
        2024: (SEP, 16),
        2025: (SEP, 6),
    }
