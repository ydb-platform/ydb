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

from holidays.calendars.gregorian import FEB, MAR, APR, JUL, AUG, SEP, DEC, SUN
from holidays.constants import HALF_DAY, PUBLIC, WORKDAY
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.observed_holiday_base import ObservedHolidayBase, SUN_TO_NEXT_MON, SUN_TO_NEXT_TUE


class SaintKittsAndNevis(
    ObservedHolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays
):
    """Saint Kitts and Nevis holidays.

    References:
        * <https://web.archive.org/web/20240725043444/https://lawcommission.gov.kn/wp-content/documents/Revised-Acts-of-St-Kitts-and-Nevis/Revised-Acts-of-St-Kitts-and-Nevis-2009/Ch-23_23-Public-Holidays-Act.pdf>
        * <https://web.archive.org/web/20220124000224/https://aglcskn.info/wp-content/documents/Act02and09TOC/Ch-23_23-Public-Holidays-Act.pdf>
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Saint_Kitts_and_Nevis>

    Cross-Checked With:
        * <https://web.archive.org/web/20250417181411/https://sknhcottawa.gov.kn/in-skn-national-public-holidays/>
        * <https://web.archive.org/web/20250321015910/https://www.timeanddate.com/holidays/saint-kitts-and-nevis/>

    If Sovereign's Birthday, New Year's Day, Independence Day, or National Heroes Day
    fall on a Sunday the next following Monday shall be a public holiday.

    Boxing Day—that is the day after Christmas Day, but if Christmas Day falls
    on a Saturday, then the next following Monday shall be a public holiday, and if
    Christmas Day falls on a Sunday, then the next following Monday and Tuesday
    shall be public holidays.

    While Culturama Day (first started in 1974) and Carnival Day are never officially
    included in the main Chapter 23.23 document, they're de facto added since at least
    2015 and should be considered as such.
    """

    country = "KN"
    supported_categories = (HALF_DAY, PUBLIC, WORKDAY)
    # %s (observed).
    observed_label = "%s (observed)"
    weekend = {SUN}
    # Public Holidays Act, Act 7 of 1983 Amendment.
    start_year = 1983

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, SaintKittsAndNevisStaticHolidays)
        kwargs.setdefault("observed_rule", SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # Carnival Day.
        self._add_observed(self._add_new_years_day("Carnival Day"), rule=SUN_TO_NEXT_TUE)

        # Carnival Day - Last Lap.
        self._add_observed(self._add_new_years_day_two("Carnival Day - Last Lap"))

        # Good Friday.
        self._add_good_friday("Good Friday")

        # Easter Monday.
        self._add_easter_monday("Easter Monday")

        # Labour Day.
        self._add_holiday_1st_mon_of_may("Labour Day")

        # While Sovereign's Birthday is officially listed in the Public Holidays Act,
        # this was de facto never included in any released calendar since at least 2015.

        # Whit Monday.
        self._add_whit_monday("Whit Monday")

        self._add_holiday_1st_mon_of_aug(
            # Emancipation Day.
            "Emancipation Day"
            if self._year >= 1998
            # First Monday of August.
            else "First Monday of August"
        )

        # Culturama Day - Last Lap.
        self._add_holiday_1_day_past_1st_mon_of_aug("Culturama Day - Last Lap")

        if self._year >= 1998:
            # National Heroes Day.
            self._add_observed(self._add_holiday_sep_16("National Heroes Day"))

        # Independence Day.
        self._add_observed(self._add_holiday_sep_19("Independence Day"))

        # Christmas Day.
        self._add_observed(self._add_christmas_day("Christmas Day"), rule=SUN_TO_NEXT_TUE)

        # Boxing Day.
        self._add_observed(self._add_christmas_day_two("Boxing Day"))

    def _populate_workday_holidays(self):
        # August 25 was declared Kim Collins Day by the government of St. Kitts and Nevis
        # in honour of one of the track star's most significant accomplishments, the gold
        # at the World Championships in Paris, France in 2003.
        if self._year >= 2003:
            # Kim Collins Day.
            self._add_holiday_aug_25("Kim Collins Day")


class KN(SaintKittsAndNevis):
    pass


class KNA(SaintKittsAndNevis):
    pass


class SaintKittsAndNevisStaticHolidays:
    """Saint Kitts and Nevis special holidays.

    References:
        * <https://web.archive.org/web/20250127081445/https://www.sknis.gov.kn/2023/07/17/state-funeral-accorded-to-sir-tapley-national-day-of-mourning-and-half-holiday-declared-for-july-20/>
        * <https://web.archive.org/web/20241010133156/https://www.sknis.gov.kn/2022/12/30/public-holiday-notice-request-from-the-department-of-labour/>
        * <https://web.archive.org/web/20240908004504/https://www.sknis.gov.kn/2022/08/06/prime-minister-drew-declares-monday-august-08-2022-as-a-public-holiday-in-st-kitts-and-nevis/>
        * <https://web.archive.org/web/20250402141902/https://www.sknis.gov.kn/2022/04/20/nia-announces-half-holiday/>
        * <https://web.archive.org/web/20250422075354/https://www.sknis.gov.kn/2021/07/26/governor-general-proclaims-tuesday-3rd-august-2021-as-a-public-holiday/>
        * <https://web.archive.org/web/20241114110536/https://nia.gov.kn/culturama-47-rescheduled-to-independence-holiday-weekend-in-september/>
        * <https://web.archive.org/web/20241012200906/https://www.sknis.gov.kn/2019/12/27/public-holidays-during-carnival-2019-2020/>
        * <https://web.archive.org/web/20250417160028/https://www.sknis.gov.kn/2018/12/29/proclamations-from-his-excellency-the-governor-general-re-carnival-public-holidays/>
        * <https://web.archive.org/web/20240731154425/https://www.sknis.gov.kn/2017/12/19/public-holiday-declared-for-nevis-today-after-the-ccms-solid-win-in-the-local-elections/>
        * <https://web.archive.org/web/20250402140120/https://www.sknis.gov.kn/2017/04/13/employers-must-comply-with-law-for-work-performed-on-national-holidays-says-labour-department/>
        * <https://web.archive.org/web/20250429133207/https://www.facebook.com/sknismedia/posts/pfbid02WRQ6HgzJKuFYnm7BhyoTXqeCYAfGBa1fUsKCKC9ffntQHTMkJUEMDAjxxQ4m22y8l?rdid=ppYG2YsWr0asGnPI>
        * <https://web.archive.org/web/20250429133414/https://www.facebook.com/sknismedia/posts/pfbid02Bgyc9YtJugY2vuPUdGT7crsCE6k4zY2MuMEJZk43nWCypmXuhEoBWvHbfPoWYLfhl?rdid=72IgP06WkImFL5Y0>
        * <https://web.archive.org/web/20250429133044/https://www.facebook.com/sknismedia/posts/pfbid02NLERm2eW3vgaHFQMT6x5jfNYz6RUUHpnGFW5kTv7dLwe1amgf8ba5V1QqgKRwQQrl?rdid=9YawfFWYcZrP91n1>
    """

    # Federal Election Victory Day.
    federal_election_victory_day_name = "Federal Election Victory Day"

    # Children's Carnival Day.
    childrens_carnival_day_name = "Children's Carnival Day"

    special_public_holidays = {
        2015: (FEB, 18, federal_election_victory_day_name),
        2017: (
            # National Clean Up Day.
            (SEP, 20, "National Clean Up Day"),
            # Local Election Victory Day.
            (DEC, 19, "Local Election Victory Day"),
        ),
        2022: (AUG, 8, federal_election_victory_day_name),
        # 50th Anniversary of the Establishment of the Caribbean Community (CARICOM).
        2023: (
            JUL,
            4,
            "50th Anniversary of the Establishment of the Caribbean Community (CARICOM)",
        ),
    }
    special_half_day_holidays = {
        2017: (
            # The Passing of His Excellency Sir Probyn Inniss.
            (MAR, 23, "The Passing of His Excellency Sir Probyn Inniss"),
            # The Passing of His Excellency Sir Cuthbert Sebastian.
            (APR, 10, "The Passing of His Excellency Sir Cuthbert Sebastian"),
        ),
        2018: (DEC, 31, childrens_carnival_day_name),
        2019: (DEC, 31, childrens_carnival_day_name),
        # 2022 Gulf Insurance Inter-Primary Schools Championship.
        2022: (APR, 27, "2022 Gulf Insurance Inter-Primary Schools Championship"),
        2023: (
            # The Passing of His Excellency Sir Tapley Seaton.
            (JUL, 20, "The Passing of His Excellency Sir Tapley Seaton"),
            (DEC, 30, childrens_carnival_day_name),
        ),
        # Junior Cultural Street Parade.
        2024: (AUG, 1, "Junior Cultural Street Parade"),
    }
