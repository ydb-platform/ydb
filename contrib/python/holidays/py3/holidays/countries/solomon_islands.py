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

from holidays.calendars.gregorian import APR, JUN, SEP, NOV
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    SAT_TO_PREV_FRI,
    SUN_TO_NEXT_MON,
    SAT_SUN_TO_NEXT_MON_TUE,
)


class SolomonIslands(
    ObservedHolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays
):
    """Solomon Islands holidays.

    References:
        * [Public Holidays Act](https://web.archive.org/web/20250615054807/https://www.paclii.org/sb/legis/consol_act_1996/pha163.pdf)
        * [2016](https://web.archive.org/web/20250530230724/https://mehrd.gov.sb/documents?view=download&format=raw&fileId=4377)
        * [2020](https://web.archive.org/web/20240908025223/https://solomons.gov.sb/national-and-provincial-public-holidays-2020/)
        * [2021](https://web.archive.org/web/20241004035112/https://solomons.gov.sb/wp-content/uploads/2021/03/Gaz-No.-54-Sup-No.-50-Wednesday-10th-March-2021.pdf)
        * [2022](https://web.archive.org/web/20240727185307/https://solomons.gov.sb/wp-content/uploads/2021/10/Gaz-No.-246-Tuesday-19th-October-2021-.pdf)
        * [2023](https://web.archive.org/web/20240810050725/https://solomons.gov.sb/wp-content/uploads/2022/12/Gaz-No.-324-Wednesday-21st-December-2022-1.pdf)
        * [2024](https://web.archive.org/web/20240727124555/https://solomons.gov.sb/wp-content/uploads/2024/02/Gaz-No.-15-Monday-12th-FEBRUARY-2024.pdf)
        * [2025](https://web.archive.org/web/20250615054517/https://solomons.gov.sb/wp-content/uploads/2024/11/Gaz-No.-178-Monday-25th-November-2024.pdf)

    About the day appointed for the celebration of the Anniversary of the Birthday of the
    Sovereign:

    * Up to 2022, the Queen's Birthday was typically observed on the second Saturday of
        June, with the preceding Friday designated as a public holiday. From 2023 onward,
        the King's Birthday has been marked on the Friday before the third Saturday in
        June. Although there has been no amendment to the Public Holidays Act to explicitly
        state this change, the pattern is evident in the annual government gazettes and
        supported by information from the relevant [Wikipedia
        entry](https://en.wikipedia.org/wiki/King's_Official_Birthday#Solomon_Islands).
    * There are a few exceptions to this rule:
        * [Queen's Birthday 2022](https://web.archive.org/web/20240727184251/https://solomons.gov.sb/wp-content/uploads/2022/05/Gaz-No.-171-Friday-27th-May-2022.pdf)
        * According to the holidays schedule for 2024, King's Birthday was celebrated on
          the 3rd saturday of June (June 15) and the preceding Friday was observed as a
          public holiday.
        * [King's Birthday 2025](https://web.archive.org/web/20250615054519/https://solomons.gov.sb/wp-content/uploads/2025/04/Gaz-No.-33-Wednesday-9th-April-2025.pdf)

    Province Days are not listed in the Public Holidays Act but are consistently announced
    in the official gazette each year.

    While the Public Holidays Act specifies the Sunday to next Monday observance rule,
    gazettes dating back to 2016 also follow the Saturday to previous Friday rule for
    shifting public holidays.
    """

    country = "SB"
    # %s (observed).
    observed_label = "%s (observed)"
    start_year = 1979
    subdivisions = (
        "CE",  # Central.
        "CH",  # Choiseul.
        "CT",  # Capital Territory (Honiara).
        "GU",  # Guadalcanal.
        "IS",  # Isabel.
        "MK",  # Makira-Ulawa.
        "ML",  # Malaita.
        "RB",  # Rennell and Bellona.
        "TE",  # Temotu.
        "WE",  # Western.
    )
    subdivisions_aliases = {
        "Central": "CE",
        "Choiseul": "CH",
        "Capital Territory": "CT",
        "Honiara": "CT",
        "Guadalcanal": "GU",
        "Isabel": "IS",
        "Makira-Ulawa": "MK",
        "Malaita": "ML",
        "Rennell and Bellona": "RB",
        "Temotu": "TE",
        "Western": "WE",
    }

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, cls=SolomonIslandsStaticHolidays)
        kwargs.setdefault("observed_rule", SUN_TO_NEXT_MON + SAT_TO_PREV_FRI)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        name = "New Year's Day"
        self._add_observed(self._add_new_years_day(name))
        self._add_observed(self._next_year_new_years_day, name=name, rule=SAT_TO_PREV_FRI)

        # Good Friday.
        self._add_good_friday("Good Friday")

        # Holy Saturday.
        self._add_holy_saturday("Holy Saturday")

        # Easter Monday.
        self._add_easter_monday("Easter Monday")

        # Whit Monday.
        self._add_whit_monday("Whit Monday")

        name = (
            # King's Birthday.
            "King's Birthday"
            if self._year >= 2023
            # Queen's Birthday.
            else "Queen's Birthday"
        )
        sovereign_birthday_dates = {
            2022: (JUN, 3),
            2025: (JUN, 13),
        }
        if dt := sovereign_birthday_dates.get(self._year):
            self._add_holiday(name, dt)
        elif self._year == 2024:
            self._add_observed(self._add_holiday_3rd_sat_of_jun(name))
        elif self._year >= 2023:
            self._add_holiday_1_day_prior_3rd_sat_of_jun(name)
        else:
            self._add_observed(self._add_holiday_2nd_sat_of_jun(name))

        # Independence Day.
        self._add_observed(self._add_holiday_jul_7("Independence Day"))

        self._add_observed(
            # Christmas Day.
            self._add_christmas_day("Christmas Day"),
            rule=SAT_SUN_TO_NEXT_MON_TUE,
        )

        self._add_observed(
            # National Day of Thanksgiving.
            self._add_christmas_day_two("National Day of Thanksgiving"),
            rule=SAT_SUN_TO_NEXT_MON_TUE,
        )

    def _populate_subdiv_ce_public_holidays(self):
        # Central Province Day.
        self._add_observed(self._add_holiday_jun_29("Central Province Day"))

    def _populate_subdiv_ch_public_holidays(self):
        # Choiseul Province Day.
        self._add_observed(self._add_holiday_feb_25("Choiseul Province Day"))

    def _populate_subdiv_gu_public_holidays(self):
        # Guadalcanal Province Day.
        self._add_observed(self._add_holiday_aug_1("Guadalcanal Province Day"))

    def _populate_subdiv_is_public_holidays(self):
        # Isabel Province Day.
        self._add_observed(self._add_holiday_jun_2("Isabel Province Day"))

    def _populate_subdiv_mk_public_holidays(self):
        # Makira-Ulawa Province Day.
        self._add_observed(self._add_holiday_aug_3("Makira-Ulawa Province Day"))

    def _populate_subdiv_ml_public_holidays(self):
        # Malaita Province Day.
        self._add_observed(self._add_holiday_aug_15("Malaita Province Day"))

    def _populate_subdiv_rb_public_holidays(self):
        # Rennell and Bellona Province Day.
        self._add_observed(self._add_holiday_jul_20("Rennell and Bellona Province Day"))

    def _populate_subdiv_te_public_holidays(self):
        # Temotu Province Day.
        self._add_observed(self._add_holiday_jun_8("Temotu Province Day"))

    def _populate_subdiv_we_public_holidays(self):
        # Western Province Day.
        self._add_observed(self._add_holiday_dec_7("Western Province Day"))


class SB(SolomonIslands):
    pass


class SLB(SolomonIslands):
    pass


class SolomonIslandsStaticHolidays:
    """Solomon Islands special holidays.

    References:
        * [2024 General Election Holiday](https://web.archive.org/web/20240711032116/https://solomonchamber.com.sb/media/2618/gaz-no-61-monday-8th-april-2024.pdf)
        * [2020 By-election Holiday](https://web.archive.org/web/20241011021616/https://solomons.gov.sb/wp-content/uploads/2020/11/GAZ-173-11th-November-2020-1.pdf)
        * [Gazette No. 269 of 2022](https://web.archive.org/web/20240727183925/https://solomons.gov.sb/wp-content/uploads/2022/09/Gaz-No.-269-Friday-9th-September-2022.pdf)
    """

    # Public Holiday.
    name = "Public Holiday"
    special_public_holidays = {
        2022: (SEP, 12, name),
        2024: (APR, 17, name),
    }

    special_ch_public_holidays = {
        2020: (NOV, 18, name),
    }

    special_gu_public_holidays = {
        2020: (NOV, 18, name),
    }
