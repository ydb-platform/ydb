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

from holidays.calendars.gregorian import JAN, FEB, APR, MAY, JUN, SEP
from holidays.constants import GOVERNMENT, PUBLIC
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    SAT_TO_PREV_FRI,
    SUN_TO_NEXT_MON,
    SAT_SUN_TO_NEXT_MON,
    SAT_SUN_TO_NEXT_MON_TUE,
)


class SaintHelenaAscensionAndTristanDaCunha(
    ObservedHolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays
):
    """Saint Helena, Ascension and Tristan da Cunha holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Saint_Helena,_Ascension_and_Tristan_da_Cunha>

    Saint Helena:
        * <https://web.archive.org/web/20250810194916/https://sainthelenaisland.info/holidays.htm>
        * [Public Holidays Ordinance](https://web.archive.org/web/20231129081100/https://www.sainthelena.gov.sh/wp-content/uploads/2013/01/Public-Holidays-Ordinance.pdf)
        * [2015](https://web.archive.org/web/20241219152350/https://www.sainthelena.gov.sh/wp-content/uploads/2012/08/Public-Holidays-2015.pdf)
        * [2020](https://web.archive.org/web/20250831203105/https://www.sainthelena.gov.sh/wp-content/uploads/2019/11/Public-Holidays-2020.pdf)
        * [2021](https://web.archive.org/web/20240626101426/https://www.sainthelena.gov.sh/wp-content/uploads/2020/08/200824_Public-Holidays-2021.pdf)
        * [2022](https://web.archive.org/web/20230922215102/https://www.sainthelena.gov.sh/wp-content/uploads/2021/11/PUBLIC-HOLIDAYS-2022.pdf)
        * [Saint Helena Day 2023](https://web.archive.org/web/20250519085755/https://www.sainthelena.gov.sh/wp-content/uploads/2022/09/EX-GAZ-100-Proclamation-St-Helena-Day-2023.pdf)
        * [2024](https://web.archive.org/web/20250614201000/https://www.sainthelena.gov.sh/2023/news/public-and-government-holidays-2024/)
        * [2025](https://web.archive.org/web/20241005191212/https://www.sainthelena.gov.sh/wp-content/uploads/2024/08/Public-Notice-Public-Holidays-2025.pdf)
        * [2026](https://web.archive.org/web/20250831200128/https://www.sainthelena.gov.sh/wp-content/uploads/2025/08/Public-Notice-Public-Holidays-2026.pdf)

    Ascension:
        * [Public Holidays Ordinance](https://web.archive.org/web/20240610184146/https://www.sainthelena.gov.sh/wp-content/uploads/2017/12/Public-Holidays-Asc-Ordinance.pdf)

    Tristan da Cunha:
        * [Ratting Day](https://web.archive.org/web/20250826151056/https://www.tristandc.com/newsratting.php)
    """

    country = "SH"
    default_language = "en_GB"
    # %s (observed).
    observed_label = tr("%s (observed)")
    # Earliest year of holidays with an accessible online record.
    start_year = 2015
    subdivisions = (
        "AC",  # Ascension.
        "HL",  # Saint Helena.
        "TA",  # Tristan da Cunha.
    )
    subdivisions_aliases = {
        "Ascension": "AC",
        "Saint Helena": "HL",
        "Tristan da Cunha": "TA",
    }
    supported_categories = (GOVERNMENT, PUBLIC)
    supported_languages = ("en_GB", "en_US")

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, cls=SaintHelenaAscensionAndTristanDaCunhaStaticHolidays)
        kwargs.setdefault("observed_rule", SAT_SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # Good Friday.
        self._add_good_friday(tr("Good Friday"))

        # Easter Monday.
        self._add_easter_monday(tr("Easter Monday"))

        if self._year <= 2022:
            # Queen's Birthday.
            name = tr("Queen's Birthday")
            if self._year <= 2019:
                self._add_holiday_3rd_mon_of_jun(name)
            else:
                self._add_holiday_2nd_mon_of_jun(name)
        else:
            # King's Birthday.
            name = tr("King's Birthday")
            if self._year == 2023:
                self._add_holiday_jun_19(name)
            else:
                self._add_holiday_1st_fri_from_nov_11(name)

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

    def _populate_subdiv_ac_public_holidays(self):
        # Ascension Day.
        self._add_ascension_thursday(tr("Ascension Day"))

    def _populate_subdiv_hl_public_holidays(self):
        self._add_observed(
            # Saint Helena Day.
            self._add_holiday_may_21(tr("Saint Helena Day")),
            rule=SAT_TO_PREV_FRI + SUN_TO_NEXT_MON,
        )

    def _populate_subdiv_ta_public_holidays(self):
        # Ascension Day.
        self._add_ascension_thursday(tr("Ascension Day"))

        ratting_day_dates = {
            2015: (MAY, 16),
            2016: (APR, 30),
            2017: (MAY, 26),
            2018: (JUN, 2),
            2019: (MAY, 24),
            2020: (APR, 25),
            2021: (APR, 9),
            2023: (JUN, 2),
            2025: (MAY, 30),
        }
        # Ratting Day.
        name = tr("Ratting Day")
        if dt := ratting_day_dates.get(self._year):
            self._add_holiday(name, dt)

        # Anniversary Day.
        self._add_holiday_aug_14(tr("Anniversary Day"))

    def _populate_government_holidays(self):
        # New Year's Day.
        self._add_observed(self._add_new_years_day(tr("New Year's Day")))

        # Whit Monday.
        self._add_whit_monday(tr("Whit Monday"))

    def _populate_subdiv_ac_government_holidays(self):
        # August Bank Holiday.
        self._add_holiday_last_mon_of_aug(tr("August Bank Holiday"))

    def _populate_subdiv_hl_government_holidays(self):
        # August Bank Holiday.
        self._add_holiday_last_mon_of_aug(tr("August Bank Holiday"))


class SH(SaintHelenaAscensionAndTristanDaCunha):
    pass


class SHN(SaintHelenaAscensionAndTristanDaCunha):
    pass


class SaintHelenaAscensionAndTristanDaCunhaStaticHolidays:
    """Saint Helena, Ascension and Tristan da Cunha special holidays.

    References:
        * <https://web.archive.org/web/20250810194916/https://sainthelenaisland.info/holidays.htm>
        * [The Duke of Edinburgh's Visit](https://web.archive.org/web/20250719002906/https://www.sainthelena.gov.sh/2024/news/public-holiday-declared-to-mark-visit-of-his-royal-highness-the-duke-of-edinburgh-public-events-announced/)
        * [Queen Elizabeth II's State Funeral](https://web.archive.org/web/20250513122736/https://www.sainthelena.gov.sh/wp-content/uploads/2022/09/EX-GAZ-94-Proclamation-Proclaiming-His-Majesty-Public-Holiday-HM-Queen-II-Funeral-STH-ASC.pdf)
        * [Coronation of His Majesty King Charles III](https://web.archive.org/web/20250719085105/https://www.sainthelena.gov.sh/2023/news/governors-deputy-declares-public-holiday-to-mark-the-coronation-of-his-majesty-king-charles-iii/)
    """

    special_public_holidays = {
        # Final Departure of R.M.S. St Helena.
        2018: (FEB, 9, tr("Final Departure of R.M.S. St Helena")),
        2022: (
            # Queen Elizabeth II's Platinum Jubilee.
            (JUN, 3, tr("Queen Elizabeth II's Platinum Jubilee")),
            # Queen Elizabeth II's State Funeral.
            (SEP, 19, tr("Queen Elizabeth II's State Funeral")),
        ),
        2023: (
            # The Duke of Edinburgh's Visit.
            (JAN, 24, tr("The Duke of Edinburgh's Visit")),
            # Coronation of His Majesty King Charles III.
            (MAY, 8, tr("Coronation of His Majesty King Charles III")),
        ),
    }
