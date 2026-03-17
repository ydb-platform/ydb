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

from holidays.calendars.gregorian import JAN, FEB, MAR, AUG
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    SUN_TO_NEXT_MON,
    SAT_SUN_TO_NEXT_MON_TUE,
    SAT_SUN_TO_NEXT_MON,
)


class AntiguaAndBarbuda(
    ObservedHolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays
):
    """Antigua and Barbuda holidays.

    References:
        * [The Public Holidays Act, 1954](https://web.archive.org/web/20250123110511/https://laws.gov.ag/wp-content/uploads/2018/08/cap-354.pdf)
        * [The Public Holidays (Amendment) Act, 2005](https://web.archive.org/web/20250103221834/https://laws.gov.ag/wp-content/uploads/2018/08/a2005-8.pdf)
        * [The Public Holidays (Amendment) Act, 2014](https://web.archive.org/web/20250427174435/https://laws.gov.ag/wp-content/uploads/2019/03/Public-Holidays-Amendment-Act.pdf)
        * [The Public Holidays (Amendment) Act, 2019](https://web.archive.org/web/20250427174433/https://laws.gov.ag/wp-content/uploads/2020/02/No.-23-of-2019-Public-Holidays-Amendment-Act-2019.pdf)
        * [No. 24 of 2006 Proclamation](https://web.archive.org/web/20240620021627/http://laws.gov.ag/wp-content/uploads/2022/06/No.-24-of-2006-Proclamation-dated-29th-November2006-Appointing-the-11th-of-December2006.pdf)
        * [No. 40 of 2012 Proclamation](https://web.archive.org/web/20250427174546/https://laws.gov.ag/wp-content/uploads/2021/08/No.-40-of-2012-Proclamation-dated-the-27th-day-of-November-2012-Apponting-the-27th-of-December-2012-as-a-Public-Holiday-throughout-Antigua-and-Barbuda.pdf)
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Antigua_and_Barbuda>

    Notes:
        In accordance with No. 24 of 2006 Proclamation, National Heroes Day was celebrated on
        Dec 11, 2006. In accordance with No. 40 of 2012 Proclamation, National Heroes Day was
        celebrated on Dec 10, 2012.
    """

    country = "AG"
    observed_label = "%s (observed)"
    start_year = 1955

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, AntiguaAndBarbudaStaticHolidays)
        kwargs.setdefault("observed_rule", SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_observed(self._add_new_years_day("New Year's Day"))

        # Good Friday.
        self._add_good_friday("Good Friday")

        # Easter Monday.
        self._add_easter_monday("Easter Monday")

        # Labour Day.
        self._add_holiday_1st_mon_of_may("Labour Day")

        # Whit Monday.
        self._add_whit_monday("Whit Monday")

        if self._year <= 2005:
            # Caribbean Community (Caricom) Day.
            self._add_holiday_1st_mon_of_jul("Caribbean Community (Caricom) Day")

        # Carnival Monday.
        self._add_holiday_1st_mon_of_aug("Carnival Monday")

        if self._year >= 2006:
            # Carnival Tuesday.
            self._add_holiday_1_day_past_1st_mon_of_aug("Carnival Tuesday")

        # Independence Day.
        dt = self._add_holiday_nov_1("Independence Day")
        if self._year >= 2005:
            self._add_observed(dt, rule=SAT_SUN_TO_NEXT_MON)

        if self._year >= 2005:
            name = (
                # Sir Vere Cornwall Bird Snr. Day.
                "Sir Vere Cornwall Bird Snr. Day"
                if self._year >= 2014
                # National Heroes Day.
                else "National Heroes Day"
            )
            if self._year == 2006:
                self._add_holiday_dec_11(name)
            elif self._year == 2012:
                self._add_holiday_dec_10(name)
            else:
                dt = self._add_holiday_dec_9(name)
                if self._year >= 2020:
                    self._add_observed(dt, rule=SAT_SUN_TO_NEXT_MON)

        # Christmas Day.
        self._add_observed(self._add_christmas_day("Christmas Day"), rule=SAT_SUN_TO_NEXT_MON_TUE)

        # Boxing Day.
        self._add_observed(self._add_christmas_day_two("Boxing Day"), rule=SAT_SUN_TO_NEXT_MON_TUE)


class AG(AntiguaAndBarbuda):
    pass


class ATG(AntiguaAndBarbuda):
    pass


class AntiguaAndBarbudaStaticHolidays:
    """Antigua and Barbuda special holidays.

    References:
        * [August 3, 1993 Holiday](https://web.archive.org/web/20241215194404/https://laws.gov.ag/wp-content/uploads/2021/08/No.-42-of-1993-Proclamation-dated-the-22nd-day-of-July-1993-appointing-Tuesday-the-3rd-day-of-August-1993-as-a-public-holiday-throughout-Antigua-and-Barbuda..pdf)
        * [State Funeral of the late The Honourable Charlesworth T. Samuel](https://web.archive.org/web/20250427174441/https://laws.gov.ag/wp-content/uploads/2022/12/No.-7-of-2008-Proclamation-dated-the-13th-day-of-February-2008-Appointing-Tuesday-19th-February-2008-as-a-Public-Holiday.pdf)
        * [State Funeral of the late The Honourable Sir George Herbert Walter](https://web.archive.org/web/20250427174444/https://laws.gov.ag/wp-content/uploads/2022/12/No.-9-of-2008-Proclamation-dated-the-13th-day-of-March-2008-Appointing-Tuesday-18th-March-2008-as-a-Public-Holiday.pdf)
        * According to the Public Holidays (Amendment) Act, 2014, the day after the general
          election is a holiday.
            * [2018 Antiguan general election](https://en.wikipedia.org/wiki/2018_Antiguan_general_election)
            * [2023 Antiguan general election](https://en.wikipedia.org/wiki/2023_Antiguan_general_election)
            * [The Public Holidays (Amendment) Act, 2014](https://web.archive.org/web/20250427174435/https://laws.gov.ag/wp-content/uploads/2019/03/Public-Holidays-Amendment-Act.pdf)
    """

    # Day after the General Election.
    day_after_the_general_election = "Day after the General Election"

    special_public_holidays = {
        1993: (AUG, 3, "Public Holiday"),
        2008: (
            # State Funeral of the late The Honourable Charlesworth T. Samuel.
            (FEB, 19, "State Funeral of the late The Honourable Charlesworth T. Samuel"),
            # State Funeral of the late The Honourable Sir George Herbert Walter.
            (MAR, 18, "State Funeral of the late The Honourable Sir George Herbert Walter"),
        ),
        2018: (MAR, 22, day_after_the_general_election),
        2023: (JAN, 19, day_after_the_general_election),
    }
