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

from holidays.calendars.gregorian import SEP, DEC
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    TUE_TO_PREV_MON,
    WED_THU_TO_NEXT_FRI,
    SAT_SUN_TO_NEXT_MON,
    SUN_TO_NEXT_MON,
    SUN_TO_NEXT_TUE,
)


class Bahamas(ObservedHolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays):
    """Bahamas holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_the_Bahamas>
        * <https://web.archive.org/web/20250421024112/http://laws.bahamas.gov.bs/cms/images/LEGISLATION/PRINCIPAL/1938/1938-0015/PublicHolidaysAct_1.pdf>
        * <https://archive.org/details/majority-rule-public-holiday-act-2013>
        * <https://archive.org/details/randol-fawkes-labour-day-act-2013>
        * <https://web.archive.org/web/20210922210604/http://www.tribune242.com/news/2013/oct/12/national-heroes-day-formally-established/>
        * <https://web.archive.org/web/20190905051207/http://eleutheranews.com/?p=3594>

    Checked With:
        * <https://web.archive.org/web/20250218064125/https://www.bahamashclondon.net/consular-information/public-holidays/>
        * <https://web.archive.org/web/20250427182357/https://bisxbahamas.com/wp-content/uploads/2020/12/Trading-Calendar-2021.pdf>
        * <https://web.archive.org/web/20231210053149/https://publicholidays.la/the-bahamas/2022-dates/>
    """

    country = "BS"
    observed_label = "%s (observed)"
    # Gained Independence on Jul 10, 1973.
    start_year = 1974

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, BahamasStaticHolidays)
        kwargs.setdefault("observed_rule", SAT_SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        # Pre-2012 Observance:
        # - If TUE, New Year's Day (observed) prev MON.
        # - If WED or THU, New Year's Day (observed) next FRI.
        # 2012 and beyond Observance: If SUN, New Year's Day (observed) next MON (not for SAT).
        self._add_observed(
            self._add_new_years_day("New Year's Day"),
            rule=SUN_TO_NEXT_MON
            if self._year >= 2012
            else SAT_SUN_TO_NEXT_MON + TUE_TO_PREV_MON + WED_THU_TO_NEXT_FRI,
        )

        # Majority Rule Day.
        # Officially made a holiday on Oct 11, 2013 under Majority Rule (Public Holiday) Act 2013.
        if self._year >= 2014:
            self._add_observed(self._add_holiday_jan_10("Majority Rule Day"))

        # Good Friday.
        self._add_good_friday("Good Friday")

        # Easter Monday.
        self._add_easter_monday("Easter Monday")

        # Whit Monday.
        self._add_whit_monday("Whit Monday")

        # Randol Fawkes Labour Day.
        # This was simply known as "Labour Day" prior to Randol Fawkes Labour Day Act 2013.
        self._add_holiday_1st_fri_of_jun(
            "Randol Fawkes Labour Day" if self._year >= 2013 else "Labour Day"
        )

        # Independence Day.
        self._add_observed(self._add_holiday_jul_10("Independence Day"))

        # Emancipation Day.
        self._add_holiday_1st_mon_of_aug("Emancipation Day")

        # National Heroes Day.
        # Known as "Discovery Day" prior to 2013, with its date fixed as Oct 12 annually.
        # Got its name changed on Oct 11, 2013 under Majority Rule (Public Holiday) Act 2013.
        # Pre-2013 Observance:
        # - If TUE, Discovery Day (observed) prev MON.
        # - If WED or THU, Discovery Day (observed) next FRI.
        if self._year >= 2013:
            self._add_holiday_2nd_mon_of_oct("National Heroes Day")
        else:
            self._add_observed(
                self._add_columbus_day("Discovery Day"),
                rule=SAT_SUN_TO_NEXT_MON + TUE_TO_PREV_MON + WED_THU_TO_NEXT_FRI,
            )

        # Christmas Holidays Exception Rules.
        # Observance Exception:
        # FRI-SAT -> Boxing Day (observed) next MON.
        # SAT-SUN -> Boxing Day (observed) next MON.
        # SUN-MON -> Christmas Day (observed) next TUE.

        # Christmas Day.
        self._add_observed(self._add_christmas_day("Christmas Day"), rule=SUN_TO_NEXT_TUE)

        # Boxing Day.
        self._add_observed(self._add_christmas_day_two("Boxing Day"))


class BS(Bahamas):
    pass


class BHS(Bahamas):
    pass


class BahamasStaticHolidays:
    special_public_holidays = {
        # https://web.archive.org/web/20250122110613/https://www.bahamas.gov.bs/wps/portal/public/gov/government/notices/national%20holiday%2019th%20september/
        2022: (SEP, 19, "State Funeral of Queen Elizabeth II"),
    }

    special_public_holidays_observed = {
        # New Year's Day observance overflow.
        # This only applies to Pre-2012 observance.
        1979: (DEC, 31, "New Year's Day"),
        1984: (DEC, 31, "New Year's Day"),
        1990: (DEC, 31, "New Year's Day"),
        2001: (DEC, 31, "New Year's Day"),
        2007: (DEC, 31, "New Year's Day"),
    }
