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

from holidays.calendars.gregorian import JAN, JUL, SEP
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.observed_holiday_base import ObservedHolidayBase, SUN_TO_NEXT_MON, SUN_TO_NEXT_TUE


class Dominica(ObservedHolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays):
    """Dominica holidays.

    References:
        * <https://web.archive.org/web/20241204154212/http://www.dominica.gov.dm/laws/chapters/chap19-10.pdf>
        * <https://web.archive.org/web/20250427181829/https://pressroomopm.gov.dm/notice-public-holiday-order-2022/>
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Dominica>
        * <https://web.archive.org/web/20220723095045/https://www.dominica.gov.dm/laws/1998/sro1-1998.pdf>

    Cross-Checked With:
        * [2010](https://web.archive.org/web/20240826101956/https://www.dominica-weekly.com/images/dominica-calendar-2010/1600-1280.jpg)
        * [2011-2020](https://web.archive.org/web/20241112201134/https://dominicaconsulategreece.com/dominica/public-holidays/)
        * <http://archive.today/2025.04.29-124436/http://www.q95da.com/news/q95-news-received-on-december-29-2020-at-731pm-the-official-public-holiday-calendar-for-2021-approved-by-the-government-of-dominica>
        * [2022-2024](https://web.archive.org/web/20250407175225/https://dominica.gov.dm/about-dominica/public-holidays)

    While Labour Day is listed in the 1990 amendment as May 1st, this has, de facto, been
    made 1st Monday of May since at least 2010.

    Where in any year two public holidays fall on the same day, then the next succeeding day
    not being itself a public holiday shall be observed as a public holiday. In practice, this
    only applies to Holidays which falls on Saturday or Sunday.
    """

    country = "DM"
    observed_label = "%s (observed)"
    # Public Holidays Act, L.I. 12 of 1990 Amendment.
    start_year = 1990

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, DominicaStaticHolidays)
        kwargs.setdefault("observed_rule", SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_observed(self._add_new_years_day("New Year's Day"))

        # Carnival Monday.
        self._add_carnival_monday("Carnival Monday")

        # Carnival Tuesday.
        self._add_carnival_tuesday("Carnival Tuesday")

        # Good Friday.
        self._add_good_friday("Good Friday")

        # Easter Monday.
        self._add_easter_monday("Easter Monday")

        # Labour Day.
        labour_day_name = "Labour Day"
        if self._year >= 2010:
            self._add_holiday_1st_mon_of_may(labour_day_name)
        else:
            self._add_observed(self._add_labor_day(labour_day_name))

        # Whit Monday.
        self._add_whit_monday("Whit Monday")

        self._add_holiday_1st_mon_of_aug(
            # Emancipation Day.
            "Emancipation Day"
            if self._year >= 1998
            # First Monday of August.
            else "First Monday of August"
        )

        # Independence Day.
        self._add_observed(self._add_holiday_nov_3("Independence Day"), rule=SUN_TO_NEXT_TUE)

        # National Day of Community Service	.
        self._add_observed(self._add_holiday_nov_4("National Day of Community Service"))

        # Christmas Day.
        self._add_observed(self._add_christmas_day("Christmas Day"), rule=SUN_TO_NEXT_TUE)

        # Boxing Day.
        self._add_observed(self._add_christmas_day_two("Boxing Day"))


class DM(Dominica):
    pass


class DMA(Dominica):
    pass


class DominicaStaticHolidays:
    """Dominica special holidays.

    References:
        * <https://web.archive.org/web/20231123061211/https://qppstudio-public-holidays-news.blogspot.com/2009/07/dominica-declares-july-28-public.html>
        * <https://web.archive.org/web/20220723095040/https://www.dominica.gov.dm/laws/2009/sro35-2009.pdf>
        * <https://web.archive.org/web/20220723095033/https://www.dominica.gov.dm/laws/2009/sro55-2009.pdf>
        * <https://web.archive.org/web/20250427181820/https://emonewsdm.com/thursday-september-19-2019-declared-public-holiday-in-dominica/>
    """

    # Special Public Holidays.
    special_public_holiday_name = "Special Public Holiday"

    special_public_holidays = {
        2009: (
            (JUL, 28, special_public_holiday_name),
            (SEP, 3, special_public_holiday_name),
        ),
        2010: (JAN, 4, special_public_holiday_name),
        # Post-Hurricane Maria Thanksgiving Celebrations.
        2019: (SEP, 19, "Post-Hurricane Maria Thanksgiving Celebrations"),
    }
