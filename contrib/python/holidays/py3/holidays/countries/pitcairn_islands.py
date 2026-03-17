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

from holidays.constants import GOVERNMENT, PUBLIC, WORKDAY
from holidays.groups import ChristianHolidays, InternationalHolidays
from holidays.holiday_base import HolidayBase


class PitcairnIslands(HolidayBase, ChristianHolidays, InternationalHolidays):
    """Pitcairn Islands holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Pitcairn_Islands>
        * [Public Holidays & Commemoration Days (2016)](https://web.archive.org/web/20161112174923/https://www.government.pn/policies/GPI%20-%20Approved%20Public%20Holidays%20&%20Commemoration%20Days.pdf)
        * [Public Holidays & Commemoration Days (2024)](https://web.archive.org/web/20250517050245/https://static1.squarespace.com/static/6526ff6fef608a3828c13d05/t/6673d53b1c0e2f660cc31848/1718867262145/GPI_Policy_Public_Holidays_&_Commemoration_Days_June_2024.pdf)
        * [The Laws of Pitcairn, Henderson, Ducie and Oeno Islands - Volume I](https://web.archive.org/web/20240418043000/https://static1.squarespace.com/static/6526ff6fef608a3828c13d05/t/65585fca2ce3972fb3bc8da3/1700290563094/Revised+Laws+of+Pitcairn,+Henderson,+Ducie+and+Oeno+Islands,+2017+Rev.+Ed.+-+Volume+1.pdf)
        * [CIA The World Factbook](https://web.archive.org/web/20211217233431/https://www.cia.gov/the-world-factbook/countries/pitcairn-islands/)
    """

    country = "PN"
    supported_categories = (GOVERNMENT, PUBLIC, WORKDAY)
    # First available online source.
    start_year = 2016

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day("New Year's Day")

        # Bounty Day.
        self._add_holiday_jan_23("Bounty Day")

        # Good Friday.
        self._add_good_friday("Good Friday")

        # Easter Monday.
        self._add_easter_monday("Easter Monday")

        if self._year >= 2023:
            # King's Birthday.
            self._add_holiday_1st_mon_of_jun("King's Birthday")
        else:
            # Queen's Birthday.
            self._add_holiday_2nd_sat_of_jun("Queen's Birthday")

        # Pitcairn Day.
        self._add_holiday_jul_2("Pitcairn Day")

        # Christmas Day.
        self._add_christmas_day("Christmas Day")

        # Boxing Day.
        self._add_christmas_day_two("Boxing Day")

    def _populate_government_holidays(self):
        # New Year's Day.
        self._add_new_years_day_two("New Year's Day")

    def _populate_workday_holidays(self):
        # ANZAC Day.
        self._add_anzac_day("ANZAC Day")

        # Remembrance Day.
        self._add_remembrance_day("Remembrance Day")


class PN(PitcairnIslands):
    pass


class PCN(PitcairnIslands):
    pass
