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

from holidays.groups import ChristianHolidays, InternationalHolidays, IslamicHolidays
from holidays.holiday_base import HolidayBase


class Uganda(HolidayBase, ChristianHolidays, InternationalHolidays, IslamicHolidays):
    """Uganda holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Uganda>
        * [Public Holidays Act, Cap. 255](https://web.archive.org/web/20250621140041/https://ulii.org/akn/ug/act/1965/23/eng@2000-12-31)
        * [National Events and Calendar](https://web.archive.org/web/20250618053410/https://www.vicepresident.go.ug/national-events-and-calender/)
        * [National Heroes Day](https://web.archive.org/web/20230622162351/https://publicholidays.africa/uganda/national-heroes-day/)
        * [NRM Liberation Day](https://web.archive.org/web/20240124064853/https://thenilewires.com/38th-nrm-a-liberation-day-celebrations-minister-outlines-nrm-achievements/)
        * [Archbishop Janani Luwum Day](https://web.archive.org/web/20200805133919/https://www.newvision.co.ug/news/1417242/uganda-declared-february-archbishop-janani-luwum-public-holiday)
        * [School Calendar 2024](https://web.archive.org/web/20240519021615/https://www.education.go.ug/wp-content/uploads/2023/11/SCHOOL-CALENDAR.pdf)
        * [Public Holidays 2025](https://web.archive.org/web/20250710193157/https://publicholidays.co.ug/)
    """

    country = "UG"
    # %s (estimated).
    estimated_label = "%s (estimated)"
    # Uganda gained independence from the United Kingdom on October 9, 1962.
    start_year = 1963

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.
        """
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(self, show_estimated=islamic_show_estimated)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day("New Year's Day")

        if self._year >= 1987:
            # NRM Liberation Day.
            self._add_holiday_jan_26("NRM Liberation Day")

        if self._year >= 2016:
            # Archbishop Janani Luwum Day.
            self._add_holiday_feb_16("Archbishop Janani Luwum Day")

        # Women's Day.
        self._add_womens_day("Women's Day")

        # Good Friday.
        self._add_good_friday("Good Friday")

        # Easter Monday.
        self._add_easter_monday("Easter Monday")

        # Labor Day.
        self._add_labor_day("Labour Day")

        # Uganda Martyrs' Day.
        self._add_holiday_jun_3("Uganda Martyrs' Day")

        if self._year >= 2001:
            # National Heroes' Day.
            self._add_holiday_jun_9("National Heroes' Day")

        # Independence Day.
        self._add_holiday_oct_9("Independence Day")

        # Christmas Day.
        self._add_christmas_day("Christmas Day")

        # Boxing Day.
        self._add_christmas_day_two("Boxing Day")

        # Eid al-Fitr.
        self._add_eid_al_fitr_day("Eid al-Fitr")

        # Eid al-Adha.
        self._add_eid_al_adha_day("Eid al-Adha")


class UG(Uganda):
    pass


class UGA(Uganda):
    pass
