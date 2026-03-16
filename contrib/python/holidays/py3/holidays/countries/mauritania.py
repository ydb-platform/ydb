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

from holidays.calendars.gregorian import FRI, SAT
from holidays.groups import InternationalHolidays, IslamicHolidays
from holidays.holiday_base import HolidayBase


class Mauritania(HolidayBase, InternationalHolidays, IslamicHolidays):
    """Mauritania holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Mauritania>
        * <https://web.archive.org/web/20250408205543/https://www.timeanddate.com/holidays/mauritania/>
    """

    country = "MR"
    # %s (estimated).
    estimated_label = "%s (estimated)"
    weekend = {FRI, SAT}

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.
        """
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(self, show_estimated=islamic_show_estimated)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day("New Year's Day")

        # Labor Day.
        self._add_labor_day("Labor Day")

        # Africa Day.
        self._add_africa_day("Africa Day")

        # Independence Day.
        if self._year >= 1960:
            self._add_holiday_nov_28("Independence Day")

        # Islamic holidays.
        # Eid al-Fitr.
        self._add_eid_al_fitr_day("Eid al-Fitr")
        self._add_eid_al_fitr_day_two("Eid al-Fitr")

        # Eid al-Adha.
        self._add_eid_al_adha_day("Eid al-Adha")
        self._add_eid_al_adha_day_two("Eid al-Adha")

        # Muharram/Islamic New Year.
        self._add_islamic_new_year_day("Islamic New Year")

        # Prophet Muhammad's Birthday.
        self._add_mawlid_day("Mawlid al-Nabi")


class MR(Mauritania):
    pass


class MRT(Mauritania):
    pass
