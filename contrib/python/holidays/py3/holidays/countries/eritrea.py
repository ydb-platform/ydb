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

from holidays.calendars.ethiopian import ETHIOPIAN_CALENDAR
from holidays.constants import GOVERNMENT, PUBLIC
from holidays.groups import ChristianHolidays, InternationalHolidays, IslamicHolidays
from holidays.holiday_base import HolidayBase


class Eritrea(HolidayBase, ChristianHolidays, InternationalHolidays, IslamicHolidays):
    """Eritrea holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Eritrea>
        * <https://web.archive.org/web/20230313234629/https://explore-eritrea.com/working-hours-and-holidays/>
        * <https://web.archive.org/web/20110903130335/http://www.eritrea.be/old/eritrea-religions.htm>
        * <https://web.archive.org/web/20250820085307/https://www.mintageworld.com/media/detail/11411-fenkil-day-in-eritrea/>
    """

    country = "ER"
    # %s (estimated).
    estimated_label = "%s (estimated)"
    # On 28 May 1993, Eritrea was admitted into the United Nations as the 182nd member state.
    start_year = 1994
    supported_categories = (GOVERNMENT, PUBLIC)

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.
        """
        ChristianHolidays.__init__(self, ETHIOPIAN_CALENDAR)
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(self, show_estimated=islamic_show_estimated)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day("New Year's Day")

        # Orthodox Christmas.
        self._add_christmas_day("Orthodox Christmas")

        # Epiphany.
        self._add_epiphany_day("Epiphany")

        # Women's Day.
        self._add_womens_day("Women's Day")

        # Good Friday.
        self._add_good_friday("Good Friday")

        # Orthodox Easter.
        self._add_easter_sunday("Orthodox Easter")

        # International Workers' Day.
        self._add_labor_day("International Workers' Day")

        # Independence Day.
        self._add_holiday_may_24("Independence Day")

        # Martyrs' Day.
        self._add_holiday_jun_20("Martyrs' Day")

        # Revolution Day.
        self._add_holiday_sep_1("Revolution Day")

        # Ethiopian New Year.
        self._add_ethiopian_new_year("Ethiopian New Year")

        # Finding of the True Cross.
        self._add_finding_of_true_cross("Finding of the True Cross")

        # Prophet's Birthday.
        self._add_mawlid_day("Prophet's Birthday")

        # Eid al-Fitr.
        self._add_eid_al_fitr_day("Eid al-Fitr")

        # Eid al-Adha.
        self._add_eid_al_adha_day("Eid al-Adha")

    def _populate_government_holidays(self):
        # Fenkil Day.
        self._add_holiday_feb_10("Fenkil Day")


class ER(Eritrea):
    pass


class ERI(Eritrea):
    pass
