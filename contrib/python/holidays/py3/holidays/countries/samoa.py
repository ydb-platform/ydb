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

from holidays.groups import ChristianHolidays, InternationalHolidays
from holidays.holiday_base import HolidayBase


class Samoa(HolidayBase, ChristianHolidays, InternationalHolidays):
    """Samoa holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Samoa>
        * <https://web.archive.org/web/20250407135339/https://www.timeanddate.com/holidays/samoa/>
        * <https://web.archive.org/web/20240914112255/https://www.mcil.gov.ws/?attachment_id=6336>
        * <https://web.archive.org/web/20250413020419/http://www.paclii.org/ws/legis/consol_act_2020/pha2008163/>
    """

    country = "WS"

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)

        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day("New Year's Day")
        self._add_new_years_day_two("The Day After New Year's Day")

        # Good Friday.
        self._add_good_friday("Good Friday")
        self._add_holy_saturday("Day After Good Friday")

        # Easter Monday.
        self._add_easter_monday("Easter Monday")

        # Mother's Day.
        self._add_holiday_1_day_past_2nd_sun_of_may("Mother's Day")

        # Independence Day.
        self._add_holiday_jun_1("Independence Day")

        # Father's Day.
        self._add_holiday_1_day_past_2nd_sun_of_aug("Father's Day")

        # White Monday (Lotu a Tamaiti).
        self._add_holiday_1_day_past_2nd_sun_of_oct("White Sunday (Lotu a Tamaiti)")

        # Christmas Day.
        self._add_christmas_day("Christmas Day")

        # Boxing Day.
        self._add_christmas_day_two("Boxing Day")


class WS(Samoa):
    pass


class WSM(Samoa):
    pass
