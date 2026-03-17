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
from holidays.observed_holiday_base import ObservedHolidayBase, SAT_TO_NONE, SUN_TO_NEXT_MON


class IceFuturesEurope(ObservedHolidayBase, ChristianHolidays, InternationalHolidays):
    """ICE Futures Europe holidays.

    References:
        * <https://web.archive.org/web/20241203121313/https://www.ice.com/publicdocs/futures/Trading_Schedule_Migrated_Liffe_Contracts.pdf>
        * <https://web.archive.org/web/20241118133442/https://www.ice.com/publicdocs/Trading_Schedule.pdf>
        * <https://web.archive.org/web/20230927015846/https://www.ice.com/publicdocs/Trading_Schedule.pdf>
        * <https://web.archive.org/web/20211022183728/https://www.ice.com/publicdocs/Trading_Schedule.pdf>
    """

    market = "IFEU"
    start_year = 2014

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        kwargs.setdefault("observed_rule", SAT_TO_NONE + SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        self._move_holiday(self._add_new_years_day("New Year's Day"))

        self._add_good_friday("Good Friday")

        self._move_holiday(self._add_christmas_day("Christmas Day"))


class ICEFuturesEurope(IceFuturesEurope):
    pass


class IFEU(IceFuturesEurope):
    pass
