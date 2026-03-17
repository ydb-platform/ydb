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

from holidays.groups import ChristianHolidays, InternationalHolidays
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    SAT_SUN_TO_NEXT_MON,
    SAT_SUN_TO_NEXT_MON_TUE,
)


class Nauru(ObservedHolidayBase, ChristianHolidays, InternationalHolidays):
    """Nauru holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Nauru>
        * [Public Service Act 2016](https://web.archive.org/web/20250613095858/https://ronlaw.gov.nr/assets/docs/acts/2016/Public%20Service%20Act%202016_serv4.pdf)
        * [Nauru's Online Legal Database](https://web.archive.org/web/20250501021029/https://ronlaw.gov.nr/)
        * <https://web.archive.org/web/20250408012244/https://www.timeanddate.com/holidays/nauru/>

    Gazettes:
        * [Public Holidays 2019](https://web.archive.org/web/20250613095031/https://ronlaw.gov.nr/assets/docs/gazettes/2018/11/Gazette%20No.%20175%20(28%20November%202018).pdf)
        * [Public Holidays 2021](https://web.archive.org/web/20250613102242/https://ronlaw.gov.nr/assets/docs/gazettes/2020/12/Gazette%20No.%20227%20(21%20December%202020).pdf)
        * [Public Holidays 2022](https://web.archive.org/web/20250613102128/https://ronlaw.gov.nr/assets/docs/gazettes/2022/01/Gazette%20No.%207%20(6%20January%202022).pdf)
        * [Public Holidays 2023](https://web.archive.org/web/20250613101855/https://ronlaw.gov.nr/assets/docs/gazettes/2022/12/Gazette%20No.%20330%20(30%20December%202022).pdf)
        * [Public Holidays 2024](https://web.archive.org/web/20250613094703/https://ronlaw.gov.nr/assets/docs/gazettes/2024/01/Nauru%20Government%20Gazette,%20No.%2014%20(15%20January%202024).pdf)
        * [RONPHOS Handover Day 2018](https://web.archive.org/web/20250613095226/https://ronlaw.gov.nr/assets/docs/gazettes/2018/06/Gazette%20No.%20102%20(29%20June%202018).pdf)
        * [Ibumin Earoeni Day 2019](https://web.archive.org/web/20250613095413/https://ronlaw.gov.nr/assets/docs/gazettes/2019/08/Gazette%20No.%20139%20(12%20August%202019).pdf)
        * [Sir Hammer DeRoburt Day 2020](https://web.archive.org/web/20250614102647/https://ronlaw.gov.nr/assets/docs/gazettes/2020/09/Gazette%20No.%20171%20(10%20September%202020).pdf)
    """

    country = "NR"
    default_language = "en_NR"
    # %s (observed).
    observed_label = tr("%s (observed)")
    supported_languages = ("en_NR", "en_US")
    # Nauru gained independence on January 31, 1968.
    start_year = 1969

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        kwargs.setdefault("observed_rule", SAT_SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_observed(self._add_new_years_day(tr("New Year's Day")))

        self._add_observed(
            # Independence Day.
            self._add_holiday_jan_31(tr("Independence Day")),
            rule=SAT_SUN_TO_NEXT_MON_TUE,
        )

        self._add_observed(
            # Day following Independence Day.
            self._add_holiday_feb_1(tr("Day following Independence Day")),
            rule=SAT_SUN_TO_NEXT_MON_TUE,
        )

        if self._year >= 2019:
            # International Women's Day.
            self._add_observed(self._add_womens_day(tr("International Women's Day")))

        # Good Friday.
        self._add_good_friday(tr("Good Friday"))

        # Easter Monday.
        self._add_easter_monday(tr("Easter Monday"))

        # Easter Tuesday.
        self._add_easter_tuesday(tr("Easter Tuesday"))

        # Constitution Day.
        self._add_observed(self._add_holiday_may_17(tr("Constitution Day")))

        if self._year >= 2018:
            # RONPHOS Handover.
            self._add_observed(self._add_holiday_jul_1(tr("RONPHOS Handover")))

        if self._year >= 2019:
            # Ibumin Earoeni Day.
            self._add_observed(self._add_holiday_aug_19(tr("Ibumin Earoeni Day")))

        if self._year >= 2001:
            self._add_observed(
                self._add_holiday_sep_25(
                    # Sir Hammer DeRoburt Day.
                    tr("Sir Hammer DeRoburt Day")
                    if self._year >= 2020
                    # National Youth Day.
                    else tr("National Youth Day")
                )
            )

        # Angam Day.
        self._add_observed(self._add_holiday_oct_26(tr("Angam Day")))

        self._add_observed(
            # Christmas Day.
            self._add_christmas_day(tr("Christmas Day")),
            rule=SAT_SUN_TO_NEXT_MON_TUE,
        )

        self._add_observed(
            # Day following Christmas.
            self._add_christmas_day_two(tr("Day following Christmas")),
            rule=SAT_SUN_TO_NEXT_MON_TUE,
        )


class NR(Nauru):
    pass


class NRU(Nauru):
    pass
