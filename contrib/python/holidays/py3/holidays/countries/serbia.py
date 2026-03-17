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

from holidays.calendars.julian import JULIAN_CALENDAR
from holidays.groups import ChristianHolidays, InternationalHolidays
from holidays.observed_holiday_base import ObservedHolidayBase, SUN_TO_NEXT_MON, SUN_TO_NEXT_TUE


class Serbia(ObservedHolidayBase, ChristianHolidays, InternationalHolidays):
    """Serbia holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Serbia>
    """

    country = "RS"
    default_language = "sr"
    # %s (observed).
    observed_label = tr("%s (слободан дан)")
    # Independence Restored on June 5th, 2006.
    start_year = 2007
    supported_languages = ("en_US", "sr")

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self, JULIAN_CALENDAR)
        InternationalHolidays.__init__(self)
        kwargs.setdefault("observed_rule", SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        name = tr("Нова година")
        self._add_observed(self._add_new_years_day(name), rule=SUN_TO_NEXT_TUE)
        self._add_observed(self._add_new_years_day_two(name))

        # Orthodox Christmas Day.
        self._add_christmas_day(tr("Божић"))

        # Statehood Day.
        name = tr("Дан државности Србије")
        self._add_observed(self._add_holiday_feb_15(name), rule=SUN_TO_NEXT_TUE)
        self._add_observed(self._add_holiday_feb_16(name))

        # Labor Day.
        name = tr("Празник рада")
        self._add_observed(self._add_labor_day(name), rule=SUN_TO_NEXT_TUE)

        self._add_observed(
            may_2 := self._add_labor_day_two(name),
            rule=SUN_TO_NEXT_TUE if may_2 == self._easter_sunday else SUN_TO_NEXT_MON,
        )

        # Armistice Day.
        self._add_observed(self._add_remembrance_day(tr("Дан примирја у Првом светском рату")))

        # Good Friday.
        self._add_good_friday(tr("Велики петак"))

        # Holy Saturday.
        self._add_holy_saturday(tr("Велика субота"))

        # Easter Sunday.
        self._add_easter_sunday(tr("Васкрс"))

        # Easter Monday.
        self._add_easter_monday(tr("Други дан Васкрса"))


class RS(Serbia):
    pass


class SRB(Serbia):
    pass
