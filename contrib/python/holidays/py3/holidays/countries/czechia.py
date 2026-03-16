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
from holidays.holiday_base import HolidayBase


class Czechia(HolidayBase, ChristianHolidays, InternationalHolidays):
    """Czechia holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_the_Czech_Republic>
        * [Law 93/1951](https://web.archive.org/web/20240924164547/https://www.zakonyprolidi.cz/cs/1951-93)
        * [Law 204/1990 (Jan Hus Day)](https://web.archive.org/web/20240924164541/https://www.zakonyprolidi.cz/cs/1990-204)
        * [Law 245/2000](https://web.archive.org/web/20241215175816/https://www.zakonyprolidi.cz/cs/2000-245)
    """

    country = "CZ"
    default_language = "cs"
    supported_languages = ("cs", "en_US", "sk", "uk")
    # Law 93/1951.
    start_year = 1952

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("Nový rok"))

        if self._year >= 2000:
            # Independent Czech State Restoration Day.
            self._add_holiday_jan_1(tr("Den obnovy samostatného českého státu"))

        if self._year >= 2016:
            # Good Friday.
            self._add_good_friday(tr("Velký pátek"))

        # Easter Monday.
        self._add_easter_monday(tr("Velikonoční pondělí"))

        # Labor Day.
        self._add_labor_day(tr("Svátek práce"))

        if self._year >= 2004:
            # Victory Day.
            name = tr("Den vítězství")
        elif self._year >= 2001:
            # Liberation Day.
            name = tr("Den osvobození")
        else:
            # Day of liberation from Fascism.
            name = tr("Den osvobození od fašismu")

        self._add_world_war_two_victory_day(name, is_western=(self._year >= 1992))

        if self._year >= 1990:
            # Saints Cyril and Methodius Day.
            self._add_holiday_jul_5(tr("Den slovanských věrozvěstů Cyrila a Metoděje"))

            # Jan Hus Day.
            self._add_holiday_jul_6(tr("Den upálení mistra Jana Husa"))

        if self._year >= 2000:
            # Statehood Day.
            self._add_holiday_sep_28(tr("Den české státnosti"))

        # Independent Czechoslovak State Day.
        self._add_holiday_oct_28(tr("Den vzniku samostatného československého státu"))

        if self._year >= 1990:
            self._add_holiday_nov_17(
                # Struggle for Freedom and Democracy Day and International Students' Day.
                tr("Den boje za svobodu a demokracii a Mezinárodní den studentstva")
                if self._year >= 2019
                # Struggle for Freedom and Democracy Day.
                else tr("Den boje za svobodu a demokracii")
            )

            # Christmas Eve.
            self._add_christmas_eve(tr("Štědrý den"))

        # Christmas Day.
        self._add_christmas_day(tr("1. svátek vánoční"))

        # Second Day of Christmas.
        self._add_christmas_day_two(tr("2. svátek vánoční"))


class CZ(Czechia):
    pass


class CZE(Czechia):
    pass
