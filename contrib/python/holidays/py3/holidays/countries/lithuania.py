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


class Lithuania(HolidayBase, ChristianHolidays, InternationalHolidays):
    """Lithuania holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Lithuania>
        * <https://web.archive.org/web/20250415141132/https://www.kalendorius.today/>
    """

    country = "LT"
    default_language = "lt"
    supported_languages = ("en_US", "lt", "uk")
    start_year = 1990

    def __init__(self, *args, **kwargs) -> None:
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self) -> None:
        # New Year's Day.
        self._add_new_years_day(tr("Naujųjų metų diena"))

        # Day of Restoration of the State of Lithuania.
        self._add_holiday_feb_16(tr("Lietuvos valstybės atkūrimo diena"))

        # Day of Restoration of Independence of Lithuania.
        self._add_holiday_mar_11(tr("Lietuvos nepriklausomybės atkūrimo diena"))

        # Easter Sunday.
        self._add_easter_sunday(tr("Šv. Velykos"))

        # Easter Monday.
        self._add_easter_monday(tr("Antroji šv. Velykų diena"))

        # International Workers' Day.
        self._add_labor_day(tr("Tarptautinė darbo diena"))

        # Mother's Day.
        self._add_holiday_1st_sun_of_may(tr("Motinos diena"))

        # Father's Day.
        self._add_holiday_1st_sun_of_jun(tr("Tėvo diena"))

        if self._year >= 2003:
            # Day of Dew and Saint John.
            self._add_saint_johns_day(tr("Rasos ir Joninių diena"))

        if self._year >= 1991:
            self._add_holiday_jul_6(
                # Statehood Day.
                tr("Valstybės (Lietuvos karaliaus Mindaugo karūnavimo) ir Tautiškos giesmės diena")
            )

        # Assumption Day.
        self._add_assumption_of_mary_day(tr("Žolinė (Švč. Mergelės Marijos ėmimo į dangų diena)"))

        # All Saints' Day.
        self._add_all_saints_day(tr("Visų Šventųjų diena"))

        if self._year >= 2020:
            # All Souls' Day.
            self._add_all_souls_day(tr("Mirusiųjų atminimo (Vėlinių) diena"))

        # Christmas Eve.
        self._add_christmas_eve(tr("Kūčių diena"))

        # Christmas Day.
        self._add_christmas_day(tr("Šv. Kalėdų pirma diena"))

        # Second Day of Christmas.
        self._add_christmas_day_two(tr("Šv. Kalėdų antra diena"))


class LT(Lithuania):
    pass


class LTU(Lithuania):
    pass
