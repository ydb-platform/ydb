# -*- coding: utf-8 -*-

#  python-holidays
#  ---------------
#  A fast, efficient Python library for generating country, province and state
#  specific sets of holidays on the fly. It aims to make determining whether a
#  specific date is a holiday as fast and flexible as possible.
#
#  Author:  ryanss <ryanssdev@icloud.com> (c) 2014-2017
#           dr-prodigy <maurizio.montel@gmail.com> (c) 2017-2020
#  Website: https://github.com/dr-prodigy/python-holidays
#  License: MIT (see LICENSE file)

from datetime import date

from dateutil.easter import easter
from dateutil.relativedelta import relativedelta as rd, SU

from holidays.holiday_base import HolidayBase


class Lithuania(HolidayBase):

    # https://en.wikipedia.org/wiki/Public_holidays_in_Lithuania
    # https://www.kalendorius.today/

    def __init__(self, **kwargs):
        self.country = "LT"
        HolidayBase.__init__(self, **kwargs)

    def _populate(self, year):
        # New Year's Day
        self[date(year, 1, 1)] = "Naujieji metai"

        # Day of Restoration of the State of Lithuania (1918)
        if year >= 1918:
            self[date(year, 2, 16)] = "Lietuvos valstybės " \
                                      "atkūrimo diena"

        # Day of Restoration of Independence of Lithuania
        # (from the Soviet Union, 1990)
        if year >= 1990:
            self[date(year, 3, 11)] = "Lietuvos nepriklausomybės " \
                                      "atkūrimo diena"

        # Easter
        easter_date = easter(year)
        self[easter_date] = "Velykos"

        # Easter 2nd day
        self[easter_date + rd(days=1)] = "Velykų antroji diena"

        # International Workers' Day
        self[date(year, 5, 1)] = "Tarptautinė darbo diena"

        # Mother's day. First Sunday in May
        self[date(year, 5, 1) + rd(weekday=SU)] = "Motinos diena"

        # Fathers's day. First Sunday in June
        self[date(year, 6, 1) + rd(weekday=SU)] = "Tėvo diena"

        # St. John's Day [Christian name],
        # Day of Dew [original pagan name]
        if year >= 2003:
            self[date(year, 6, 24)] = "Joninės, Rasos"

        # Statehood Day
        if year >= 1991:
            self[date(year, 7, 6)] = "Valstybės (Lietuvos " \
                                     "karaliaus Mindaugo " \
                                     "karūnavimo) diena"

        # Assumption Day
        self[date(year, 8, 15)] = "Žolinė (Švč. Mergelės " \
                                  "Marijos ėmimo į dangų diena)"

        # All Saints' Day
        self[date(year, 11, 1)] = "Visų šventųjų diena (Vėlinės)"

        # Christmas Eve
        self[date(year, 12, 24)] = "Šv. Kūčios"

        # Christmas 1st day
        self[date(year, 12, 25)] = "Šv. Kalėdų pirma diena"

        # Christmas 2nd day
        self[date(year, 12, 26)] = "Šv. Kalėdų antra diena"


class LT(Lithuania):
    pass


class LTU(Lithuania):
    pass
