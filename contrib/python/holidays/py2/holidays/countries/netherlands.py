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
from dateutil.relativedelta import relativedelta as rd, FR

from holidays.constants import JAN, APR, MAY, AUG, DEC
from holidays.constants import SUN
from holidays.holiday_base import HolidayBase


class Netherlands(HolidayBase):

    def __init__(self, **kwargs):
        # http://www.iamsterdam.com/en/plan-your-trip/practical-info/public-holidays
        self.country = "NL"
        HolidayBase.__init__(self, **kwargs)

    def _populate(self, year):
        # New years
        self[date(year, JAN, 1)] = "Nieuwjaarsdag"

        easter_date = easter(year)

        # Easter
        self[easter_date] = "Eerste paasdag"

        # Good friday
        self[easter_date + rd(weekday=FR(-1))] = "Goede Vrijdag"

        # Second easter day
        self[easter_date + rd(days=1)] = "Tweede paasdag"

        # Ascension day
        self[easter_date + rd(days=39)] = "Hemelvaart"

        # Pentecost
        self[easter_date + rd(days=49)] = "Eerste Pinksterdag"

        # Pentecost monday
        self[easter_date + rd(days=50)] = "Tweede Pinksterdag"

        # First christmas
        self[date(year, DEC, 25)] = "Eerste Kerstdag"

        # Second christmas
        self[date(year, DEC, 26)] = "Tweede Kerstdag"

        # Liberation day
        if year >= 1945 and year % 5 == 0:
            self[date(year, MAY, 5)] = "Bevrijdingsdag"

        # Kingsday
        if year >= 2014:
            kings_day = date(year, APR, 27)
            if kings_day.weekday() == SUN:
                kings_day = kings_day - rd(days=1)

            self[kings_day] = "Koningsdag"

        # Queen's day
        if 1891 <= year <= 2013:
            queens_day = date(year, APR, 30)
            if year <= 1948:
                queens_day = date(year, AUG, 31)

            if queens_day.weekday() == SUN:
                if year < 1980:
                    queens_day = queens_day + rd(days=1)
                else:
                    queens_day = queens_day - rd(days=1)

            self[queens_day] = "Koninginnedag"


class NL(Netherlands):
    pass


class NLD(Netherlands):
    pass
