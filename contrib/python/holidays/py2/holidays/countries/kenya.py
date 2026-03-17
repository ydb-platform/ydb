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
from dateutil.relativedelta import relativedelta as rd, FR, MO

from holidays.constants import JAN, MAY, JUN, OCT, \
    DEC
from holidays.constants import SUN
from holidays.holiday_base import HolidayBase


class Kenya(HolidayBase):
    # https://en.wikipedia.org/wiki/Public_holidays_in_Kenya
    # http://kenyaembassyberlin.de/Public-Holidays-in-Kenya.48.0.html
    def __init__(self, **kwargs):
        self.country = "KE"
        HolidayBase.__init__(self, **kwargs)

    def _populate(self, year):
        # Public holidays
        self[date(year, JAN, 1)] = "New Year's Day"
        self[date(year, MAY, 1)] = "Labour Day"
        self[date(year, JUN, 1)] = "Madaraka Day"
        self[date(year, OCT, 20)] = "Mashujaa Day"
        self[date(year, DEC, 12)] = "Jamhuri (Independence) Day"
        self[date(year, DEC, 25)] = "Christmas Day"
        self[date(year, DEC, 26)] = "Boxing Day"
        for k, v in list(self.items()):
            if self.observed and k.weekday() == SUN:
                self[k + rd(days=1)] = v + " (Observed)"

        self[easter(year) - rd(weekday=FR(-1))] = "Good Friday"
        self[easter(year) + rd(weekday=MO(+1))] = "Easter Monday"


class KE(Kenya):
    pass


class KEN(Kenya):
    pass
