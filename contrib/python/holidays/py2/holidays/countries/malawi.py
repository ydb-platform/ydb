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
from dateutil.relativedelta import relativedelta as rd

from holidays.constants import TUE, SAT, SUN
from holidays.constants import JAN, FEB, MAR, APR, MAY, JUL, SEP, OCT, NOV, DEC
from holidays.holiday_base import HolidayBase


class Malawi(HolidayBase):

    def __init__(self, **kwargs):
        # https://www.officeholidays.com/countries/malawi
        # https://www.timeanddate.com/holidays/malawi/
        self.country = 'MW'
        HolidayBase.__init__(self, **kwargs)

    def _populate(self, year):
        # Observed since 2000
        if year > 1999:
            self[date(year, 1, 1)] = "New Year's Day"

            e = easter(year)
            good_friday = e - rd(days=2)
            easter_monday = e + rd(days=1)
            self[good_friday] = "Good Friday"
            self[easter_monday] = "Easter Monday"

            self[date(year, JAN, 15)] = "John Chilembwe Day"
            self[date(year, MAR, 3)] = "Martyrs Day"
            self[date(year, MAY, 1)] = "Labour Day"
            self[date(year, MAY, 14)] = "Kamuzu Day"
            self[date(year, JUL, 6)] = "Independence Day"
            self[date(year, OCT, 15)] = "Mother's Day"
            self[date(year, DEC, 25)] = "Christmas Day"
            self[date(year, DEC, 26)] = "Boxing Day"

        for k, v in list(self.items()):
            if self.observed and year > 1994:
                if k.weekday() == SUN:
                    self[k + rd(days=1)] = v + " (Observed)"
                elif k.weekday() == SAT:
                    self[k + rd(days=2)] = v + " (Observed)"


class MW(Malawi):
    pass


class MWI(Malawi):
    pass
