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

from dateutil.relativedelta import relativedelta as rd, MO

from holidays.constants import FRI, SAT, SUN
from holidays.constants import JAN, FEB, MAR, MAY, SEP, NOV, DEC
from holidays.holiday_base import HolidayBase


class Mexico(HolidayBase):

    def __init__(self, **kwargs):
        self.country = 'MX'
        HolidayBase.__init__(self, **kwargs)

    def _populate(self, year):
        # New Year's Day
        name = "Año Nuevo [New Year's Day]"
        self[date(year, JAN, 1)] = name
        if self.observed and date(year, JAN, 1).weekday() == SUN:
            self[date(year, JAN, 1) + rd(days=+1)] = name + " (Observed)"
        # The next year's observed New Year's Day can be in this year
        # when it falls on a Friday (Jan 1st is a Saturday)
        if self.observed and date(year, DEC, 31).weekday() == FRI:
            self[date(year, DEC, 31)] = name + " (Observed)"

        # Constitution Day
        name = "Día de la Constitución [Constitution Day]"
        if self.observed and year >= 2007:
            self[date(year, FEB, 1) + rd(weekday=MO(+1))] = \
                name + " (Observed)"

        if year >= 1917:
            self[date(year, FEB, 5)] = name

        # Benito Juárez's birthday
        name = "Natalicio de Benito Juárez [Benito Juárez's birthday]"
        if self.observed and year >= 2007:
            self[date(year, MAR, 1) + rd(weekday=MO(+3))] = \
                name + " (Observed)"

        if year >= 1917:
            self[date(year, MAR, 21)] = name

        # Labor Day
        if year >= 1923:
            name = "Día del Trabajo [Labour Day]"
            self[date(year, MAY, 1)] = name
            if self.observed and date(year, MAY, 1).weekday() == SAT:
                self[date(year, MAY, 1) + rd(days=-1)] = name + " (Observed)"
            elif self.observed and date(year, MAY, 1).weekday() == SUN:
                self[date(year, MAY, 1) + rd(days=+1)] = name + " (Observed)"

        # Independence Day
        name = "Día de la Independencia [Independence Day]"
        self[date(year, SEP, 16)] = name
        if self.observed and date(year, SEP, 16).weekday() == SAT:
            self[date(year, SEP, 16) + rd(days=-1)] = name + " (Observed)"
        elif self.observed and date(year, SEP, 16).weekday() == SUN:
            self[date(year, SEP, 16) + rd(days=+1)] = name + " (Observed)"

        # Revolution Day
        name = "Día de la Revolución [Revolution Day]"
        if self.observed and year >= 2007:
            self[date(year, NOV, 1) + rd(weekday=MO(+3))] = \
                name + " (Observed)"

        if year >= 1917:
            self[date(year, NOV, 20)] = name

        # Change of Federal Government
        # Every six years--next observance 2018
        name = "Transmisión del Poder Ejecutivo Federal"
        name += " [Change of Federal Government]"
        if year >= 1970 and (2096 - year) % 6 == 0:
            self[date(year, DEC, 1)] = name
            if self.observed and date(year, DEC, 1).weekday() == SAT:
                self[date(year, DEC, 1) + rd(days=-1)] = name + " (Observed)"
            elif self.observed and date(year, DEC, 1).weekday() == SUN:
                self[date(year, DEC, 1) + rd(days=+1)] = name + " (Observed)"

        # Christmas
        self[date(year, DEC, 25)] = "Navidad [Christmas]"
        if self.observed and date(year, DEC, 25).weekday() == SAT:
            self[date(year, DEC, 25) + rd(days=-1)] = name + " (Observed)"
        elif self.observed and date(year, DEC, 25).weekday() == SUN:
            self[date(year, DEC, 25) + rd(days=+1)] = name + " (Observed)"


class MX(Mexico):
    pass


class MEX(Mexico):
    pass
