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
from dateutil.relativedelta import relativedelta as rd, MO

from holidays.constants import JAN, MAY, AUG, OCT, \
    NOV, DEC
from holidays.holiday_base import HolidayBase


class Austria(HolidayBase):
    PROVINCES = ['1', '2', '3', '4', '5', '6', '7', '8', '9']

    def __init__(self, **kwargs):
        self.country = 'AT'
        self.prov = kwargs.pop('prov', kwargs.pop('state', '9'))
        HolidayBase.__init__(self, **kwargs)

    def _populate(self, year):
        # public holidays
        self[date(year, JAN, 1)] = "Neujahr"
        self[date(year, JAN, 6)] = "Heilige Drei Könige"
        self[easter(year) + rd(weekday=MO)] = "Ostermontag"
        self[date(year, MAY, 1)] = "Staatsfeiertag"
        self[easter(year) + rd(days=39)] = "Christi Himmelfahrt"
        self[easter(year) + rd(days=50)] = "Pfingstmontag"
        self[easter(year) + rd(days=60)] = "Fronleichnam"
        self[date(year, AUG, 15)] = "Mariä Himmelfahrt"
        if 1919 <= year <= 1934:
            self[date(year, NOV, 12)] = "Nationalfeiertag"
        if year >= 1967:
            self[date(year, OCT, 26)] = "Nationalfeiertag"
        self[date(year, NOV, 1)] = "Allerheiligen"
        self[date(year, DEC, 8)] = "Mariä Empfängnis"
        self[date(year, DEC, 25)] = "Christtag"
        self[date(year, DEC, 26)] = "Stefanitag"


class AT(Austria):
    pass


class AUT(Austria):
    pass
