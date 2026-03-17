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

import warnings
from datetime import date

from dateutil.easter import easter
from dateutil.relativedelta import relativedelta as rd

from holidays.constants import JAN, MAY, AUG, NOV, DEC
from holidays.holiday_base import HolidayBase


class Poland(HolidayBase):
    # https://pl.wikipedia.org/wiki/Dni_wolne_od_pracy_w_Polsce

    def __init__(self, **kwargs):
        self.country = 'PL'
        HolidayBase.__init__(self, **kwargs)

    def _populate(self, year):
        self[date(year, JAN, 1)] = 'Nowy Rok'
        if year >= 2011:
            self[date(year, JAN, 6)] = 'Święto Trzech Króli'

        e = easter(year)
        self[e] = 'Niedziela Wielkanocna'
        self[e + rd(days=1)] = 'Poniedziałek Wielkanocny'

        if year >= 1950:
            self[date(year, MAY, 1)] = 'Święto Państwowe'
        if year >= 1919:
            self[date(year, MAY, 3)] = 'Święto Narodowe Trzeciego Maja'

        self[e + rd(days=49)] = 'Zielone Świątki'
        self[e + rd(days=60)] = 'Dzień Bożego Ciała'

        self[date(year, AUG, 15)] = 'Wniebowzięcie Najświętszej Marii Panny'

        self[date(year, NOV, 1)] = 'Uroczystość Wszystkich świętych'
        if (1937 <= year <= 1945) or year >= 1989:
            self[date(year, NOV, 11)] = 'Narodowe Święto Niepodległości'

        self[date(year, DEC, 25)] = 'Boże Narodzenie (pierwszy dzień)'
        self[date(year, DEC, 26)] = 'Boże Narodzenie (drugi dzień)'


class PL(Poland):
    pass


class POL(Poland):
    pass


class Polish(Poland):
    def __init__(self, **kwargs):
        warnings.warn("Polish is deprecated, use Poland instead.",
                      DeprecationWarning)
        super(Polish, self).__init__(**kwargs)
