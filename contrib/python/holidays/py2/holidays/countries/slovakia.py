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

from holidays.constants import JAN, MAY, JUL, AUG, SEP, OCT, \
    NOV, DEC
from holidays.holiday_base import HolidayBase


class Slovakia(HolidayBase):
    # https://sk.wikipedia.org/wiki/Sviatok
    # https://www.slov-lex.sk/pravne-predpisy/SK/ZZ/1993/241/20181011.html

    def __init__(self, **kwargs):
        self.country = 'SK'
        HolidayBase.__init__(self, **kwargs)

    def _populate(self, year):
        self[date(year, JAN, 1)] = "Deň vzniku Slovenskej republiky"
        self[date(year, JAN, 6)] = "Zjavenie Pána (Traja králi a" \
                                   " vianočnýsviatok pravoslávnych" \
                                   " kresťanov)"

        e = easter(year)
        self[e - rd(days=2)] = "Veľký piatok"
        self[e + rd(days=1)] = "Veľkonočný pondelok"

        self[date(year, MAY, 1)] = "Sviatok práce"

        if year >= 1997:
            self[date(year, MAY, 8)] = "Deň víťazstva nad fašizmom"

        self[date(year, JUL, 5)] = "Sviatok svätého Cyrila a svätého Metoda"

        self[date(year, AUG, 29)] = "Výročie Slovenského národného" \
                                    " povstania"

        self[date(year, SEP, 1)] = "Deň Ústavy Slovenskej republiky"

        self[date(year, SEP, 15)] = "Sedembolestná Panna Mária"
        if year == 2018:
            self[date(year, OCT, 30)] = "100. výročie prijatia" \
                " Deklarácie slovenského národa"
        self[date(year, NOV, 1)] = "Sviatok Všetkých svätých"

        if year >= 2001:
            self[date(year, NOV, 17)] = "Deň boja za slobodu a demokraciu"

        self[date(year, DEC, 24)] = "Štedrý deň"

        self[date(year, DEC, 25)] = "Prvý sviatok vianočný"

        self[date(year, DEC, 26)] = "Druhý sviatok vianočný"


class SK(Slovakia):
    pass


class SVK(Slovakia):
    pass


class Slovak(Slovakia):
    def __init__(self, **kwargs):
        warnings.warn("Slovak is deprecated, use Slovakia instead.",
                      DeprecationWarning)
        super(Slovak, self).__init__(**kwargs)
