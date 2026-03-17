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
from dateutil.relativedelta import relativedelta as rd, FR, TH, MO

from holidays.constants import JAN, APR, MAY, JUN, AUG, DEC
from holidays.holiday_base import HolidayBase


class Iceland(HolidayBase):
    # https://en.wikipedia.org/wiki/Public_holidays_in_Iceland
    # https://www.officeholidays.com/countries/iceland/index.php
    def __init__(self, **kwargs):
        self.country = "IS"
        HolidayBase.__init__(self, **kwargs)

    def _populate(self, year):
        # Public holidays
        self[date(year, JAN, 1)] = "Nýársdagur"
        self[easter(year) - rd(days=3)] = "Skírdagur"
        self[easter(year) + rd(weekday=FR(-1))] = "Föstudagurinn langi"
        self[easter(year)] = "Páskadagur"
        self[easter(year) + rd(days=1)] = "Annar í páskum"
        self[date(year, APR, 19) + rd(weekday=TH(+1))] = \
            "Sumardagurinn fyrsti"
        self[date(year, MAY, 1)] = "Verkalýðsdagurinn"
        self[easter(year) + rd(days=39)] = "Uppstigningardagur"
        self[easter(year) + rd(days=49)] = "Hvítasunnudagur"
        self[easter(year) + rd(days=50)] = "Annar í hvítasunnu"
        self[date(year, JUN, 17)] = "Þjóðhátíðardagurinn"
        # First Monday of August
        self[date(year, AUG, 1) + rd(weekday=MO(+1))] = \
            "Frídagur verslunarmanna"
        self[date(year, DEC, 24)] = "Aðfangadagur"
        self[date(year, DEC, 25)] = "Jóladagur"
        self[date(year, DEC, 26)] = "Annar í jólum"
        self[date(year, DEC, 31)] = "Gamlársdagur"


class IS(Iceland):
    pass


class ISL(Iceland):
    pass
