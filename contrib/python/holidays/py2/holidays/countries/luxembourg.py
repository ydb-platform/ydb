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

from holidays.constants import JAN, MAY, JUN, AUG, NOV, DEC
from holidays.holiday_base import HolidayBase


class Luxembourg(HolidayBase):

    # https://en.wikipedia.org/wiki/Public_holidays_in_Luxembourg

    def __init__(self, **kwargs):
        self.country = 'LU'
        HolidayBase.__init__(self, **kwargs)

    def _populate(self, year):
        # Public holidays
        self[date(year, JAN, 1)] = "Neijoerschdag"
        self[easter(year) + rd(weekday=MO)] = "Ouschterméindeg"
        self[date(year, MAY, 1)] = "Dag vun der Aarbecht"
        if year >= 2019:
            # Europe Day: not in legislation yet, but introduced starting 2019
            self[date(year, MAY, 9)] = "Europadag"
        self[easter(year) + rd(days=39)] = "Christi Himmelfaart"
        self[easter(year) + rd(days=50)] = "Péngschtméindeg"
        self[date(year, JUN, 23)] = "Nationalfeierdag"
        self[date(year, AUG, 15)] = "Léiffrawëschdag"
        self[date(year, NOV, 1)] = "Allerhellgen"
        self[date(year, DEC, 25)] = "Chrëschtdag"
        self[date(year, DEC, 26)] = "Stiefesdag"


class LU(Luxembourg):
    pass


class LUX(Luxembourg):
    pass
