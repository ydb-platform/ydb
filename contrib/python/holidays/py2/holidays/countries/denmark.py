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
from dateutil.relativedelta import relativedelta as rd, SU, TH, FR, MO

from holidays.constants import JAN, DEC
from holidays.holiday_base import HolidayBase


class Denmark(HolidayBase):
    # https://en.wikipedia.org/wiki/Public_holidays_in_Denmark

    def __init__(self, **kwargs):
        self.country = 'DK'
        HolidayBase.__init__(self, **kwargs)

    def _populate(self, year):
        # Public holidays
        self[date(year, JAN, 1)] = "Nytårsdag"
        self[easter(year) + rd(weekday=SU(-2))] = "Palmesøndag"
        self[easter(year) + rd(weekday=TH(-1))] = "Skærtorsdag"
        self[easter(year) + rd(weekday=FR(-1))] = "Langfredag"
        self[easter(year)] = "Påskedag"
        self[easter(year) + rd(weekday=MO)] = "Anden påskedag"
        self[easter(year) + rd(weekday=FR(+4))] = "Store bededag"
        self[easter(year) + rd(days=39)] = "Kristi himmelfartsdag"
        self[easter(year) + rd(days=49)] = "Pinsedag"
        self[easter(year) + rd(days=50)] = "Anden pinsedag"
        self[date(year, DEC, 25)] = "Juledag"
        self[date(year, DEC, 26)] = "Anden juledag"


class DK(Denmark):
    pass


class DNK(Denmark):
    pass
