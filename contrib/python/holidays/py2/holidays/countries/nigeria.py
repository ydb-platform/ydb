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

from holidays.constants import JAN, MAY, JUN, OCT, \
    DEC
from holidays.holiday_base import HolidayBase


class Nigeria(HolidayBase):
    # https://en.wikipedia.org/wiki/Public_holidays_in_Nigeria
    def __init__(self, **kwargs):
        self.country = "NG"
        HolidayBase.__init__(self, **kwargs)

    def _populate(self, year):
        # New Year's Day
        self[date(year, JAN, 1)] = "New Year's day"

        # Worker's day
        self[date(year, MAY, 1)] = "Worker's day"

        # Children's day
        self[date(year, MAY, 27)] = "Children's day"

        # Democracy day
        self[date(year, JUN, 12)] = "Democracy day"

        # Independence Day
        self[date(year, OCT, 1)] = "Independence day"

        # Christmas day
        self[date(year, DEC, 25)] = "Christmas day"

        # Boxing day
        self[date(year, DEC, 26)] = "Boxing day"


class NG(Nigeria):
    pass


class NGA(Nigeria):
    pass
