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

from holidays.constants import JAN, FEB, MAR, MAY, JUN, NOV
from holidays.holiday_base import HolidayBase


class Russia(HolidayBase):
    """
    https://en.wikipedia.org/wiki/Public_holidays_in_Russia
    """

    def __init__(self, **kwargs):
        self.country = "RU"
        HolidayBase.__init__(self, **kwargs)

    def _populate(self, year):
        # New Year's Day
        self[date(year, JAN, 1)] = "Новый год"
        # New Year's Day
        self[date(year, JAN, 2)] = "Новый год"
        # New Year's Day
        self[date(year, JAN, 3)] = "Новый год"
        # New Year's Day
        self[date(year, JAN, 4)] = "Новый год"
        # New Year's Day
        self[date(year, JAN, 5)] = "Новый год"
        # New Year's Day
        self[date(year, JAN, 6)] = "Новый год"
        # Christmas Day (Orthodox)
        self[date(year, JAN, 7)] = "Православное Рождество"
        # New Year's Day
        self[date(year, JAN, 8)] = "Новый год"
        # Man Day
        self[date(year, FEB, 23)] = "День защитника отечества"
        # Women's Day
        self[date(year, MAR, 8)] = "День женщин"
        # Labour Day
        self[date(year, MAY, 1)] = "Праздник Весны и Труда"
        # Victory Day
        self[date(year, MAY, 9)] = "День Победы"
        # Russia's Day
        self[date(year, JUN, 12)] = "День России"
        if year >= 2005:
            # Unity Day
            self[date(year, NOV, 4)] = "День народного единства"
        else:
            # October Revolution Day
            self[date(year, NOV, 7)] = "День Октябрьской революции"


class RU(Russia):
    pass


class RUS(Russia):
    pass
