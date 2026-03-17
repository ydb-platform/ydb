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
from holidays.constants import SUN
from holidays.constants import JAN, FEB, APR, MAY, JUL, AUG, OCT, NOV, DEC
from holidays.holiday_base import HolidayBase
from holidays.utils import get_gre_date


class Burundi(HolidayBase):
    """
    Burundian holidays
    Note that holidays falling on a sunday maybe observed
    on the following Monday.
    This depends on formal annoucemnts by the government,
    which only happens close to the date of the holiday.

    Primary sources:
    https://www.officeholidays.com/countries/burundi
    """

    def __init__(self, **kwargs):
        self.country = 'BI'
        HolidayBase.__init__(self, **kwargs)

    def _populate(self, year):
        # New Year's Day
        self[date(year, JAN, 1)] = "New Year's Day"

        # Unity Day
        name = "Unity Day"
        self[date(year, FEB, 5)] = name
        if date(year, FEB, 5).weekday() == SUN:
            self[date(year, FEB, 6)] = name + " (Observed)"

        # President Ntaryamira Day
        name = "President Ntaryamira Day"
        self[date(year, APR, 6)] = "President Ntaryamira Day"
        if date(year, APR, 6).weekday() == SUN:
            self[date(year, APR, 7)] = name + " (Observed)"

        # Labour Day
        name = "Labour Day"
        self[date(year, MAY, 1)] = name
        if date(year, MAY, 1).weekday() == SUN:
            self[date(year, MAY, 2)] = name + " (Observed)"

        # Ascension Day
        name = "Ascension Day"
        self[easter(year) + rd(days=+39)] = name

        # Independence Day post 1962
        name = "Independence Day"
        if year > 1961:
            self[date(year, JUL, 1)] = name
            if date(year, JUL, 1).weekday() == SUN:
                self[date(year, JUL, 2)] = name + " (Observed)"

        # Eid Al Adha- Feast of the Sacrifice
        # date of observance is announced yearly
        for date_obs in get_gre_date(year, 12, 10):
            hol_date = date_obs
            self[hol_date] = "Eid Al Adha"
            self[hol_date + rd(days=1)] = "Eid Al Adha"

        # Assumption Day
        name = 'Assumption Day'
        self[date(year, AUG, 15)] = name

        # Prince Louis Rwagasore Day
        name = "Prince Louis Rwagasore Day"
        self[date(year, OCT, 13)] = name
        if date(year, OCT, 13).weekday() == SUN:
            self[date(year, OCT, 14)] = name + " (Observed)"

        # President Ndadaye's Day
        name = "President Ndadaye's Day"
        self[date(year, OCT, 21)] = name
        if date(year, OCT, 21).weekday() == SUN:
            self[date(year, OCT, 22)] = name + " (Observed)"

        # All Saints' Day
        name = "All Saints' Day"
        self[date(year, NOV, 1)] = name
        if date(year, NOV, 1).weekday() == SUN:
            self[date(year, NOV, 2)] = name + " (Observed)"

        # Christmas Day
        self[date(year, DEC, 25)] = "Christmas Day"


class BI(Burundi):
    pass


class BDI(Burundi):
    pass
