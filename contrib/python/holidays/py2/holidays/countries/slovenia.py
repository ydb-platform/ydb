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

from holidays.constants import JAN, FEB, APR, MAY, JUN, AUG, OCT, \
    NOV, DEC
from holidays.holiday_base import HolidayBase


class Slovenia(HolidayBase):
    """
    Contains all work-free public holidays in Slovenia.
    No holidays are returned before year 1991 when Slovenia became independent
    country. Before that Slovenia was part of Socialist federal republic of
    Yugoslavia.

    List of holidays (including those that are not work-free:
    https://en.wikipedia.org/wiki/Public_holidays_in_Slovenia
    """

    def __init__(self, **kwargs):
        self.country = 'SI'
        HolidayBase.__init__(self, **kwargs)

    def _populate(self, year):
        if year <= 1990:
            return

        if year > 1991:
            self[date(year, JAN, 1)] = "novo leto"

            # Between 2012 and 2017 2nd January was not public holiday,
            # or at least not work-free day
            if year < 2013 or year > 2016:
                self[date(year, JAN, 2)] = "novo leto"

            # Prešeren's day, slovenian cultural holiday
            self[date(year, FEB, 8)] = "Prešernov dan"

            # Easter monday is the only easter related work-free day
            easter_day = easter(year)
            self[easter_day + rd(days=1)] = "Velikonočni ponedeljek"

            # Day of uprising against occupation
            self[date(year, APR, 27)] = "dan upora proti okupatorju"

            # Labour day, two days of it!
            self[date(year, MAY, 1)] = "praznik dela"
            self[date(year, MAY, 2)] = "praznik dela"

            # Statehood day
            self[date(year, JUN, 25)] = "dan državnosti"

            # Assumption day
            self[date(year, AUG, 15)] = "Marijino vnebovzetje"

            # Reformation day
            self[date(year, OCT, 31)] = "dan reformacije"

            # Remembrance day
            self[date(year, NOV, 1)] = "dan spomina na mrtve"

            # Christmas
            self[date(year, DEC, 25)] = "Božič"

            # Day of independence and unity
            self[date(year, DEC, 26)] = "dan samostojnosti in enotnosti"


class SI(Slovenia):
    pass


class SVN(Slovenia):
    pass
