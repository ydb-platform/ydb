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

from datetime import date, timedelta

from dateutil.easter import easter

from holidays.constants import JAN, MAY, JUN, AUG, OCT, NOV, DEC
from holidays.holiday_base import HolidayBase


class Croatia(HolidayBase):

    # Updated with act 022-03 / 19-01 / 219 of 14 November 2019
    # https://narodne-novine.nn.hr/clanci/sluzbeni/2019_11_110_2212.html
    # https://en.wikipedia.org/wiki/Public_holidays_in_Croatia

    def __init__(self, **kwargs):
        self.country = "HR"
        HolidayBase.__init__(self, **kwargs)

    def _populate(self, year):
        # New years
        self[date(year, JAN, 1)] = "Nova Godina"

        # Epiphany
        self[date(year, JAN, 6)] = "Sveta tri kralja"
        easter_date = easter(year)

        # Easter
        self[easter_date] = "Uskrs"
        # Easter Monday
        self[easter_date + timedelta(days=1)] = "Uskrsni ponedjeljak"

        # Corpus Christi
        self[easter_date + timedelta(days=60)] = "Tijelovo"

        # International Workers' Day
        self[date(year, MAY, 1)] = "Međunarodni praznik rada"

        # Statehood day (new)
        if year >= 2020:
            self[date(year, MAY, 30)] = "Dan državnosti"

        # Anti-fascist struggle day
        self[date(year, JUN, 22)] = "Dan antifašističke borbe"

        # Statehood day (old)
        if year < 2020:
            self[date(year, JUN, 25)] = "Dan državnosti"

        # Victory and Homeland Thanksgiving Day
        self[date(year, AUG, 5)] = "Dan pobjede i domovinske zahvalnosti"

        # Assumption of Mary
        self[date(year, AUG, 15)] = "Velika Gospa"

        # Independence Day (old)
        if year < 2020:
            self[date(year, OCT, 8)] = "Dan neovisnosti"

        # All Saints' Day
        self[date(year, NOV, 1)] = "Svi sveti"

        if year >= 2020:
            # Memorial day
            self[date(year, NOV, 18)] = "Dan sjećanja"

        # Christmas day
        self[date(year, DEC, 25)] = "Božić"

        # St. Stephen's day
        self[date(year, DEC, 26)] = "Sveti Stjepan"


class HR(Croatia):
    pass


class HRV(Croatia):
    pass
