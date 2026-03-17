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
from dateutil.relativedelta import relativedelta as rd, TH, FR

from holidays.constants import JAN, MAY, JUL, AUG, SEP, DEC
from holidays.holiday_base import HolidayBase


class Nicaragua(HolidayBase):
    PROVINCES = ['MN']

    def __init__(self, **kwargs):
        self.country = 'NI'
        self.prov = kwargs.pop('prov', kwargs.pop('state', 'MN'))
        HolidayBase.__init__(self, **kwargs)

    def _populate(self, year):
        # New Years
        self[date(year, JAN, 1)] = "Año Nuevo [New Year's Day]"
        # Maundy Thursday
        self[easter(year) + rd(weekday=TH(-1))] =\
            "Jueves Santo [Maundy Thursday]"
        # Good Friday
        self[easter(year) + rd(weekday=FR(-1))] = "Viernes Santo [Good Friday]"
        # Labor Day
        self[date(year, MAY, 1)] = "Día del Trabajo [Labour Day]"
        # Revolution Day
        if 2020 >= year >= 1979:
            self[date(year, JUL, 19)] = "Día de la Revolución [Revolution Day]"
        # Battle of San Jacinto Day
        self[date(year, SEP, 14)] =\
            "Batalla de San Jacinto [Battle of San Jacinto]"
        # Independence Day
        self[date(year, SEP, 15)] =\
            "Día de la Independencia [Independence Day]"
        # Virgin's Day
        self[date(year, DEC, 8)] = "Concepción de María [Virgin's Day]"
        # Christmas
        self[date(year, DEC, 25)] = "Navidad [Christmas]"

        # Provinces festive day
        if self.prov:
            if self.prov == 'MN':
                # Santo Domingo Day Down
                self[date(year, AUG, 1)] = "Bajada de Santo Domingo"
                # Santo Domingo Day Up
                self[date(year, AUG, 10)] = "Subida de Santo Domingo"


class NI(Nicaragua):
    pass


class NIC(Nicaragua):
    pass
