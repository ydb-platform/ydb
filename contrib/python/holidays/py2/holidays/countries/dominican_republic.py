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
from dateutil.relativedelta import relativedelta as rd, MO, FR

from holidays.constants import JAN, FEB, MAY, JUN, AUG, SEP, NOV, DEC
from holidays.holiday_base import HolidayBase


class DominicanRepublic(HolidayBase):
    # http://ojd.org.do/Normativas/LABORAL/Leyes/Ley%20No.%20%20139-97.pdf
    # https://es.wikipedia.org/wiki/Rep%C3%BAblica_Dominicana#D%C3%ADas_festivos_nacionales

    def __init__(self, **kwargs):
        self.country = 'DO'
        HolidayBase.__init__(self, **kwargs)

    @staticmethod
    def __change_day_by_law(holiday, latest_days=(3, 4)):
        # Law No. 139-97 - Holidays Dominican Republic - Jun 27, 1997
        if holiday >= date(1997, 6, 27):
            if holiday.weekday() in [1, 2]:
                holiday -= rd(weekday=MO(-1))
            elif holiday.weekday() in latest_days:
                holiday += rd(weekday=MO(1))
        return holiday

    def _populate(self, year):
        # New Year's Day
        self[date(year, JAN, 1)] = "Año Nuevo [New Year's Day]"

        # Epiphany
        epiphany_day = self.__change_day_by_law(date(year, JAN, 6))
        self[epiphany_day] = "Día de los Santos Reyes [Epiphany]"

        # Lady of Altagracia
        self[date(year, JAN, 21)] = "Día de la Altagracia [Lady of Altagracia]"

        # Juan Pablo Duarte Day
        duarte_day = self.__change_day_by_law(date(year, JAN, 26))
        self[duarte_day] = "Día de Duarte [Juan Pablo Duarte Day]"

        # Independence Day
        self[date(year, FEB, 27)] = "Día de Independencia [Independence Day]"

        # Good Friday
        self[easter(year) + rd(weekday=FR(-1))] = "Viernes Santo [Good Friday]"

        # Labor Day
        labor_day = self.__change_day_by_law(date(year, MAY, 1), (3, 4, 6))
        self[labor_day] = "Día del Trabajo [Labor Day]"

        # Feast of Corpus Christi
        self[date(year, JUN, 11)] = "Corpus Christi [Feast of Corpus Christi]"

        # Restoration Day
        # Judgment No. 14 of Feb 20, 2008 of the Supreme Court of Justice
        restoration_day = date(year, AUG, 16) if ((year - 2000) % 4 == 0) \
            and year < 2008 else self.__change_day_by_law(date(year, AUG, 16))
        self[restoration_day] = "Día de la Restauración [Restoration Day]"

        # Our Lady of Mercedes Day
        self[date(year, SEP, 24)] = "Día de las Mercedes \
            [Our Lady of Mercedes Day]"

        # Constitution Day
        constitution_day = self.__change_day_by_law(date(year, NOV, 6))
        self[constitution_day] = "Día de la Constitución [Constitution Day]"

        # Christmas Day
        self[date(year, DEC, 25)] = "Día de Navidad [Christmas Day]"


class DO(DominicanRepublic):
    pass


class DOM(DominicanRepublic):
    pass
