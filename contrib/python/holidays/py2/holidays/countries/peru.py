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
from dateutil.relativedelta import relativedelta as rd, TH, FR, SA, SU

from holidays.constants import JAN, MAY, JUN, JUL, AUG, OCT, \
    NOV, DEC
from holidays.holiday_base import HolidayBase


class Peru(HolidayBase):
    # https://www.gob.pe/feriados
    # https://es.wikipedia.org/wiki/Anexo:Días_feriados_en_el_Perú
    def __init__(self, **kwargs):
        self.country = "PE"
        HolidayBase.__init__(self, **kwargs)

    def _populate(self, year):
        # New Year's Day
        self[date(year, JAN, 1)] = "Año Nuevo [New Year's Day]"

        # Feast of Saints Peter and Paul
        name = "San Pedro y San Pablo [Feast of Saints Peter and Paul]"
        self[date(year, JUN, 29)] = name

        # Independence Day
        name = "Día de la Independencia [Independence Day]"
        self[date(year, JUL, 28)] = name

        name = "Día de las Fuerzas Armadas y la Policía del Perú"
        self[date(year, JUL, 29)] = name

        # Santa Rosa de Lima
        name = "Día de Santa Rosa de Lima"
        self[date(year, AUG, 30)] = name

        # Battle of Angamos
        name = "Combate Naval de Angamos [Battle of Angamos]"
        self[date(year, OCT, 8)] = name

        # Holy Thursday
        self[easter(year) + rd(weekday=TH(-1))
             ] = "Jueves Santo [Maundy Thursday]"

        # Good Friday
        self[easter(year) + rd(weekday=FR(-1))
             ] = "Viernes Santo [Good Friday]"

        # Holy Saturday
        self[easter(year) + rd(weekday=SA(-1))
             ] = "Sábado de Gloria [Holy Saturday]"

        # Easter Sunday
        self[easter(year) + rd(weekday=SU(-1))
             ] = "Domingo de Resurrección [Easter Sunday]"

        # Labor Day
        self[date(year, MAY, 1)] = "Día del Trabajo [Labour Day]"

        # All Saints Day
        name = "Día de Todos Los Santos [All Saints Day]"
        self[date(year, NOV, 1)] = name

        # Inmaculada Concepción
        name = "Inmaculada Concepción [Immaculate Conception]"
        self[date(year, DEC, 8)] = name

        # Christmas
        self[date(year, DEC, 25)] = "Navidad [Christmas]"


class PE(Peru):
    pass


class PER(Peru):
    pass
