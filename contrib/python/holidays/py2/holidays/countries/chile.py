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
from dateutil.relativedelta import relativedelta as rd, MO, FR, SA

from holidays.constants import JAN, MAY, JUN, JUL, AUG, SEP, OCT, \
    NOV, DEC
from holidays.constants import TUE, THU, FRI, SAT, SUN
from holidays.holiday_base import HolidayBase


class Chile(HolidayBase):
    # https://www.feriados.cl
    # http://www.feriadoschilenos.cl/ (excellent history)
    # https://es.wikipedia.org/wiki/Anexo:D%C3%ADas_feriados_en_Chile

    # ISO 3166-2 codes for the principal subdivisions, called regions
    STATES = ['AI', 'AN', 'AP', 'AR', 'AT', 'BI', 'CO', 'LI', 'LL', 'LR',
              'MA', 'ML', 'NB', 'RM', 'TA', 'VS']

    def __init__(self, **kwargs):
        self.country = 'CL'
        HolidayBase.__init__(self, **kwargs)

    def _populate(self, year):
        # New Year's Day (Law 2.977)
        self[date(year, JAN, 1)] = "Año Nuevo [New Year's Day]"
        # Day after, if it's a Sunday (Law 20.983)
        if year > 2016 and date(year, JAN, 1).weekday() == SUN:
            self[date(year, JAN, 2)] = "Fiestas Patrias [Holiday]"

        # Holy Week (Law 2.977)
        name_fri = "Semana Santa (Viernes Santo) [Good Friday)]"
        name_sat = "Semana Santa (Sábado Santo) [Good Saturday)]"
        name_easter = 'Día de Pascuas [Easter Day]'

        self[easter(year) + rd(weekday=FR(-1))] = name_fri
        self[easter(year) + rd(weekday=SA(-1))] = name_sat
        self[easter(year)] = name_easter

        # Labor Day (Law 2.200, renamed with Law 18.018)
        name = "Día Nacional del Trabajo [Labour Day]"
        self[date(year, MAY, 1)] = name

        # Naval Glories Day (Law 2.977)
        name = "Día de las Glorias Navales [Navy Day]"
        self[date(year, MAY, 21)] = name

        # Saint Peter and Saint Paul (Law 18.432)
        name = "San Pedro y San Pablo [Saint Peter and Saint Paul]"
        if year < 2020:
            self[date(year, JUN, 29)] = name
        else:
            # floating Monday holiday (Law 19.668)
            if date(year, JUN, 29).weekday() <= THU:
                self[date(year, JUN, 29) + rd(date(year, JUN, 29),
                                              weekday=MO(-1))] = name
            elif date(year, JUN, 29).weekday() == FRI:
                self[date(year, JUN, 29) + rd(weekday=MO)] = name
            else:
                self[date(year, JUN, 29)] = name

        # Day of Virgin of Carmen (Law 20.148)
        if year > 2006:
            name = "Virgen del Carmen [Our Lady of Mount Carmel]"
            self[date(year, JUL, 16)] = name

        # Day of Assumption of the Virgin (Law 2.977)
        name = "Asunción de la Virgen [Assumption of Mary]"
        self[date(year, AUG, 15)] = name

        # National Holiday Friday preceding Independence Day (Law 20.983)
        if year > 2016 and date(year, SEP, 18).weekday() == SAT:
            self[date(year, SEP, 17)] = "Fiestas Patrias [Holiday]"

        # National Holiday Monday preceding Independence Day (Law 20.215)
        if year > 2007 and date(year, SEP, 18).weekday() == TUE:
            self[date(year, SEP, 17)] = "Fiestas Patrias [Holiday]"

        # Independence Day (Law 2.977)
        name = "Día de la Independencia [Independence Day]"
        self[date(year, SEP, 18)] = name

        # Day of Glories of the Army of Chile (Law 2.977)
        name = "Día de las Glorias del Ejército [Army Day]"
        self[date(year, SEP, 19)] = name

        # National Holiday Friday following Army Day (Law 20.215)
        if year > 2007 and date(year, SEP, 19).weekday() == THU:
            self[date(year, SEP, 20)] = "Fiestas Patrias [Holiday]"

        # Day of the Meeting of Two Worlds (Law 3.810)
        if year < 2010:
            self[date(year, OCT, 12)] = "Día de la Raza [Columbus day]"
        elif year < 2020:
            self[date(year, OCT, 12)] = "Día del Respeto a la Diversidad"\
                                        " [Day of the Meeting " \
                                        " of Two Worlds]"
        else:
            # floating Monday holiday (Law 19.668)
            name = ("Día del Descubrimiento de dos Mundos [Columbus Day]")
            if date(year, OCT, 12).weekday() <= THU:
                self[date(year, OCT, 12) + rd(date(year, OCT, 12),
                                              weekday=MO(-1))] = name
            elif date(year, OCT, 12).weekday() == FRI:
                self[date(year, OCT, 12) + rd(weekday=MO)] = name
            else:
                self[date(year, OCT, 12)] = name

        # National Day of the Evangelical and Protestant Churches (Law 20.299)
        if year > 2007:
            name = ("Día Nacional de las Iglesias Evangélicas y Protestantes "
                    " [Reformation Day]")
            self[date(year, OCT, 31)] = name

        # All Saints Day (Law 2.977)
        name = "Día de Todos los Santos [All Saints Day]"
        self[date(year, NOV, 1)] = name

        # Immaculate Conception (Law 2.977)
        self[date(year, DEC, 8)] = "La Inmaculada Concepción" \
                                   " [Immaculate Conception]"

        # Christmas (Law 2.977)
        self[date(year, DEC, 25)] = "Navidad [Christmas]"

        # región de Arica y Parinacota
        if self.state == 'AP' and year >= 2020:
            # Law 20.663
            self[date(year, JUN, 7)] = ("Asalto y Toma del Morro de Arica"
                                        " [Assault and Capture of Cape Arica]")

        # región de Ñuble
        if self.state == 'NB' and year >= 2014:
            # Law 20.678
            self[date(year, AUG, 20)] =\
                ("Nacimiento del Prócer de la Independencia"
                 " (Chillán y Chillán Viejo)"
                 " [Nativity of Bernardo O'Higgins]")


class CL(Chile):
    pass


class CHL(Chile):
    pass
