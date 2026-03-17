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
from dateutil.relativedelta import relativedelta as rd, MO, TH, FR

from holidays.constants import JAN, MAR, MAY, JUN, JUL, AUG, OCT, \
    NOV, DEC
from holidays.constants import MON, WEEKEND
from holidays.holiday_base import HolidayBase


class Colombia(HolidayBase):
    # https://es.wikipedia.org/wiki/Anexo:D%C3%ADas_festivos_en_Colombia

    def __init__(self, **kwargs):
        self.country = 'CO'
        HolidayBase.__init__(self, **kwargs)

    def _populate(self, year):

        # Fixed date holidays!
        # If observed=True and they fall on a weekend they are not observed.
        # If observed=False there are 18 holidays

        # New Year's Day
        if self.observed and date(year, JAN, 1).weekday() in WEEKEND:
            pass
        else:
            self[date(year, JAN, 1)] = "Año Nuevo [New Year's Day]"

        # Labor Day
        self[date(year, MAY, 1)] = "Día del Trabajo [Labour Day]"

        # Independence Day
        name = "Día de la Independencia [Independence Day]"
        if self.observed and date(year, JUL, 20).weekday() in WEEKEND:
            pass
        else:
            self[date(year, JUL, 20)] = name

        # Battle of Boyaca
        self[date(year, AUG, 7)] = "Batalla de Boyacá [Battle of Boyacá]"

        # Immaculate Conception
        if self.observed and date(year, DEC, 8).weekday() in WEEKEND:
            pass
        else:
            self[date(year, DEC, 8)] = "La Inmaculada Concepción" \
                " [Immaculate Conception]"

        # Christmas
        self[date(year, DEC, 25)] = "Navidad [Christmas]"

        # Emiliani Law holidays!
        # Unless they fall on a Monday they are observed the following monday

        #  Epiphany
        name = "Día de los Reyes Magos [Epiphany]"
        if date(year, JAN, 6).weekday() == MON or not self.observed:
            self[date(year, JAN, 6)] = name
        else:
            self[date(year, JAN, 6) + rd(weekday=MO)] = name + "(Observed)"

        # Saint Joseph's Day
        name = "Día de San José [Saint Joseph's Day]"
        if date(year, MAR, 19).weekday() == MON or not self.observed:
            self[date(year, MAR, 19)] = name
        else:
            self[date(year, MAR, 19) + rd(weekday=MO)] = name + "(Observed)"

        # Saint Peter and Saint Paul's Day
        name = "San Pedro y San Pablo [Saint Peter and Saint Paul]"
        if date(year, JUN, 29).weekday() == MON or not self.observed:
            self[date(year, JUN, 29)] = name
        else:
            self[date(year, JUN, 29) + rd(weekday=MO)] = name + "(Observed)"

        # Assumption of Mary
        name = "La Asunción [Assumption of Mary]"
        if date(year, AUG, 15).weekday() == MON or not self.observed:
            self[date(year, AUG, 15)] = name
        else:
            self[date(year, AUG, 15) + rd(weekday=MO)] = name + "(Observed)"

        # Discovery of America
        name = "Descubrimiento de América [Discovery of America]"
        if date(year, OCT, 12).weekday() == MON or not self.observed:
            self[date(year, OCT, 12)] = name
        else:
            self[date(year, OCT, 12) + rd(weekday=MO)] = name + \
                "(Observed)"

        # All Saints’ Day
        name = "Dia de Todos los Santos [All Saint's Day]"
        if date(year, NOV, 1).weekday() == MON or not self.observed:
            self[date(year, NOV, 1)] = name
        else:
            self[date(year, NOV, 1) + rd(weekday=MO)] = name + \
                "(Observed)"

        # Independence of Cartagena
        name = "Independencia de Cartagena [Independence of Cartagena]"
        if date(year, NOV, 11).weekday() == MON or not self.observed:
            self[date(year, NOV, 11)] = name
        else:
            self[date(year, NOV, 11) + rd(weekday=MO)] = name + \
                "(Observed)"

        # Holidays based on Easter

        # Maundy Thursday
        self[easter(year) + rd(weekday=TH(-1))
             ] = "Jueves Santo [Maundy Thursday]"

        # Good Friday
        self[easter(year) + rd(weekday=FR(-1))
             ] = "Viernes Santo [Good Friday]"

        # Holidays based on Easter but are observed the following monday
        # (unless they occur on a monday)

        # Ascension of Jesus
        name = "Ascensión del señor [Ascension of Jesus]"
        hdate = easter(year) + rd(days=+39)
        if hdate.weekday() == MON or not self.observed:
            self[hdate] = name
        else:
            self[hdate + rd(weekday=MO)] = name + "(Observed)"

        # Corpus Christi
        name = "Corpus Christi [Corpus Christi]"
        hdate = easter(year) + rd(days=+60)
        if hdate.weekday() == MON or not self.observed:
            self[hdate] = name
        else:
            self[hdate + rd(weekday=MO)] = name + "(Observed)"

        # Sacred Heart
        name = "Sagrado Corazón [Sacred Heart]"
        hdate = easter(year) + rd(days=+68)
        if hdate.weekday() == MON or not self.observed:
            self[hdate] = name
        else:
            self[hdate + rd(weekday=MO)] = name + "(Observed)"


class CO(Colombia):
    pass


class COL(Colombia):
    pass
