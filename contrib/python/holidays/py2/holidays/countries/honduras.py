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

from holidays.constants import JAN, FEB, MAR, APR, MAY, SEP, OCT, \
    DEC
from holidays.holiday_base import HolidayBase


class Honduras(HolidayBase):
    # https://www.timeanddate.com/holidays/honduras/

    def __init__(self, **kwargs):
        self.country = "HND"
        HolidayBase.__init__(self, **kwargs)

    def _populate(self, year):
        # New Year's Day
        if self.observed and date(year, JAN, 1):
            self[date(year, JAN, 1)] = "Año Nuevo [New Year's Day]"

        # The Three Wise Men Day
        if self.observed and date(year, JAN, 6):
            name = "Día de los Reyes Magos [The Three Wise Men Day] (Observed)"
            self[date(year, JAN, 6)] = name

        # The Three Wise Men Day
        if self.observed and date(year, FEB, 3):
            name = "Día de la virgen de Suyapa [Our Lady of Suyapa] (Observed)"
            self[date(year, FEB, 3)] = name

        # The Father's Day
        if self.observed and date(year, MAR, 19):
            name = "Día del Padre [Father's Day] (Observed)"
            self[date(year, MAR, 19)] = name

        # Maundy Thursday
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

        # America Day
        if self.observed and date(year, APR, 14):
            self[date(year, APR, 14)] = "Día de las Américas [America Day]"

        # Labor Day
        if self.observed and date(year, MAY, 1):
            self[date(year, MAY, 1)] = "Día del Trabajo [Labour Day]"

        # Mother's Day
        may_first = date(int(year), 5, 1)
        weekday_seq = may_first.weekday()
        mom_day = (14 - weekday_seq)
        if self.observed and date(year, MAY, mom_day):
            str_day = "Día de la madre [Mother's Day] (Observed)"
            self[date(year, MAY, mom_day)] = str_day

        # Children's Day
        if self.observed and date(year, SEP, 10):
            name = "Día del niño [Children day] (Observed)"
            self[date(year, SEP, 10)] = name

        # Independence Day
        if self.observed and date(year, SEP, 15):
            name = "Día de la Independencia [Independence Day]"
            self[date(year, SEP, 15)] = name

        # Teacher's Day
        if self.observed and date(year, SEP, 17):
            name = "Día del Maestro [Teacher's day] (Observed)"
            self[date(year, SEP, 17)] = name

        # October Holidays are joined on 3 days starting at October 3 to 6.
        # Some companies work medium day and take the rest on saturday.
        # This holiday is variant and some companies work normally.
        # If start day is weekend is ignored.
        # The main objective of this is to increase the tourism.

        # https://www.hondurastips.hn/2017/09/20/de-donde-nace-el-feriado-morazanico/

        if year <= 2014:
            # Morazan's Day
            if self.observed and date(year, OCT, 3):
                self[date(year, OCT, 3)] = "Día de Morazán [Morazan's Day]"

            # Columbus Day
            if self.observed and date(year, OCT, 12):
                self[date(year, OCT, 12)] = "Día de la Raza [Columbus Day]"

            # Amy Day
            if self.observed and date(year, OCT, 21):
                str_day = "Día de las Fuerzas Armadas [Army Day]"
                self[date(year, OCT, 21)] = str_day
        else:
            # Morazan Weekend
            if self.observed and date(year, OCT, 3):
                name = "Semana Morazánica [Morazan Weekend]"
                self[date(year, OCT, 3)] = name

            # Morazan Weekend
            if self.observed and date(year, OCT, 4):
                name = "Semana Morazánica [Morazan Weekend]"
                self[date(year, OCT, 4)] = name

            # Morazan Weekend
            if self.observed and date(year, OCT, 5):
                name = "Semana Morazánica [Morazan Weekend]"
                self[date(year, OCT, 5)] = name

        # Christmas
        self[date(year, DEC, 25)] = "Navidad [Christmas]"


class HN(Honduras):
    pass


class HND(Honduras):
    pass
