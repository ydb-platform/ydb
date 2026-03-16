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


from holidays.constants import TUE, THU, SUN
from holidays.constants import FEB, MAR, APR, MAY, SEP, NOV, DEC
from holidays.holiday_base import HolidayBase


class Angola(HolidayBase):

    def __init__(self, **kwargs):
        # https://www.officeholidays.com/countries/angola/
        # https://www.timeanddate.com/holidays/angola/
        self.country = 'AO'
        HolidayBase.__init__(self, **kwargs)

    def _populate(self, year):
        # Observed since 1975
        # TODO do more research on history of Angolan holidays

        if year > 2018:
            self[date(year, MAR, 23)] = "Dia da Libertação da África Austral"

        if year > 1979:
            self[date(year, SEP, 17)] = "Dia do Herói Nacional"

        if year > 1974:
            self[date(year, 1, 1)] = "Ano novo"

            e = easter(year)
            good_friday = e - rd(days=2)
            self[good_friday] = "Sexta-feira Santa"

            # carnival is the Tuesday before Ash Wednesday
            # which is 40 days before easter excluding sundays
            carnival = e - rd(days=46)
            while carnival.weekday() != TUE:
                carnival = carnival - rd(days=1)
            self[carnival] = "Carnaval"

            self[date(year, FEB, 4)] = "Dia do Início da Luta Armada"
            self[date(year, MAR, 8)] = "Dia Internacional da Mulher"
            self[date(year, APR, 4)] = "Dia da Paz e Reconciliação"
            self[date(year, MAY, 1)] = "Dia Mundial do Trabalho"
            self[date(year, SEP, 17)] = "Dia dos Heroes Nacional"
            self[date(year, NOV, 2)] = "Dia dos Finados"
            self[date(year, NOV, 11)] = "Dia da Independência"
            self[date(year, DEC, 25)] = "Dia de Natal e da Família"

        # As of 1995/1/1, whenever a public holiday falls on a Sunday,
        # it rolls over to the following Monday
        # Since 2018 when a public holiday falls on the Tuesday or Thursday
        # the Monday or Friday is also a holiday
        for k, v in list(self.items()):
            if self.observed and year > 1974:
                if k.weekday() == SUN:
                    self[k + rd(days=1)] = v + " (Observed)"
            if self.observed and year > 2017:
                if k.weekday() == SUN:
                    pass
            if self.observed and year > 2017:
                if k.weekday() == TUE:
                    self[k - rd(days=1)] = v + " (Day off)"
                elif k.weekday() == THU:
                    self[k + rd(days=1)] = v + " (Day off)"
            if self.observed and year > 1994 and k.weekday() == SUN:
                self[k + rd(days=1)] = v + " (Observed)"


class AO(Angola):
    pass


class AGO(Angola):
    pass
