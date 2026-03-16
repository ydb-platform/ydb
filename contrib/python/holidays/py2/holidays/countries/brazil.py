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
from dateutil.relativedelta import relativedelta as rd, TU

from holidays.constants import JAN, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, \
    NOV, DEC
from holidays.holiday_base import HolidayBase


class Brazil(HolidayBase):
    """
    https://pt.wikipedia.org/wiki/Feriados_no_Brasil
    """

    STATES = ['AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT',
              'MS', 'MG', 'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 'RS', 'RO',
              'RR', 'SC', 'SP', 'SE', 'TO']

    def __init__(self, **kwargs):
        self.country = 'BR'
        HolidayBase.__init__(self, **kwargs)

    def _populate(self, year):
        # New Year's Day
        self[date(year, JAN, 1)] = "Ano novo"

        self[date(year, APR, 21)] = "Tiradentes"

        self[date(year, MAY, 1)] = "Dia Mundial do Trabalho"

        self[date(year, SEP, 7)] = "Independência do Brasil"

        self[date(year, OCT, 12)] = "Nossa Senhora Aparecida"

        self[date(year, NOV, 2)] = "Finados"

        self[date(year, NOV, 15)] = "Proclamação da República"

        # Christmas Day
        self[date(year, DEC, 25)] = "Natal"

        self[easter(year) - rd(days=2)] = "Sexta-feira Santa"

        self[easter(year)] = "Páscoa"

        self[easter(year) + rd(days=60)] = "Corpus Christi"

        quaresma = easter(year) - rd(days=46)
        self[quaresma] = "Quarta-feira de cinzas (Início da Quaresma)"

        self[quaresma - rd(weekday=TU(-1))] = "Carnaval"

        if self.state == 'AC':
            self[date(year, JAN, 23)] = "Dia do evangélico"
            self[date(year, JUN, 15)] = "Aniversário do Acre"
            self[date(year, SEP, 5)] = "Dia da Amazônia"
            self[date(year, NOV, 17)] = "Assinatura do Tratado de" \
                                        " Petrópolis"

        if self.state == 'AL':
            self[date(year, JUN, 24)] = "São João"
            self[date(year, JUN, 29)] = "São Pedro"
            self[date(year, SEP, 16)] = "Emancipação política de Alagoas"
            self[date(year, NOV, 20)] = "Consciência Negra"

        if self.state == 'AP':
            self[date(year, MAR, 19)] = "Dia de São José"
            self[date(year, JUL, 25)] = "São Tiago"
            self[date(year, OCT, 5)] = "Criação do estado"
            self[date(year, NOV, 20)] = "Consciência Negra"

        if self.state == 'AM':
            self[date(year, SEP, 5)] = "Elevação do Amazonas" \
                " à categoria de província"
            self[date(year, NOV, 20)] = "Consciência Negra"
            self[date(year, DEC, 8)] = "Dia de Nossa Senhora da Conceição"

        if self.state == 'BA':
            self[date(year, JUL, 2)] = "Independência da Bahia"

        if self.state == 'CE':
            self[date(year, MAR, 19)] = "São José"
            self[date(year, MAR, 25)] = "Data Magna do Ceará"

        if self.state == 'DF':
            self[date(year, APR, 21)] = "Fundação de Brasília"
            self[date(year, NOV, 30)] = "Dia do Evangélico"

        if self.state == 'ES':
            self[date(year, OCT, 28)] = "Dia do Servidor Público"

        if self.state == 'GO':
            self[date(year, OCT, 28)] = "Dia do Servidor Público"

        if self.state == 'MA':
            self[date(year, JUL, 28)] = "Adesão do Maranhão" \
                " à independência do Brasil"
            self[date(year, DEC, 8)] = "Dia de Nossa Senhora da Conceição"

        if self.state == 'MT':
            self[date(year, NOV, 20)] = "Consciência Negra"

        if self.state == 'MS':
            self[date(year, OCT, 11)] = "Criação do estado"

        if self.state == 'MG':
            self[date(year, APR, 21)] = "Data Magna de MG"

        if self.state == 'PA':
            self[date(year, AUG, 15)] = "Adesão do Grão-Pará" \
                " à independência do Brasil"

        if self.state == 'PB':
            self[date(year, AUG, 5)] = "Fundação do Estado"

        if self.state == 'PE':
            self[date(year, MAR, 6)] = "Revolução Pernambucana (Data Magna)"
            self[date(year, JUN, 24)] = "São João"

        if self.state == 'PI':
            self[date(year, MAR, 13)] = "Dia da Batalha do Jenipapo"
            self[date(year, OCT, 19)] = "Dia do Piauí"

        if self.state == 'PR':
            self[date(year, DEC, 19)] = "Emancipação do Paraná"

        if self.state == 'RJ':
            self[date(year, APR, 23)] = "Dia de São Jorge"
            self[date(year, OCT, 28)] = "Dia do Funcionário Público"
            self[date(year, NOV, 20)] = "Zumbi dos Palmares"

        if self.state == 'RN':
            self[date(year, JUN, 29)] = "Dia de São Pedro"
            self[date(year, OCT, 3)] = "Mártires de Cunhaú e Uruaçuu"

        if self.state == 'RS':
            self[date(year, SEP, 20)] = "Revolução Farroupilha"

        if self.state == 'RO':
            self[date(year, JAN, 4)] = "Criação do estado"
            self[date(year, JUN, 18)] = "Dia do Evangélico"

        if self.state == 'RR':
            self[date(year, OCT, 5)] = "Criação de Roraima"

        if self.state == 'SC':
            self[date(year, AUG, 11)] = "Criação da capitania," \
                " separando-se de SP"

        if self.state == 'SP':
            self[date(year, JUL, 9)] = "Revolução Constitucionalista de 1932"

        if self.state == 'SE':
            self[date(year, JUL, 8)] = "Autonomia política de Sergipe"

        if self.state == 'TO':
            self[date(year, JAN, 1)] = "Instalação de Tocantins"
            self[date(year, SEP, 8)] = "Nossa Senhora da Natividade"
            self[date(year, OCT, 5)] = "Criação de Tocantins"


class BR(Brazil):
    pass


class BRA(Brazil):
    pass
