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
from dateutil.relativedelta import relativedelta as rd, MO

from holidays.constants import JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, \
    OCT, \
    NOV, DEC
from holidays.holiday_base import HolidayBase


class Italy(HolidayBase):
    PROVINCES = ['AN', 'AO', 'BA', 'BL', 'BO',
                 'BZ', 'BS', 'CB', 'CT', 'Cesena',
                 'CH', 'CS', 'KR', 'EN', 'FE', 'FI',
                 'FC', 'Forli', 'FR', 'GE', 'GO', 'IS',
                 'SP', 'LT', 'MN', 'MS', 'MI',
                 'MO', 'MB', 'NA', 'PD', 'PA',
                 'PR', 'PG', 'PE', 'PC', 'PI',
                 'PD', 'PT', 'RA', 'RE',
                 'RI', 'RN', 'RM', 'RO', 'SA',
                 'SR', 'TE', 'TO', 'TS', 'Pesaro', 'PU',
                 'Urbino', 'VE', 'VC', 'VI']

    def __init__(self, **kwargs):
        self.country = 'IT'
        self.prov = kwargs.pop('prov', kwargs.pop('state', ''))
        HolidayBase.__init__(self, **kwargs)

    def _populate(self, year):
        self[date(year, JAN, 1)] = "Capodanno"
        self[date(year, JAN, 6)] = "Epifania del Signore"
        self[easter(year)] = "Pasqua di Resurrezione"
        self[easter(year) + rd(weekday=MO)] = "Lunedì dell'Angelo"
        if year >= 1946:
            self[date(year, APR, 25)] = "Festa della Liberazione"
        self[date(year, MAY, 1)] = "Festa dei Lavoratori"
        if year >= 1948:
            self[date(year, JUN, 2)] = "Festa della Repubblica"
        self[date(year, AUG, 15)] = "Assunzione della Vergine"
        self[date(year, NOV, 1)] = "Tutti i Santi"
        self[date(year, DEC, 8)] = "Immacolata Concezione"
        self[date(year, DEC, 25)] = "Natale"
        self[date(year, DEC, 26)] = "Santo Stefano"

        # Provinces holidays
        if self.prov:
            if self.prov == 'AN':
                self[date(year, MAY, 4)] = "San Ciriaco"
            elif self.prov == 'AO':
                self[date(year, SEP, 7)] = "San Grato"
            elif self.prov in ('BA'):
                self[date(year, DEC, 6)] = "San Nicola"
            elif self.prov == 'BL':
                self[date(year, NOV, 11)] = "San Martino"
            elif self.prov in ('BO'):
                self[date(year, OCT, 4)] = "San Petronio"
            elif self.prov == 'BZ':
                self[date(year, AUG, 15)] = "Maria Santissima Assunta"
            elif self.prov == 'BS':
                self[date(year, FEB, 15)] = "Santi Faustino e Giovita"
            elif self.prov == 'CB':
                self[date(year, APR, 23)] = "San Giorgio"
            elif self.prov == 'CT':
                self[date(year, FEB, 5)] = "Sant'Agata"
            elif self.prov in ('FC', 'Cesena'):
                self[date(year, JUN, 24)] = "San Giovanni Battista"
            if self.prov in ('FC', 'Forlì'):
                self[date(year, FEB, 4)] = "Madonna del Fuoco"
            elif self.prov == 'CH':
                self[date(year, MAY, 11)] = "San Giustino di Chieti"
            elif self.prov == 'CS':
                self[date(year, FEB, 12)] = "Madonna del Pilerio"
            elif self.prov == 'KR':
                self[date(year, OCT, 9)] = "San Dionigi"
            elif self.prov == 'EN':
                self[date(year, JUL, 2)] = "Madonna della Visitazione"
            elif self.prov == 'FE':
                self[date(year, APR, 23)] = "San Giorgio"
            elif self.prov == 'FI':
                self[date(year, JUN, 24)] = "San Giovanni Battista"
            elif self.prov == 'FR':
                self[date(year, JUN, 20)] = "San Silverio"
            elif self.prov == 'GE':
                self[date(year, JUN, 24)] = "San Giovanni Battista"
            elif self.prov == 'GO':
                self[date(year, MAR, 16)] = "Santi Ilario e Taziano"
            elif self.prov == 'IS':
                self[date(year, MAY, 19)] = "San Pietro Celestino"
            elif self.prov == 'SP':
                self[date(year, MAR, 19)] = "San Giuseppe"
            elif self.prov == 'LT':
                self[date(year, APR, 25)] = "San Marco evangelista"
            elif self.prov == 'ME':
                self[date(year, JUN, 3)] = "Madonna della Lettera"
            elif self.prov == 'MI':
                self[date(year, DEC, 7)] = "Sant'Ambrogio"
            elif self.prov == 'MN':
                self[date(year, MAR, 18)] = "Sant'Anselmo da Baggio"
            elif self.prov == 'MS':
                self[date(year, OCT, 4)] = "San Francesco d'Assisi"
            elif self.prov == 'MO':
                self[date(year, JAN, 31)] = "San Geminiano"
            elif self.prov == 'MB':
                self[date(year, JUN, 24)] = "San Giovanni Battista"
            elif self.prov == 'NA':
                self[date(year, SEP, 19)] = "San Gennaro"
            elif self.prov == 'PD':
                self[date(year, JUN, 13)] = "Sant'Antonio di Padova"
            elif self.prov == 'PA':
                self[date(year, JUL, 15)] = "San Giovanni"
            elif self.prov == 'PR':
                self[date(year, JAN, 13)] = "Sant'Ilario di Poitiers"
            elif self.prov == 'PG':
                self[date(year, JAN, 29)] = "Sant'Ercolano e San Lorenzo"
            elif self.prov == 'PC':
                self[date(year, JUL, 4)] = "Sant'Antonino di Piacenza"
            elif self.prov == 'RM':
                self[date(year, JUN, 29)] = "Santi Pietro e Paolo"
            elif self.prov == 'TO':
                self[date(year, JUN, 24)] = "San Giovanni Battista"
            elif self.prov == 'TS':
                self[date(year, NOV, 3)] = "San Giusto"
            elif self.prov == 'VI':
                self[date(year, APR, 25)] = "San Marco"

        # TODO: add missing provinces' holidays:
        # 'Pisa', 'Pordenone', 'Potenza', 'Ravenna',
        # 'Reggio Emilia', 'Rieti', 'Rimini', 'Rovigo',
        # 'Salerno', 'Siracusa', 'Teramo', 'Torino', 'Urbino',
        # 'Venezia'


class IT(Italy):
    pass


class ITA(Italy):
    pass
