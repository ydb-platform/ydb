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
from dateutil.relativedelta import relativedelta as rd, MO, FR, WE, TU

from holidays.constants import JAN, FEB, MAR, APR, JUN, SEP, OCT, \
    NOV, DEC
from holidays.constants import TUE, WED, THU, WEEKEND
from holidays.holiday_base import HolidayBase


class NewZealand(HolidayBase):
    PROVINCES = ['NTL', 'AUK', 'TKI', 'HKB', 'WGN', 'MBH', 'NSN', 'CAN',
                 'STC', 'WTL', 'OTA', 'STL', 'CIT']

    def __init__(self, **kwargs):
        self.country = 'NZ'
        HolidayBase.__init__(self, **kwargs)

    def _populate(self, year):
        # Bank Holidays Act 1873
        # The Employment of Females Act 1873
        # Factories Act 1894
        # Industrial Conciliation and Arbitration Act 1894
        # Labour Day Act 1899
        # Anzac Day Act 1920, 1949, 1956
        # New Zealand Day Act 1973
        # Waitangi Day Act 1960, 1976
        # Sovereign's Birthday Observance Act 1937, 1952
        # Holidays Act 1981, 2003
        if year < 1894:
            return

        # New Year's Day
        name = "New Year's Day"
        jan1 = date(year, JAN, 1)
        self[jan1] = name
        if self.observed and jan1.weekday() in WEEKEND:
            self[date(year, JAN, 3)] = name + " (Observed)"

        name = "Day after New Year's Day"
        jan2 = date(year, JAN, 2)
        self[jan2] = name
        if self.observed and jan2.weekday() in WEEKEND:
            self[date(year, JAN, 4)] = name + " (Observed)"

        # Waitangi Day
        if year > 1973:
            name = "New Zealand Day"
            if year > 1976:
                name = "Waitangi Day"
            feb6 = date(year, FEB, 6)
            self[feb6] = name
            if self.observed and year >= 2014 and feb6.weekday() in WEEKEND:
                self[feb6 + rd(weekday=MO)] = name + " (Observed)"

        # Easter
        self[easter(year) + rd(weekday=FR(-1))] = "Good Friday"
        self[easter(year) + rd(weekday=MO)] = "Easter Monday"

        # Anzac Day
        if year > 1920:
            name = "Anzac Day"
            apr25 = date(year, APR, 25)
            self[apr25] = name
            if self.observed and year >= 2014 and apr25.weekday() in WEEKEND:
                self[apr25 + rd(weekday=MO)] = name + " (Observed)"

        # Sovereign's Birthday
        if year >= 1952:
            name = "Queen's Birthday"
        elif year > 1901:
            name = "King's Birthday"
        if year == 1952:
            self[date(year, JUN, 2)] = name  # Elizabeth II
        elif year > 1937:
            self[date(year, JUN, 1) + rd(weekday=MO(+1))] = name  # EII & GVI
        elif year == 1937:
            self[date(year, JUN, 9)] = name  # George VI
        elif year == 1936:
            self[date(year, JUN, 23)] = name  # Edward VIII
        elif year > 1911:
            self[date(year, JUN, 3)] = name  # George V
        elif year > 1901:
            # http://paperspast.natlib.govt.nz/cgi-bin/paperspast?a=d&d=NZH19091110.2.67
            self[date(year, NOV, 9)] = name  # Edward VII

        # Labour Day
        name = "Labour Day"
        if year >= 1910:
            self[date(year, OCT, 1) + rd(weekday=MO(+4))] = name
        elif year > 1899:
            self[date(year, OCT, 1) + rd(weekday=WE(+2))] = name

        # Christmas Day
        name = "Christmas Day"
        dec25 = date(year, DEC, 25)
        self[dec25] = name
        if self.observed and dec25.weekday() in WEEKEND:
            self[date(year, DEC, 27)] = name + " (Observed)"

        # Boxing Day
        name = "Boxing Day"
        dec26 = date(year, DEC, 26)
        self[dec26] = name
        if self.observed and dec26.weekday() in WEEKEND:
            self[date(year, DEC, 28)] = name + " (Observed)"

        # Province Anniversary Day
        if self.prov in ('NTL', 'Northland', 'AUK', 'Auckland'):
            if 1963 < year <= 1973 and self.prov in ('NTL', 'Northland'):
                name = "Waitangi Day"
                dt = date(year, FEB, 6)
            else:
                name = "Auckland Anniversary Day"
                dt = date(year, JAN, 29)
            if dt.weekday() in (TUE, WED, THU):
                self[dt + rd(weekday=MO(-1))] = name
            else:
                self[dt + rd(weekday=MO)] = name

        elif self.prov in ('TKI', 'Taranaki', 'New Plymouth'):
            name = "Taranaki Anniversary Day"
            self[date(year, MAR, 1) + rd(weekday=MO(+2))] = name

        elif self.prov in ('HKB', "Hawke's Bay"):
            name = "Hawke's Bay Anniversary Day"
            labour_day = date(year, OCT, 1) + rd(weekday=MO(+4))
            self[labour_day + rd(weekday=FR(-1))] = name

        elif self.prov in ('WGN', 'Wellington'):
            name = "Wellington Anniversary Day"
            jan22 = date(year, JAN, 22)
            if jan22.weekday() in (TUE, WED, THU):
                self[jan22 + rd(weekday=MO(-1))] = name
            else:
                self[jan22 + rd(weekday=MO)] = name

        elif self.prov in ('MBH', 'Marlborough'):
            name = "Marlborough Anniversary Day"
            labour_day = date(year, OCT, 1) + rd(weekday=MO(+4))
            self[labour_day + rd(weeks=1)] = name

        elif self.prov in ('NSN', 'Nelson'):
            name = "Nelson Anniversary Day"
            feb1 = date(year, FEB, 1)
            if feb1.weekday() in (TUE, WED, THU):
                self[feb1 + rd(weekday=MO(-1))] = name
            else:
                self[feb1 + rd(weekday=MO)] = name

        elif self.prov in ('CAN', 'Canterbury'):
            name = "Canterbury Anniversary Day"
            showday = date(year, NOV, 1) + rd(weekday=TU) + \
                rd(weekday=FR(+2))
            self[showday] = name

        elif self.prov in ('STC', 'South Canterbury'):
            name = "South Canterbury Anniversary Day"
            dominion_day = date(year, SEP, 1) + rd(weekday=MO(4))
            self[dominion_day] = name

        elif self.prov in ('WTL', 'Westland'):
            name = "Westland Anniversary Day"
            dec1 = date(year, DEC, 1)
            # Observance varies?!?!
            if year == 2005:  # special case?!?!
                self[date(year, DEC, 5)] = name
            elif dec1.weekday() in (TUE, WED, THU):
                self[dec1 + rd(weekday=MO(-1))] = name
            else:
                self[dec1 + rd(weekday=MO)] = name

        elif self.prov in ('OTA', 'Otago'):
            name = "Otago Anniversary Day"
            mar23 = date(year, MAR, 23)
            # there is no easily determined single day of local observance?!?!
            if mar23.weekday() in (TUE, WED, THU):
                dt = mar23 + rd(weekday=MO(-1))
            else:
                dt = mar23 + rd(weekday=MO)
            if dt == easter(year) + rd(weekday=MO):  # Avoid Easter Monday
                dt += rd(days=1)
            self[dt] = name

        elif self.prov in ('STL', 'Southland'):
            name = "Southland Anniversary Day"
            jan17 = date(year, JAN, 17)
            if year > 2011:
                self[easter(year) + rd(weekday=TU)] = name
            else:
                if jan17.weekday() in (TUE, WED, THU):
                    self[jan17 + rd(weekday=MO(-1))] = name
                else:
                    self[jan17 + rd(weekday=MO)] = name

        elif self.prov in ('CIT', 'Chatham Islands'):
            name = "Chatham Islands Anniversary Day"
            nov30 = date(year, NOV, 30)
            if nov30.weekday() in (TUE, WED, THU):
                self[nov30 + rd(weekday=MO(-1))] = name
            else:
                self[nov30 + rd(weekday=MO)] = name


class NZ(NewZealand):
    pass


class NZL(NewZealand):
    pass
