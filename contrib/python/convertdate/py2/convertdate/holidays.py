# -*- coding: utf-8 -*-

# This file is part of convertdate.
# http://github.com/fitnr/convertdate

# Licensed under the MIT license:
# http://opensource.org/licenses/MIT
# Copyright (c) 2016, fitnr <fitnr@fakeisthenewreal>
import time
import calendar
from math import trunc
from .utils import nth_day_of_month
from . import hebrew, julian

# weekdays
MON = 0
TUE = 1
WED = 2
THU = 3
FRI = 4
SAT = 5
SUN = 6

# months
JAN = 1
FEB = 2
MAR = 3
APR = 4
MAY = 5
JUN = 6
JUL = 7
AUG = 8
SEP = 9
OCT = 10
NOV = 11
DEC = 12


def new_years(year, observed=None):
    '''Jan 1st, possibly observed on last day of previous year'''
    if observed:
        weekday = calendar.weekday(year, JAN, 1)
        if weekday == SAT:
            return (year - 1, DEC, 31)
        if weekday == SUN:
            return (year, JAN, 2)
    return (year, JAN, 1)


def martin_luther_king_day(year):
    '''third monday in January'''
    return nth_day_of_month(3, MON, JAN, year)


def lincolns_birthday(year):
    '''Feb 12'''
    return (year, FEB, 12)


def valentines_day(year):
    '''feb 14th'''
    return (year, FEB, 14)


def washingtons_birthday(year, observed=None):
    '''Feb 22, possibly observed on 3rd Monday in February'''
    if observed:
        return nth_day_of_month(3, MON, FEB, year)
    return (year, FEB, 22)


def presidents_day(year):
    '''3rd Monday of Feb'''
    return nth_day_of_month(3, MON, FEB, year)


def pulaski_day(year):
    '''1st monday in March'''
    return nth_day_of_month(1, MON, MAR, year)


def easter(year, church=None):
    '''Calculate Easter in the given church according to the given calendar.'''
    church = church or "western"

    if church == "western":
        return _easter_western(year)

    if church == "orthodox":
        return julian.to_gregorian(*_easter_julian(year))

    if church == "eastern":
        return julian.to_gregorian(*_easter_julian(year, mode="eastern"))

    raise ValueError("Unknown value for 'church'")


def _easter_western(year):
    '''Calculate western easter'''
    # formula taken from http://aa.usno.navy.mil/faq/docs/easter.html
    c = trunc(year / 100)
    n = year - 19 * trunc(year / 19)

    k = trunc((c - 17) / 25)

    i = c - trunc(c / 4) - trunc((c - k) / 3) + (19 * n) + 15
    i = i - 30 * trunc(i / 30)
    i = i - trunc(i / 28) * (1 - trunc(i / 28) * trunc(29 / (i + 1)) * trunc((21 - n) / 11))

    j = year + trunc(year / 4) + i + 2 - c + trunc(c / 4)
    j = j - 7 * trunc(j / 7)

    L = i - j

    month = 3 + trunc((L + 40) / 44)
    day = L + 28 - 31 * trunc(month / 4)

    date = (year, int(month), int(day))

    return date


def _easter_julian(year, mode="dionysian"):
    '''Calculate Easter for the orthodox and eastern churches in the Julian calendar.'''
    # Uses Meeus's Julian algorithm.
    meton = (year % 19)
    b = year % 4
    c = year % 7
    d = (19 * meton + 15) % 30
    if mode == "eastern" and meton == 0:
        d = d + 1
    e = (2 * b + 4 * c - d + 6) % 7
    fmj = 113 + d  # Easter full moon (days after -92 March)
    dmj = fmj + e + 1  # Easter Sunday (days after -92 March)
    esmj = trunc(dmj / 31)  # month of Easter Sunday
    esdj = (dmj % 31) + 1  # day of Easter Sunday

    return year, esmj, esdj


def may_day(year):
    return (year, MAY, 1)


def mothers_day(year):
    '''2nd Sunday in May'''
    return nth_day_of_month(2, SUN, MAY, year)


def memorial_day(year):
    '''last Monday in May'''
    return nth_day_of_month(0, MON, MAY, year)


def fathers_day(year):
    '''3rd Sunday in June'''
    return nth_day_of_month(3, SUN, JUN, year)


def flag_day(year):
    '''June 14th'''
    return (year, JUN, 14)


def independence_day(year, observed=None):
    '''July 4th'''
    day = 4

    if observed:
        weekday = calendar.weekday(year, JUL, 4)
        if weekday == SAT:
            day = 3
        if weekday == SUN:
            day = 5

    return (year, JUL, day)


def labor_day(year):
    '''first Monday in Sep'''
    return nth_day_of_month(1, MON, SEP, year)


def columbus_day(year, country='usa'):
    '''in USA: 2nd Monday in Oct
       Elsewhere: Oct 12'''
    if country == 'usa':
        return nth_day_of_month(2, MON, OCT, year)
    else:
        return (year, OCT, 12)


def halloween(year):
    '''Oct 31'''
    return (year, OCT, 31)


def election_day(year):
    '''1st Tues in Nov'''
    return nth_day_of_month(1, TUE, NOV, year)


def veterans_day(year, observed=None):
    '''Nov 11, or the following closest weekday'''
    day = 11
    if observed:
        weekday = calendar.weekday(year, NOV, 11)
        if weekday == SAT:
            day = 10
        if weekday == SUN:
            day = 12

    return (year, NOV, day)


def rememberance_day(year):
    return veterans_day(year)


def armistice_day(year):
    return veterans_day(year)


def thanksgiving(year, country='usa'):
    '''USA: last Thurs. of November, Canada: 2nd Mon. of October'''
    if country == 'usa':
        if year in [1940, 1941]:
            return nth_day_of_month(3, THU, NOV, year)
        elif year == 1939:
            return nth_day_of_month(4, THU, NOV, year)
        else:
            return nth_day_of_month(0, THU, NOV, year)

    if country == 'canada':
        return nth_day_of_month(2, MON, OCT, year)


def christmas_eve(year):
    '''24th of December'''
    return (year, DEC, 24)


def christmas(year, observed=None):
    '''25th of December'''
    day = 25
    if observed:
        weekday = calendar.weekday(year, DEC, 25)
        if weekday == SAT:
            day = 24
        if weekday == SUN:
            day = 26
    return (year, DEC, day)


def new_years_eve(year):
    '''Dec 31st'''
    return (year, DEC, 31)

# Jewish holidays begins the sunset before the first (secular) day of the holiday
# With the eve option set, the day of this sunset is returned
# without the option, the (secular) day is returned


def hanukkah(year, eve=None):
    year, month, day = hebrew.to_jd_gregorianyear(year, hebrew.KISLEV, 25)
    if eve:
        day = day - 1
    return year, month, day


def purim(year, eve=None):
    if not hebrew.leap(year + hebrew.HEBREW_YEAR_OFFSET):
        year, month, day = hebrew.to_jd_gregorianyear(year, hebrew.ADAR, 14)
    else:
        year, month, day = hebrew.to_jd_gregorianyear(year, hebrew.VEADAR, 14)
    if eve:
        day = day - 1
    return year, month, day


def rosh_hashanah(year, eve=None):
    year, month, day = hebrew.to_jd_gregorianyear(year, hebrew.TISHRI, 1)
    if eve:
        day = day - 1
    return year, month, day


def yom_kippur(year, eve=None):
    year, month, day = hebrew.to_jd_gregorianyear(year, hebrew.TISHRI, 10)
    if eve:
        day = day - 1
    return year, month, day


def passover(year, eve=None):
    year, month, day = hebrew.to_jd_gregorianyear(year, hebrew.NISAN, 15)
    if eve:
        day = day - 1
    return year, month, day


def shavuot(year, eve=None):
    year, month, day = hebrew.to_jd_gregorianyear(year, hebrew.SIVAN, 6)
    if eve:
        day = day - 1
    return year, month, day


def sukkot(year, eve=None):
    year, month, day = hebrew.to_jd_gregorianyear(year, hebrew.TISHRI, 15)
    if eve:
        day = day - 1
    return year, month, day


# Mexican holidays


def dia_constitucion(year, observed=True):
    if observed:
        return nth_day_of_month(1, MON, FEB, year)
    else:
        return (year, FEB, 5)


def natalicio_benito_juarez(year, observed=True):
    if observed:
        return nth_day_of_month(3, MON, MAR, year)
    else:
        return (year, MAR, 21)


def dia_independencia(year):
    return year, SEP, 16


def dia_revolucion(year):
    return nth_day_of_month(3, MON, NOV, year)


class Holidays(object):

    def __init__(self, year=None):
        self.year = year or time.localtime().tm_year

    def set_year(self, year):
        self.year = year

    def __repr__(self):
        return 'Holidays({})'.format(self.year)

    # the holidays...
    @property
    def christmas(self):
        return christmas(self.year, True)

    @property
    def christmas_eve(self):
        return christmas_eve(self.year)

    @property
    def thanksgiving(self, country='usa'):
        return thanksgiving(self.year, country)

    @property
    def new_years(self):
        return new_years(self.year, True)

    @property
    def new_years_eve(self):
        return new_years_eve(self.year)

    @property
    def independence_day(self):
        return independence_day(self.year, observed=True)

    @property
    def flag_day(self):
        return flag_day(self.year)

    @property
    def election_day(self):
        return election_day(self.year)

    @property
    def presidents_day(self):
        return presidents_day(self.year)

    @property
    def washingtons_birthday(self):
        return washingtons_birthday(self.year)

    @property
    def lincolns_birthday(self):
        return lincolns_birthday(self.year)

    @property
    def memorial_day(self):
        return memorial_day(self.year)

    @property
    def labor_day(self):
        return labor_day(self.year)

    @property
    def columbus_day(self, country='usa'):
        return columbus_day(self.year, country)

    @property
    def veterans_day(self):
        return veterans_day(self.year, True)

    @property
    def valentines_day(self):
        return valentines_day(self.year)

    @property
    def halloween(self):
        return halloween(self.year)

    @property
    def mothers_day(self):
        return mothers_day(self.year)

    @property
    def fathers_day(self):
        return fathers_day(self.year)

    @property
    def pulaski_day(self):
        return pulaski_day(self.year)

    @property
    def easter(self):
        return easter(self.year)

    @property
    def martin_luther_king_day(self):
        return martin_luther_king_day(self.year)

    @property
    def hanukkah(self):
        return hanukkah(self.year, eve=False)

    @property
    def purim(self):
        return purim(self.year, eve=False)

    @property
    def rosh_hashanah(self):
        return rosh_hashanah(self.year, eve=False)

    @property
    def yom_kippur(self):
        return yom_kippur(self.year, eve=False)

    @property
    def passover(self):
        return passover(self.year, eve=False)

    @property
    def shavuot(self):
        return shavuot(self.year, eve=False)

    @property
    def sukkot(self):
        return sukkot(self.year, eve=False)

    @property
    def dia_constitucion(self):
        return dia_constitucion(self.year, observed=True)

    @property
    def natalicio_benito_juarez(self):
        return natalicio_benito_juarez(self.year, observed=True)

    @property
    def dia_independencia(self):
        return dia_independencia(self.year)

    @property
    def dia_revolucion(self):
        return dia_revolucion(self.year)


if __name__ == '__main__':
    holiday = Holidays(time.localtime().tm_year)
