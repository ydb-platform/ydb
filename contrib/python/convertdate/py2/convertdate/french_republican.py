# -*- coding: utf-8 -*-

# This file is part of convertdate.
# http://github.com/fitnr/convertdate

# Licensed under the MIT license:
# http://opensource.org/licenses/MIT
# Copyright (c) 2016, fitnr <fitnr@fakeisthenewreal>
from math import trunc
from pymeeus.Sun import Sun
from . import gregorian
from .data.french_republican_days import french_republican_days

# julian day (1792, 9, 22)
EPOCH = 2375839.5

YEAR_EPOCH = 1791.

DAYS_IN_YEAR = 365.

MOIS = [
    u'Vendémiaire',
    u'Brumaire',
    u'Frimaire',
    u'Nivôse',
    u'Pluviôse',
    u'Ventôse',
    u'Germinal',
    u'Floréal',
    u'Prairial',
    u'Messidor',
    u'Thermidor',
    u'Fructidor',
    u'Sansculottides'
]

LEAP_CYCLE_DAYS = 1461.  # 365 * 4 + 1
LEAP_CYCLE_YEARS = 4.


def leap(year, method=None):
    '''
    Determine if this is a leap year in the FR calendar using one of three methods: 4, 100, 128
    (every 4th years, every 4th or 400th but not 100th, every 4th but not 128th)

    Methods:
        * 4 (concordance rule): leap every four years: 3, 7, 11, 15, ... etc
        * 100 (Romme's rule): leap every 4th and 400th year, but not 100th:
            20, 24, ... 96, 104, ... 396, 400, 404 ...
        * 128 (von Mädler's rule): leap every 4th but not 128th: 20, 24, ... 124, 132, ...
        * equinox [default]: use calculation of the equinox to determine date, never returns a leap year
    '''
    method = method or 'equinox'

    if year in (3, 7, 11):
        return True

    if year < 15:
        return False

    if method in (4, 'continuous') or (year <= 16 and method in (128, 'madler', 4, 'continuous')):
        return year % 4 == 3

    if method in (100, 'romme'):
        return (year % 4 == 0 and year % 100 != 0) or year % 400 == 0

    if method in (128, 'madler'):
        return year % 4 == 0 and year % 128 != 0

    if method == 'equinox':
        # Is equinox on 366th day after (year, 1, 1)
        startjd = to_jd(year, 1, 1, method='equinox')
        if premier_da_la_annee(startjd + 367) - startjd == 366.0:
            return True
    else:
        raise ValueError("Unknown leap year method. Try: continuous, romme, madler or equinox")

    return False


def _previous_fall_equinox(jd):
    '''Return the julian day count of the previous fall equinox.'''
    y, _, _ = gregorian.from_jd(jd)
    eqx = Sun.get_equinox_solstice(y, "autumn").jde()
    if eqx > jd:
        eqx = Sun.get_equinox_solstice(y - 1, "autumn").jde()

    return eqx


def _next_fall_equinox(jd):
    '''Return the julian day count of the previous fall equinox.'''
    y, _, _ = gregorian.from_jd(jd)
    eqx = Sun.get_equinox_solstice(y, "autumn").jde()
    if eqx < jd:
        eqx = Sun.get_equinox_solstice(y + 1, "autumn").jde()

    return eqx


def premier_da_la_annee(jd):
    '''
        Returns Julian day number containing fall equinox (first day of the FR year)
        of the current FR year.
    '''
    previous = trunc(_previous_fall_equinox(jd) - 0.5) + 0.5

    if previous + 364 < jd:
        # test if current day is the equinox if the previous equinox was a long time ago
        nxt = trunc(_next_fall_equinox(jd) - 0.5) + 0.5

        if nxt <= jd:
            return nxt

    return previous


def to_jd(year, month, day, method=None):
    '''Obtain Julian day from a given French Revolutionary calendar date.'''
    method = method or 'equinox'

    if day < 1 or day > 30:
        raise ValueError("Invalid day for this calendar")

    if month > 13:
        raise ValueError("Invalid month for this calendar")

    if month == 13 and day > 5 + leap(year, method=method):
        raise ValueError("Invalid day for this month in this calendar")

    if method == 'equinox':
        return _to_jd_equinox(year, month, day)

    return _to_jd_schematic(year, month, day, method)


def _to_jd_schematic(year, month, day, method):
    '''Calculate JD using various leap-year calculation methods'''

    y0, y1, y2, y3, y4, y5 = 0, 0, 0, 0, 0, 0

    intercal_cycle_yrs, over_cycle_yrs, leap_suppression_yrs = None, None, None

    # Use the every-four-years method below year 16 (madler) or below 15 (romme)
    if ((method in (100, 'romme') and year < 15) or
            (method in (128, 'madler') and year < 17)):
        method = 4

    if method in (4, 'continuous'):
        # Leap years: 15, 19, 23, ...
        y5 = -365

    elif method in (100, 'romme'):
        year = year - 13
        y5 = DAYS_IN_YEAR * 12 + 3

        leap_suppression_yrs = 100.
        leap_suppression_days = 36524  # leap_cycle_days * 25 - 1

        intercal_cycle_yrs = 400.
        intercal_cycle_days = 146097  # leap_suppression_days * 4 + 1

        over_cycle_yrs = 4000.
        over_cycle_days = 1460969  # intercal_cycle_days * 10 - 1

    elif method in (128, 'madler'):
        year = year - 17
        y5 = DAYS_IN_YEAR * 16 + 4

        leap_suppression_days = 46751  # 32 * leap_cycle_days - 1
        leap_suppression_yrs = 128

    else:
        raise ValueError("Unknown leap year method. Try: continuous, romme, madler or equinox")

    if over_cycle_yrs:
        y0 = trunc(year / over_cycle_yrs) * over_cycle_days
        year = year % over_cycle_yrs

    # count intercalary cycles in days (400 years long or None)
    if intercal_cycle_yrs:
        y1 = trunc(year / intercal_cycle_yrs) * intercal_cycle_days
        year = year % intercal_cycle_yrs

    # count leap suppresion cycles in days (100 or 128 years long)
    if leap_suppression_yrs:
        y2 = trunc(year / leap_suppression_yrs) * leap_suppression_days
        year = year % leap_suppression_yrs

    y3 = trunc(year / LEAP_CYCLE_YEARS) * LEAP_CYCLE_DAYS
    year = year % LEAP_CYCLE_YEARS

    # Adjust 'year' by one to account for lack of year 0
    y4 = year * DAYS_IN_YEAR

    yj = y0 + y1 + y2 + y3 + y4 + y5

    mj = (month - 1) * 30

    return EPOCH + yj + mj + day - 1


def _to_jd_equinox(an, mois, jour):
    '''Return jd of this FR date, counting from the previous equinox.'''
    day_of_adr = (30 * (mois - 1)) + (jour - 1)
    equinoxe = _next_fall_equinox(gregorian.to_jd(an + YEAR_EPOCH, 1, 1))
    return trunc(equinoxe - 0.5) + 0.5 + day_of_adr


def from_jd(jd, method=None):
    '''Calculate date in the French Revolutionary
    calendar from Julian day.  The five or six
    "sansculottides" are considered a thirteenth
    month in the results of this function.'''
    method = method or 'equinox'

    if method == 'equinox':
        return _from_jd_equinox(jd)

    else:
        return _from_jd_schematic(jd, method)


def _from_jd_schematic(jd, method):
    '''Convert from JD using various leap-year calculation methods'''
    if jd < EPOCH:
        raise ValueError("Can't convert days before the French Revolution")

    # days since Epoch
    J = trunc(jd) + 0.5 - EPOCH

    y0, y1, y2, y3, y4, y5 = 0, 0, 0, 0, 0, 0
    intercal_cycle_days = leap_suppression_days = over_cycle_days = None

    # Use the every-four-years method below year 17
    if (J <= DAYS_IN_YEAR * 12 + 3 and
            method in (100, 'romme')) or (J <= DAYS_IN_YEAR * 17 + 4 and method in (128, 'madler')):
        method = 4

    # set p and r in Hatcher algorithm
    if method in (4, 'continuous'):
        # Leap years: 15, 19, 23, ...
        # Reorganize so that leap day is last day of cycle
        J = J + 365
        y5 = - 1

    elif method in (100, 'romme'):
        # Year 15 is not a leap year
        # Year 16 is leap, then multiples of 4, not multiples of 100, yes multiples of 400
        y5 = 12
        J = J - DAYS_IN_YEAR * 12 - 3

        leap_suppression_yrs = 100.
        leap_suppression_days = 36524  # LEAP_CYCLE_DAYS * 25 - 1

        intercal_cycle_yrs = 400.
        intercal_cycle_days = 146097  # leap_suppression_days * 4 + 1

        over_cycle_yrs = 4000.
        over_cycle_days = 1460969  # intercal_cycle_days * 10 - 1

    elif method in (128, 'madler'):
        # Year 15 is a leap year, then year 20 and multiples of 4, not multiples of 128
        y5 = 16
        J = J - DAYS_IN_YEAR * 16 - 4

        leap_suppression_yrs = 128
        leap_suppression_days = 46751  # 32 * leap_cycle_days - 1

    else:
        raise ValueError("Unknown leap year method. Try: continuous, romme, madler or equinox")

    if over_cycle_days:
        y0 = trunc(J / over_cycle_days) * over_cycle_yrs
        J = J % over_cycle_days

    if intercal_cycle_days:
        y1 = trunc(J / intercal_cycle_days) * intercal_cycle_yrs
        J = J % intercal_cycle_days

    if leap_suppression_days:
        y2 = trunc(J / leap_suppression_days) * leap_suppression_yrs
        J = J % leap_suppression_days

    y3 = trunc(J / LEAP_CYCLE_DAYS) * LEAP_CYCLE_YEARS

    if J % LEAP_CYCLE_DAYS == LEAP_CYCLE_DAYS - 1:
        J = 1460
    else:
        J = J % LEAP_CYCLE_DAYS

    # 0 <= J <= 1460
    # J needs to be 365 here on leap days ONLY

    y4 = trunc(J / DAYS_IN_YEAR)

    if J == DAYS_IN_YEAR * 4:
        y4 = y4 - 1
        J = 365.0
    else:
        J = J % DAYS_IN_YEAR

    year = y0 + y1 + y2 + y3 + y4 + y5

    month = trunc(J / 30.)
    J = J - month * 30

    return year + 1, month + 1, trunc(J) + 1


def _from_jd_equinox(jd):
    '''Calculate the FR day using the equinox as day 1'''
    jd = trunc(jd) + 0.5
    equinoxe = premier_da_la_annee(jd)

    an = gregorian.from_jd(equinoxe)[0] - YEAR_EPOCH
    mois = trunc((jd - equinoxe) / 30.) + 1
    jour = int((jd - equinoxe) % 30) + 1

    return (an, mois, jour)


def decade(jour):
    return trunc(jour / 100.) + 1


def day_name(month, day):
    return french_republican_days[month][day - 1]


def from_gregorian(year, month, day, method=None):
    return from_jd(gregorian.to_jd(year, month, day), method=method)


def to_gregorian(an, mois, jour, method=None):
    return gregorian.from_jd(to_jd(an, mois, jour, method=method))


def format(an, mois, jour):
    return "{0} {1} {2}".format(jour, MOIS[mois - 1], an)
