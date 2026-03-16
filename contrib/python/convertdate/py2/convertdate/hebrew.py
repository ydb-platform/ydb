# -*- coding: utf-8 -*-

# This file is part of convertdate.
# http://github.com/fitnr/convertdate

# Licensed under the MIT license:
# http://opensource.org/licenses/MIT
# Copyright (c) 2016, fitnr <fitnr@fakeisthenewreal>
from math import trunc
from . import gregorian
from .utils import jwday, monthcalendarhelper

EPOCH = 347995.5
HEBREW_YEAR_OFFSET = 3760

# Hebrew months
NISAN = 1
IYYAR = 2
SIVAN = 3
TAMMUZ = 4
AV = 5
ELUL = 6
TISHRI = 7
HESHVAN = 8
KISLEV = 9
TEVETH = 10
SHEVAT = 11
ADAR = 12
VEADAR = 13


def leap(year):
    # Is a given Hebrew year a leap year ?
    return (((year * 7) + 1) % 19) < 7


def year_months(year):
    '''How many months are there in a Hebrew year (12 = normal, 13 = leap)'''
    if leap(year):
        return 13
    else:
        return 12


def delay_1(year):
    '''Test for delay of start of new year and to avoid'''
    # Sunday, Wednesday, and Friday as start of the new year.
    months = trunc(((235 * year) - 234) / 19)
    parts = 12084 + (13753 * months)
    day = trunc((months * 29) + parts / 25920)

    if ((3 * (day + 1)) % 7) < 3:
        day += 1

    return day


def delay_2(year):
    '''Check for delay in start of new year due to length of adjacent years'''

    last = delay_1(year - 1)
    present = delay_1(year)
    next_ = delay_1(year + 1)

    if next_ - present == 356:
        return 2
    elif present - last == 382:
        return 1
    else:
        return 0


def year_days(year):
    '''How many days are in a Hebrew year ?'''
    return to_jd(year + 1, 7, 1) - to_jd(year, 7, 1)


def month_days(year, month):
    '''How many days are in a given month of a given year'''
    if month > 13:
        raise ValueError("Incorrect month index")

    # First of all, dispose of fixed-length 29 day months
    if month in (IYYAR, TAMMUZ, ELUL, TEVETH, VEADAR):
        return 29

    # If it's not a leap year, Adar has 29 days
    if month == ADAR and not leap(year):
        return 29

    # If it's Heshvan, days depend on length of year
    if month == HESHVAN and (year_days(year) % 10) != 5:
        return 29

    # Similarly, Kislev varies with the length of year
    if month == KISLEV and (year_days(year) % 10) == 3:
        return 29

    # Nope, it's a 30 day month
    return 30


def to_jd(year, month, day):
    months = year_months(year)
    jd = EPOCH + delay_1(year) + delay_2(year) + day + 1

    if month < 7:
        for mon in range(7, months + 1):
            jd += month_days(year, mon)

        for mon in range(1, month):
            jd += month_days(year, mon)
    else:
        for mon in range(7, month):
            jd += month_days(year, mon)

    return int(jd) + 0.5


def from_jd(jd):
    jd = trunc(jd) + 0.5
    count = trunc(((jd - EPOCH) * 98496.0) / 35975351.0)
    year = count - 1
    i = count
    while jd >= to_jd(i, 7, 1):
        i += 1
        year += 1

    if jd < to_jd(year, 1, 1):
        first = 7
    else:
        first = 1

    month = i = first
    while jd > to_jd(year, i, month_days(year, i)):
        i += 1
        month += 1

    day = int(jd - to_jd(year, month, 1)) + 1
    return (year, month, day)


def to_jd_gregorianyear(gregorianyear, hebrew_month, hebrew_day):
    # gregorian year is either 3760 or 3761 years less than hebrew year
    # we'll first try 3760 if conversion to gregorian isn't the same
    # year that was passed to this method, then it must be 3761.

    for y in (gregorianyear + HEBREW_YEAR_OFFSET, gregorianyear + HEBREW_YEAR_OFFSET + 1):
        jd = to_jd(y, hebrew_month, hebrew_day)
        gd = gregorian.from_jd(jd)
        if gd[0] == gregorianyear:
            break
        else:
            gd = None

    if not gd:  # should never occur, but just incase...
        raise ValueError("Could not determine gregorian year")

    # tuple: (y, m, d)
    return (gd[0], gd[1], gd[2])


def from_gregorian(year, month, day):
    return from_jd(gregorian.to_jd(year, month, day))


def to_gregorian(year, month, day):
    return gregorian.from_jd(to_jd(year, month, day))


def monthcalendar(year, month):
    start_weekday = jwday(to_jd(year, month, 1))
    monthlen = month_days(year, month)
    return monthcalendarhelper(start_weekday, monthlen)
