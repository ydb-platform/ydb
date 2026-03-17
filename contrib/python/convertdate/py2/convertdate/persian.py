# -*- coding: utf-8 -*-

# This file is part of convertdate.
# http://github.com/fitnr/convertdate

# Licensed under the MIT license:
# http://opensource.org/licenses/MIT
# Copyright (c) 2016, fitnr <fitnr@fakeisthenewreal>
from math import trunc
from .utils import ceil, jwday, monthcalendarhelper
from . import gregorian

EPOCH = 1948320.5
WEEKDAYS = ("Doshanbeh", "Seshhanbeh",
            "Chaharshanbeh", "Panjshanbeh",
            "Jomeh", "Shanbeh", "Yekshanbeh")

HAS_31_DAYS = (1, 2, 3, 4, 5, 6)
HAS_30_DAYS = (7, 8, 9, 10, 11)


def leap(year):
    '''Is a given year a leap year in the Persian calendar ?'''
    if year > 0:
        y = 474
    else:
        y = 473

    return ((((((year - y % 2820) + 474) + 38) * 682) % 2816) < 682)


def to_jd(year, month, day):
    '''Determine Julian day from Persian date'''

    if year >= 0:
        y = 474
    else:
        y = 473
    epbase = year - y
    epyear = 474 + (epbase % 2820)

    if month <= 7:
        m = (month - 1) * 31
    else:
        m = (month - 1) * 30 + 6

    return day + m + trunc(((epyear * 682) - 110) / 2816) + (epyear - 1) * 365 + trunc(epbase / 2820) * 1029983 + (EPOCH - 1)


def from_jd(jd):
    '''Calculate Persian date from Julian day'''
    jd = trunc(jd) + 0.5

    depoch = jd - to_jd(475, 1, 1)
    cycle = trunc(depoch / 1029983)
    cyear = (depoch % 1029983)

    if cyear == 1029982:
        ycycle = 2820
    else:
        aux1 = trunc(cyear / 366)
        aux2 = cyear % 366
        ycycle = trunc(((2134 * aux1) + (2816 * aux2) + 2815) / 1028522) + aux1 + 1

    year = ycycle + (2820 * cycle) + 474

    if (year <= 0):
        year -= 1

    yday = (jd - to_jd(year, 1, 1)) + 1

    if yday <= 186:
        month = ceil(yday / 31)
    else:
        month = ceil((yday - 6) / 30)

    day = int(jd - to_jd(year, month, 1)) + 1

    return (year, month, day)


def from_gregorian(year, month, day):
    return from_jd(gregorian.to_jd(year, month, day))


def to_gregorian(year, month, day):
    return gregorian.from_jd(to_jd(year, month, day))


def month_length(year, month):
    if month in HAS_30_DAYS or (month == 12 and leap(year)):
        return 30
    elif month in HAS_31_DAYS:
        return 31

    return 29


def monthcalendar(year, month):
    start_weekday = jwday(to_jd(year, month, 1))
    monthlen = month_length(year, month)
    return monthcalendarhelper(start_weekday, monthlen)
