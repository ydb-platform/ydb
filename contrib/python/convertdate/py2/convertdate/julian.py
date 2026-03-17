# -*- coding: utf-8 -*-

# This file is part of convertdate.
# http://github.com/fitnr/convertdate

# Licensed under the MIT license:
# http://opensource.org/licenses/MIT
# Copyright (c) 2016, fitnr <fitnr@fakeisthenewreal>
from math import trunc
from .utils import jwday, monthcalendarhelper
from .gregorian import to_jd as gregorian_to_jd, from_jd as gregorian_from_jd

J0000 = 1721424.5  # Julian date of Gregorian epoch: 0000-01-01
J1970 = 2440587.5  # Julian date at Unix epoch: 1970-01-01
JMJD = 2400000.5  # Epoch of Modified Julian Date system

JULIAN_EPOCH = 1721423.5
J2000 = 2451545.0  # Julian day of J2000 epoch
JULIANCENTURY = 36525.0  # Days in Julian century

HAVE_30_DAYS = (4, 6, 9, 11)
HAVE_31_DAYS = (1, 3, 5, 7, 8, 10, 12)


def leap(year):
    if year % 4 and year > 0:
        return 0
    else:
        return 3


def month_length(year, month):
    if month == 2:
        daysinmonth = 29 if leap(year) else 28
    else:
        daysinmonth = 30 if month in HAVE_30_DAYS else 31

    return daysinmonth


def legal_date(year, month, day):
    '''Check if this is a legal date in the Julian calendar'''
    daysinmonth = month_length(year, month)

    if not (0 < day <= daysinmonth):
        raise ValueError("Month {} doesn't have a day {}".format(month, day))

    return True


def from_jd(jd):
    '''Calculate Julian calendar date from Julian day'''

    jd += 0.5
    z = trunc(jd)

    a = z
    b = a + 1524
    c = trunc((b - 122.1) / 365.25)
    d = trunc(365.25 * c)
    e = trunc((b - d) / 30.6001)

    if trunc(e < 14):
        month = e - 1
    else:
        month = e - 13

    if trunc(month > 2):
        year = c - 4716
    else:
        year = c - 4715

    day = b - d - trunc(30.6001 * e)

    return (year, month, day)


def to_jd(year, month, day):
    '''Convert to Julian day using astronomical years (0 = 1 BC, -1 = 2 BC)'''

    legal_date(year, month, day)

    # Algorithm as given in Meeus, Astronomical Algorithms, Chapter 7, page 61

    if month <= 2:
        year -= 1
        month += 12

    return (trunc((365.25 * (year + 4716))) + trunc((30.6001 * (month + 1))) + day) - 1524.5


def from_gregorian(year, month, day):
    return from_jd(gregorian_to_jd(year, month, day))


def to_gregorian(year, month, day):
    return gregorian_from_jd(to_jd(year, month, day))


def monthcalendar(year, month):
    start_weekday = jwday(to_jd(year, month, 1))
    monthlen = month_length(year, month)

    return monthcalendarhelper(start_weekday, monthlen)
