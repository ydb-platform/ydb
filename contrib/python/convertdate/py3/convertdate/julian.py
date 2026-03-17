# -*- coding: utf-8 -*-
# This file is part of convertdate.
# http://github.com/fitnr/convertdate
# Licensed under the MIT license:
# http://opensource.org/licenses/MIT
# Copyright (c) 2016, fitnr <fitnr@fakeisthenewreal>
"""
The Julian calendar was implemented by Julius Caesar in 45 BC as a reformation of
the Roman calendar. It is designed to follow the solar year, with a standard year
of 365 days and a quadrennial leap year with an intercalary day (29 February).

For the first several centuries of its use, the Julian calendar did not have a
single year-numbering system. The Romans initially specific years with the names of political
leaders. Later on, different areas employed different era with various epochs.
Between the sixth and eighth centuries, western Europe adopted the Anno Domini convention.

This numbering system does not include a year 0. However, for dates before 1,
this module uses the astronomical convention of including a year 0 to simplify
mathematical comparisons across epochs. To present a date in the standard
convention, use the :meth:`julian.format` function.
"""
from datetime import date
from math import floor

from .gregorian import from_jd as gregorian_from_jd
from .gregorian import to_jd as gregorian_to_jd
from .utils import jwday, monthcalendarhelper

J0000 = 1721424.5  # Julian date of Gregorian epoch: 0000-01-01
J1970 = 2440587.5  # Julian date at Unix epoch: 1970-01-01
JMJD = 2400000.5  # Epoch of Modified Julian Date system

JULIAN_EPOCH = 1721423.5
J2000 = 2451545.0  # Julian day of J2000 epoch
JULIANCENTURY = 36525.0  # Days in Julian century

HAVE_30_DAYS = (4, 6, 9, 11)
HAVE_31_DAYS = (1, 3, 5, 7, 8, 10, 12)


def leap(year):
    return year % 4 == 0


def month_length(year, month):
    if month == 2:
        daysinmonth = 29 if leap(year) else 28
    else:
        daysinmonth = 30 if month in HAVE_30_DAYS else 31

    return daysinmonth


def legal_date(year, month, day):
    '''Check if this is a legal date in the Julian calendar'''
    daysinmonth = month_length(year, month)

    if not 0 < day <= daysinmonth:
        raise ValueError("Month {} doesn't have a day {}".format(month, day))

    return True


def from_jd(jd):
    '''Calculate Julian calendar date from Julian day'''
    jd += 0.5
    a = floor(jd)
    b = a + 1524
    c = floor((b - 122.1) / 365.25)
    d = floor(365.25 * c)
    e = floor((b - d) / 30.6001)
    if e < 14:
        month = floor(e - 1)
    else:
        month = floor(e - 13)
    if month > 2:
        year = floor(c - 4716)
    else:
        year = floor(c - 4715)
    day = b - d - floor(30.6001 * e)
    return (year, month, day)


def to_jd(year, month, day):
    '''Convert to Julian day using astronomical years (0 = 1 BC, -1 = 2 BC)'''
    legal_date(year, month, day)

    # Algorithm as given in Meeus, Astronomical Algorithms, Chapter 7, page 61
    if month <= 2:
        year -= 1
        month += 12

    return (floor((365.25 * (year + 4716))) + floor((30.6001 * (month + 1))) + day) - 1524.5


def from_gregorian(year, month, day):
    '''Convert a Gregorian date to a Julian date.'''
    return from_jd(gregorian_to_jd(year, month, day))


def to_gregorian(year, month, day):
    '''Convert a Julian date to a Gregorian date.'''
    return gregorian_from_jd(to_jd(year, month, day))


def monthcalendar(year, month):
    '''
    Returns a matrix representing a month’s calendar. Each row represents a week;
    days outside of the month are represented by zeros. Each week begins with Sunday.
    '''
    start_weekday = jwday(to_jd(year, month, 1))
    monthlen = month_length(year, month)
    return monthcalendarhelper(start_weekday, monthlen)


def format(year, month, day, format_string="%-d %B %y"):
    # pylint: disable=redefined-builtin
    epoch = ''
    if year <= 0:
        year = (year - 1) * -1
        epoch = ' BCE'
    d = date(year, month, day)
    return d.strftime(format_string) + epoch
