# -*- coding: utf-8 -*-
# This file is part of convertdate.
# http://github.com/fitnr/convertdate
# Licensed under the MIT license:
# http://opensource.org/licenses/MIT
# Copyright (c) 2016, fitnr <fitnr@fakeisthenewreal>

"""
The Armenian calendar begins on 11 July 552 (Julian) and has two modes of
reckoning. The first is the invariant-length version consisting of 12 months
of 30 days each and five epagomenal days; the second is the version
established by Yovhannes Sarkawag in 1084, which fixed the first day of the
year with respect to the Julian calendar and added a sixth epagomenal day
every four years.

By default the invariant calendar is used, but the Sarkawag calendar can be
used beginning with the Armenian year 533 (11 August 1084) by passing the
parameter ``method='sarkawag'`` to the relevant functions.
"""

from math import trunc

from . import gregorian, julian
from .utils import jwday, monthcalendarhelper

EPOCH = 1922501.5  # beginning of proleptic year 0, day 0 of the moveable calendar
EPOCH_SARKAWAG = 2117210.5  # last day of Sarkawag's first cycle
MONTHS = [
    "nawasard",
    "hoṙi",
    "sahmi",
    "trē",
    "kʿałocʿ",
    "aracʿ",
    "mehekan",
    "areg",
    "ahekan",
    "mareri",
    "margacʿ",
    "hroticʿ",
    "aweleacʿ",
]
MONTHS_ARM = [
    "նաւասարդ",
    "հոռի",
    "սահմի",
    "տրէ",
    "քաղոց",
    "արաց",
    "մեհեկան",
    "արեգ",
    "ահեկան",
    "մարերի",
    "մարգաց",
    "հրոտից",
    "աւելեաց",
]


def _valid_date(year, month, day, method=None):
    try:
        assert 1 <= month <= 13, "Month out of range"
    except AssertionError as error:
        raise ValueError from error

    day_max = 30
    method = method or "moveable"

    if method.lower() == "sarkawag":
        year_min = 533
        if month == 13:
            day_max = 6 if leap(year) else 5

    else:
        year_min = 1
        if month == 13:
            day_max = 5

    try:
        assert year >= year_min, "Year out of range for Armenian calendar ({} method)".format(method)
    except AssertionError as err:
        raise ValueError from err

    try:
        assert 1 <= day <= day_max, "Day out of range"
    except AssertionError as err:
        raise ValueError from err

    return True


def leap(year):
    """Return true if the year was a leap year under the system of Sarkawag"""
    if year < 533:
        return False
    return year % 4 == 0


def to_jd(year, month, day, method=None):
    """Convert Armenian date to Julian day count. Use the method of Sarkawag if requested."""
    _valid_date(year, month, day, method)
    yeardays = (month - 1) * 30 + day
    if method == "sarkawag":
        yeardelta = year - 533
        leapdays = trunc(yeardelta / 4)
        return EPOCH_SARKAWAG + (365 * yeardelta) + leapdays + yeardays

    return EPOCH + (365 * year) + yeardays


def from_jd(jd, method=None):
    """Convert a Julian day count to an Armenian date. Use the method of Sarkawag if requested."""
    if method == "sarkawag":
        dc = jd - EPOCH_SARKAWAG
        if dc < 0:
            raise ValueError("Day count out of range for method")
        years = trunc(dc / 365.25)
        yeardays = dc - (365 * years + trunc(years / 4))
        if yeardays == 0:
            yeardays = 366 if years % 4 == 0 else 365
            years -= 1
        months = trunc((yeardays - 1) / 30)
        days = yeardays - (30 * months)
        return years + 533, months + 1, trunc(days)

    dc = jd - EPOCH

    if dc < 0:
        raise ValueError("Day count out of range")

    years = trunc((dc - 1) / 365)
    months = trunc(((dc - 1) % 365) / 30)
    days = dc - (365 * years) - (30 * months)

    return years, months + 1, trunc(days)


def to_julian(year, month, day, method=None):
    """Convert an Armenian date to a Julian date"""
    return julian.from_jd(to_jd(year, month, day, method))


def from_julian(year, month, day, method=None):
    """Convert a Julian date to an Armenian date"""
    return from_jd(julian.to_jd(year, month, day), method)


def to_gregorian(year, month, day, method=None):
    """Convert an Armenian date to a Gregorian date"""
    return gregorian.from_jd(to_jd(year, month, day, method))


def from_gregorian(year, month, day, method=None):
    """Convert a Gregorian date to an Armenian date"""
    return from_jd(gregorian.to_jd(year, month, day), method)


def month_length(year, month, method=None):
    if month > 13:
        raise ValueError("Requested month %d doesn't exist" % month)

    if month == 13:
        return 6 if (method == "sarkawag" and leap(year)) else 5

    return 30


def monthcalendar(year, month, method=None):
    """Returns a matrix representing a month’s calendar.
    Each row represents a week; days outside of the month are represented by zeros."""
    start_weekday = jwday(to_jd(year, month, 1, method))
    monthlen = month_length(year, month, method)
    return monthcalendarhelper(start_weekday, monthlen)


def format(year, month, day, lang=None):
    """Convert an Armenian date into a string with the format DD MONTH YYYY."""
    # pylint: disable=redefined-builtin
    lang = lang or "en"
    if lang[0:2] == 'hy' or lang[0:2] == 'am' or lang == 'arm':
        month_name = MONTHS_ARM[month - 1]
    else:
        month = month_name = MONTHS[month - 1]

    return "{0:d} {1:} {2:d}".format(day, month_name, year)


def tostring(year, month, day, lang=None):
    """Kept for backwards compatibility, the format function name will be standard across the library"""
    return format(year, month, day, lang)
