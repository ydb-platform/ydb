# -*- coding: utf-8 -*-
# This file is part of convertdate.
# http://github.com/fitnr/convertdate
# Licensed under the MIT license:
# http://opensource.org/licenses/MIT
# Copyright (c) 2016, fitnr <fitnr@fakeisthenewreal>
"""Helper utilities useful in several converters."""
import calendar
import math

TROPICALYEAR = 365.24219878  # Mean solar tropical year


def amod(a, b):
    '''Modulus function which returns numerator if modulus is zero'''
    modded = int(a % b)
    return b if modded == 0 else modded


def jwday(j):
    '''Calculate day of week from Julian day. Consider using ``calendar.weekday``!'''
    return math.trunc((j + 0.5)) % 7


def weekday_before(weekday, jd):
    return jd - jwday(jd - weekday)


def search_weekday(weekday, jd, direction, offset):
    '''
    Determine the Julian date for the next or previous weekday

    Arguments:
        weekday (int): Day of week desired, 0 = Monday
        jd (float): Julian date to begin search
        direction(int): 1 = next weekday, -1 = last weekday
        offset(int): Offset from jd to begin search.
    '''
    return weekday_before(weekday, jd + (direction * offset))


#  Utility weekday functions, just wrappers for search_weekday


def nearest_weekday(weekday, jd):
    return search_weekday(weekday, jd, 1, 3)


def next_weekday(weekday, jd):
    return search_weekday(weekday, jd, 1, 7)


def next_or_current_weekday(weekday, jd):
    return search_weekday(weekday, jd, 1, 6)


def previous_weekday(weekday, jd):
    return search_weekday(weekday, jd, -1, 1)


def previous_or_current_weekday(weekday, jd):
    return search_weekday(weekday, jd, 1, 0)


def n_weeks(weekday, jd, nthweek):
    j = 7 * nthweek

    if nthweek > 0:
        j += previous_weekday(weekday, jd)
    else:
        j += next_weekday(weekday, jd)

    return j


def monthcalendarhelper(start_weekday, month_length):

    lpad = (start_weekday + 1) % 7

    days = [None] * lpad + list(range(1, 1 + month_length)) + 7 * [None]

    return [days[i : i + 7] for i in range(0, lpad + month_length, 7)]


def nth_day_of_month(n, weekday, month, year):
    """
    Return (year, month, day) tuple that represents nth weekday of month in year.
    If n==0, returns last weekday of month. Weekdays: Monday=0
    """
    if not 0 <= n <= 5:
        raise IndexError("Nth day of month must be 0-5. Received: {}".format(n))

    if not 0 <= weekday <= 6:
        raise IndexError("Weekday must be 0-6")

    firstday, daysinmonth = calendar.monthrange(year, month)

    # Get first WEEKDAY of month
    first_weekday_of_kind = 1 + (weekday - firstday) % 7

    if n == 0:
        # find last weekday of kind, which is 5 if these conditions are met, else 4
        if first_weekday_of_kind in [1, 2, 3] and first_weekday_of_kind + 28 <= daysinmonth:
            n = 5
        else:
            n = 4

    day = first_weekday_of_kind + ((n - 1) * 7)

    if day > daysinmonth:
        raise IndexError("No {}th day of month {}".format(n, month))

    return (year, month, day)
