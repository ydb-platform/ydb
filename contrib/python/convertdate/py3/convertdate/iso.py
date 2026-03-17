# -*- coding: utf-8 -*-
# This file is part of convertdate.
# http://github.com/fitnr/convertdate
# Licensed under the MIT license:
# http://opensource.org/licenses/MIT
# Copyright (c) 2016, fitnr <fitnr@fakeisthenewreal>
"""Conversions for the `ISO calendar system <https://en.wikipedia.org/wiki/ISO_week_date>`__."""
from calendar import isleap
from datetime import date

from . import gregorian
from .utils import jwday, n_weeks

MON = 0
TUE = 1
WED = 2
THU = 3
FRI = 4
SAT = 5
SUN = 6


def to_jd(year, week, day):
    '''Return Julian day count of given ISO year, week, and day'''
    return day + n_weeks(SUN, gregorian.to_jd(year - 1, 12, 28), week)


def from_jd(jd):
    '''Return tuple of ISO (year, week, day) for Julian day'''
    return from_gregorian(*gregorian.from_jd(jd))


def weeks_per_year(year):
    '''Number of ISO weeks in a year'''
    # 53 weeks: any year starting on Thursday and any leap year starting on Wednesday
    jan1 = jwday(gregorian.to_jd(year, 1, 1))

    if jan1 == THU or (jan1 == WED and isleap(year)):
        return 53

    return 52


def from_gregorian(year, month, day):
    return date(year, month, day).isocalendar()


def to_gregorian(year, week, day):
    return gregorian.from_jd(to_jd(year, week, day))


def format(year, week, day):
    # pylint: disable=redefined-builtin
    return "{}-W{:02}-{}".format(year, week, day)
