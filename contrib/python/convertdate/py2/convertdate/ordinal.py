# -*- coding: utf-8 -*-

# This file is part of convertdate.
# http://github.com/fitnr/convertdate

# Licensed under the MIT license:
# http://opensource.org/licenses/MIT
# Copyright (c) 2016, fitnr <fitnr@fakeisthenewreal>
from math import trunc
from calendar import isleap
from . import gregorian


def to_jd(year, dayofyear):
    '''Return Julian day count of given ISO year, and day of year'''
    return gregorian.to_jd(year, 1, 1) + dayofyear - 1


def from_jd(jd):
    return from_gregorian(*gregorian.from_jd(jd))


def from_gregorian(year, month, day):
    m = month + 1

    if m <= 3:
        m = m + 12

    leap = isleap(year)

    t = (trunc(30.6 * m) + day - 122 + 59 + leap) % (365 + leap)

    return year, t


def to_gregorian(year, dayofyear):
    leap = isleap(year)

    if dayofyear < 59 + leap:
        leap_adj = 0
    elif leap:
        leap_adj = 1
    else:
        leap_adj = 2

    month = trunc((((dayofyear - 1 + leap_adj) * 12) + 373) / 367)

    startofmonth = from_gregorian(year, month, 1)

    return year, month, dayofyear - startofmonth[1] + 1
