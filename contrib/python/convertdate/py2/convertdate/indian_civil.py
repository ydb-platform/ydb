# -*- coding: utf-8 -*-

# This file is part of convertdate.
# http://github.com/fitnr/convertdate

# Licensed under the MIT license:
# http://opensource.org/licenses/MIT
# Copyright (c) 2016, fitnr <fitnr@fakeisthenewreal>
from math import trunc
from calendar import isleap
from . import gregorian
from .utils import jwday, monthcalendarhelper

# 0 = Sunday
WEEKDAYS = (
    "Ravivāra",
    "Somavāra",
    "Maṅgalavāra",
    "Budhavāra",
    "Guruvāra",
    "Śukravāra",
    "Śanivāra",
)

MONTHS = (
    "Chaitra",
    "Vaishākha",
    "Jyēshtha",
    "Āshādha",
    "Shrāvana",
    "Bhādrapada",
    "Āshwin",
    "Kārtika",
    "Mārgashīrsha",
    "Pausha",
    "Māgha",
    "Phālguna",
)

HAVE_31_DAYS = (2, 3, 4, 5, 6)
HAVE_30_DAYS = (7, 8, 9, 10, 11, 12)

SAKA_EPOCH = 78


def to_jd(year, month, day):
    '''Obtain Julian day for Indian Civil date'''

    gyear = year + 78
    leap = isleap(gyear)
    # // Is this a leap year ?

    # 22 - leap = 21 if leap, 22 non-leap
    start = gregorian.to_jd(gyear, 3, 22 - leap)
    if leap:
        Caitra = 31
    else:
        Caitra = 30

    if month == 1:
        jd = start + (day - 1)
    else:
        jd = start + Caitra
        m = month - 2
        m = min(m, 5)
        jd += m * 31
        if month >= 8:
            m = month - 7
            jd += m * 30

        jd += day - 1

    return jd


def from_jd(jd):
    '''Calculate Indian Civil date from Julian day
    Offset in years from Saka era to Gregorian epoch'''

    start = 80
    # Day offset between Saka and Gregorian

    jd = trunc(jd) + 0.5
    greg = gregorian.from_jd(jd)  # Gregorian date for Julian day
    leap = isleap(greg[0])  # Is this a leap year?
    # Tentative year in Saka era
    year = greg[0] - SAKA_EPOCH
    # JD at start of Gregorian year
    greg0 = gregorian.to_jd(greg[0], 1, 1)
    yday = jd - greg0  # Day number (0 based) in Gregorian year

    if leap:
        Caitra = 31  # Days in Caitra this year
    else:
        Caitra = 30

    if yday < start:
        # Day is at the end of the preceding Saka year
        year -= 1
        yday += Caitra + (31 * 5) + (30 * 3) + 10 + start

    yday -= start
    if yday < Caitra:
        month = 1
        day = yday + 1
    else:
        mday = yday - Caitra
        if (mday < (31 * 5)):
            month = trunc(mday / 31) + 2
            day = (mday % 31) + 1
        else:
            mday -= 31 * 5
            month = trunc(mday / 30) + 7
            day = (mday % 30) + 1

    return (year, month, int(day))


def from_gregorian(year, month, day):
    return from_jd(gregorian.to_jd(year, month, day))


def to_gregorian(year, month, day):
    return gregorian.from_jd(to_jd(year, month, day))


def month_length(year, month):
    if month in HAVE_31_DAYS or (month == 1 and isleap(year - SAKA_EPOCH)):
        return 31
    return 30


def monthcalendar(year, month):
    start_weekday = jwday(to_jd(year, month, 1))
    monthlen = month_length(year, month)
    return monthcalendarhelper(start_weekday, monthlen)
