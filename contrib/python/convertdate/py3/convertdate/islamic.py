# -*- coding: utf-8 -*-
# This file is part of convertdate.
# http://github.com/fitnr/convertdate
# Licensed under the MIT license:
# http://opensource.org/licenses/MIT
# Copyright (c) 2016, fitnr <fitnr@fakeisthenewreal>
"""Convert dates between the Hijri calendar and the Gregorian and Julian calendars."""
from math import ceil, floor

from . import gregorian
from .utils import jwday, monthcalendarhelper

EPOCH = 1948439.5
WEEKDAYS = ("al-'ahad", "al-'ithnayn", "ath-thalatha'", "al-'arb`a'", "al-khamis", "al-jum`a", "as-sabt")
MONTHS = [
    "al-Muḥarram",
    "Ṣafar",
    "Rabīʿ al-ʾAwwal",
    "Rabīʿ ath-Thānī",
    "Jumādā al-ʾAwwal",
    "Jumādā ath-Thāniyah",
    "Rajab",
    "Shaʿbān",
    "Ramaḍān",
    "Shawwāl",
    "Zū al-Qaʿdah",
    "Zū al-Ḥijjah",
]
HAS_29_DAYS = (2, 4, 6, 8, 10)
HAS_30_DAYS = (1, 3, 5, 7, 9, 11)


def leap(year):
    '''Is a given year a leap year in the Islamic calendar'''
    return (((year * 11) + 14) % 30) < 11


def to_jd(year, month, day):
    '''Determine Julian day count from Islamic date'''
    return (day + ceil(29.5 * (month - 1)) + (year - 1) * 354 + floor((3 + (11 * year)) / 30) + EPOCH) - 1


def from_jd(jd):
    '''Calculate Islamic date from Julian day'''
    jd = floor(jd) + 0.5
    year = floor(((30 * (jd - EPOCH)) + 10646) / 10631)
    month = min(12, ceil((jd - (29 + to_jd(year, 1, 1))) / 29.5) + 1)
    day = int(jd - to_jd(year, month, 1)) + 1
    return (year, month, day)


def to_jd_gregorianyear(gregorianyear, islamic_month, islamic_day):
    # Gregorian year is either 578 or 623 years greater than Islamic year
    # we'll first try 622 if conversion to gregorian isn't the same
    # year that was passed to this method, then it must be 623.
    jan1 = gregorian.to_jd(gregorianyear, 1, 1)
    yi, mi, _ = from_jd(jan1)

    if mi > islamic_month:
        yi = yi + 1

    return to_jd(yi, islamic_month, islamic_day)


def from_gregorian(year, month, day):
    return from_jd(gregorian.to_jd(year, month, day))


def to_gregorian(year, month, day):
    return gregorian.from_jd(to_jd(year, month, day))


def month_length(year, month):
    if month in HAS_30_DAYS or (month == 12 and leap(year)):
        return 30

    return 29


def monthcalendar(year, month):
    start_weekday = jwday(to_jd(year, month, 1))
    monthlen = month_length(year, month)
    return monthcalendarhelper(start_weekday, monthlen)


def format(year, month, day):
    """Convert an Islamic date into a string with the format DD MONTH YYYY."""
    # pylint: disable=redefined-builtin
    return "{0:d} {1:} {2:d}".format(day, MONTHS[month - 1], year)
