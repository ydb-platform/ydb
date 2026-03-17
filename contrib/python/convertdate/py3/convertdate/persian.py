# -*- coding: utf-8 -*-
# This file is part of convertdate.
# http://github.com/fitnr/convertdate
# Licensed under the MIT license:
# http://opensource.org/licenses/MIT
# Copyright (c) 2016, fitnr <fitnr@fakeisthenewreal>
"""
The modern Persian calendar, or the Solar Hijri calendar, was adopted in 1911.
It consists of twelve months of 30 or 31 days. The new year always falls on the
March equinox.
"""
from math import ceil, floor

from pymeeus.Epoch import Epoch
from pymeeus.Sun import Sun

from . import gregorian
from .utils import TROPICALYEAR, jwday, monthcalendarhelper

EPOCH = 1948320.5
WEEKDAYS = ("Doshanbeh", "Seshhanbeh", "Chaharshanbeh", "Panjshanbeh", "Jomeh", "Shanbeh", "Yekshanbeh")

MONTHS = [
    "Farvardin",
    "Ordibehesht",
    "Khordad",
    "Tir",
    "Mordad",
    "Shahrivar",
    "Mehr",
    "Aban",
    "Azar",
    "Dey",
    "Bahman",
    "Esfand",
]

HAS_31_DAYS = (1, 2, 3, 4, 5, 6)
HAS_30_DAYS = (7, 8, 9, 10, 11)


def leap(year):
    '''Is a given year a leap year in the Persian calendar ?'''
    return (to_jd(year + 1, 1, 1) - to_jd(year, 1, 1)) > 365


def equinox_jd(gyear):
    """Calculate Julian day during which the March equinox, reckoned from the
    Tehran meridian, occurred for a given Gregorian year."""
    mean_jd = Sun.get_equinox_solstice(gyear, target='spring')
    deltat_jd = mean_jd - Epoch.tt2ut(gyear, 3) / (24 * 60 * 60.)
    # Apparent JD in universal time
    apparent_jd = deltat_jd + (Sun.equation_of_time(deltat_jd)[0] / (24 * 60.))
    # Correct for meridian of Tehran + 52.5 degrees
    return floor(apparent_jd.jde() + (52.5 / 360))


def last_equinox_jd(jd):
    """Return the Julian date of spring equinox immediately preceeding the
    given Julian date."""
    guessyear = gregorian.from_jd(jd)[0]
    last_equinox = equinox_jd(guessyear)

    while last_equinox > jd:
        guessyear = guessyear - 1
        last_equinox = equinox_jd(guessyear)

    next_equinox = last_equinox - 1

    while not last_equinox <= jd < next_equinox:
        last_equinox = next_equinox
        guessyear = guessyear + 1
        next_equinox = equinox_jd(guessyear)

    return last_equinox


def jd_to_pyear(jd):
    """
    Determine the year in the Persian astronomical calendar in which a given
    Julian day falls.

    Returns:
        tuple - (Persian year, Julian day number containing equinox for this year)
    """
    lasteq = last_equinox_jd(jd)
    return round((lasteq - EPOCH) / TROPICALYEAR) + 1, lasteq


def to_jd(year, month, day):
    '''Determine Julian day from Persian date'''
    guess = (EPOCH - 1) + (TROPICALYEAR * ((year - 1) - 1))
    y0, equinox = year - 1, 0

    while y0 < year:
        y0, equinox = jd_to_pyear(guess)
        guess = equinox + TROPICALYEAR + 2

    if month <= 7:
        m = (month - 1) * 31
    else:
        m = ((month - 1) * 30) + 6

    return equinox + m + day + 0.5


def from_jd(jd):
    '''Calculate Persian date from Julian day'''
    jd = floor(jd) + 0.5
    equinox = last_equinox_jd(jd)
    year = round((equinox - EPOCH) / TROPICALYEAR) + 1
    yday = jd - (equinox + 0.5)

    if yday <= 186:
        month = ceil(yday / 31)
        day = yday - ((month - 1) * 31)
    else:
        month = ceil((yday - 6) / 30)
        day = yday - ((month - 1) * 30) - 6

    return int(year), int(month), int(day)


def from_gregorian(year, month, day):
    return from_jd(gregorian.to_jd(year, month, day))


def to_gregorian(year, month, day):
    return gregorian.from_jd(to_jd(year, month, day))


def month_length(year, month):
    if month in HAS_30_DAYS or (month == 12 and leap(year)):
        return 30
    if month in HAS_31_DAYS:
        return 31

    return 29


def monthcalendar(year, month):
    start_weekday = jwday(to_jd(year, month, 1))
    monthlen = month_length(year, month)
    return monthcalendarhelper(start_weekday, monthlen)


def format(year, month, day):
    """Convert a Persian date into a string with the format DD MONTH YYYY."""
    # pylint: disable=redefined-builtin
    return "{0:d} {1:} {2:d}".format(day, MONTHS[month - 1], year)
