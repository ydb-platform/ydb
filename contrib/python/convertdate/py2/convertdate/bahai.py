# -*- coding: utf-8 -*-

# This file is part of convertdate.
# http://github.com/fitnr/convertdate

# Licensed under the MIT license:
# http://opensource.org/licenses/MIT
# Copyright (c) 2016, fitnr <fitnr@fakeisthenewreal>
from math import trunc, ceil
from calendar import isleap
from pymeeus.Sun import Sun
from pymeeus.Epoch import Epoch
from pymeeus.Angle import Angle
from . import gregorian
from .utils import monthcalendarhelper, jwday


EPOCH = 2394646.5
EPOCH_GREGORIAN_YEAR = 1844

TEHRAN = 51.4215, 35.6944

WEEKDAYS = ("Jamál", "Kamál", "Fidál", "Idál", "Istijlál", "Istiqlál", "Jalál")

MONTHS = ("Bahá", "Jalál", "Jamál", "‘Aẓamat", "Núr", "Raḥmat", "Kalimát", "Kamál", "Asmá’",
          "‘Izzat", "Mashíyyat", "‘Ilm", "Qudrat", "Qawl", "Masá’il", "Sharaf", "Sulṭán", "Mulk",
          "Ayyám-i-Há", "‘Alá")

ENGLISH_MONTHS = ("Splendor", "Glory", "Beauty", "Grandeur", "Light", "Mercy", "Words",
                  "Perfection", "Names", "Might", "Will", "Knowledge", "Power", "Speech", "Questions",
                  "Honour", "Sovereignty", "Dominion", "Days of Há", "Loftiness")


def gregorian_nawruz(year):
    '''
        Return Nawruz in the Gregorian calendar.
        Returns a tuple (month, day), where month is always 3
    '''
    if year == 2059:
        return 3, 20

    # Timestamp of spring equinox.
    equinox = Sun.get_equinox_solstice(year, "spring")

    # Get times of sunsets in Tehran near vernal equinox.
    x, y = Angle(TEHRAN[0]), Angle(TEHRAN[1])
    days = trunc(equinox.get_date()[2]), ceil(equinox.get_date()[2])

    for day in days:
        sunset = Epoch(year, 3, day).rise_set(y, x)[1]
        if sunset > equinox:
            return 3, day


def to_jd(year, month, day):
    '''Determine Julian day from Bahai date'''
    if month <= 18:
        gy = year - 1 + EPOCH_GREGORIAN_YEAR
        n_month, n_day = gregorian_nawruz(gy)
        return gregorian.to_jd(gy, n_month, n_day - 1) + day + (month - 1) * 19

    return to_jd(year, month - 1, day) + month_length(year, month)


def from_jd(jd):
    '''Calculate Bahai date from Julian day'''
    jd = trunc(jd) + 0.5
    g = gregorian.from_jd(jd)
    gy = g[0]
    n_month, n_day = gregorian_nawruz(gy)

    bstarty = EPOCH_GREGORIAN_YEAR

    if jd <= gregorian.to_jd(gy, n_month, 20):
        x = 1
    else:
        x = 0
    # verify this next line...
    bys = gy - (bstarty + (((gregorian.to_jd(gy, 1, 1) <= jd) and x)))

    year = bys + 1
    days = jd - to_jd(year, 1, 1)
    bld = to_jd(year, n_day - 1, 1)

    if jd >= bld:
        month = 20
    else:
        month = trunc(days / 19) + 1
    day = int((jd + 1) - to_jd(year, month, 1))

    return year, month, day


def from_gregorian(year, month, day):
    return from_jd(gregorian.to_jd(year, month, day))


def to_gregorian(year, month, day):
    return gregorian.from_jd(to_jd(year, month, day))


def month_length(year, month):
    gy = year + EPOCH_GREGORIAN_YEAR - 1

    if month == 19:
        _, nawruz_future = gregorian_nawruz(gy + 1)
        _, nawruz_past = gregorian_nawruz(gy)
        length_of_year = nawruz_future + 365 - nawruz_past

        if isleap(gy + 1):
            length_of_year = length_of_year + 1

        return length_of_year - 19 * 19

    return 19


def monthcalendar(year, month):
    start_weekday = jwday(to_jd(year, month, 1))
    monthlen = month_length(year, month)
    return monthcalendarhelper(start_weekday, monthlen)
