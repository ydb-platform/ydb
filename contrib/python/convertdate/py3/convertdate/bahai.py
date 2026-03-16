# -*- coding: utf-8 -*-
# This file is part of convertdate.
# http://github.com/fitnr/convertdate
# Licensed under the MIT license:
# http://opensource.org/licenses/MIT
# Copyright (c) 2016, fitnr <fitnr@fakeisthenewreal>

"""
The Bahá'í (Badí) calendar is a solar calendar with 19 months of 19 days.

Every four years, an intercalary period, Ayyam-i-Há, occurs between the 18th and 19th
months. Dates in this period are returned as month 19, and the month of ‘Alá is always
reported as month 20.

.. code-block:: python

   from convertdate import bahai
   # the first day of Ayyam-i-Ha:
   bahai.to_gregorian(175, 19, 1)
   # (2019, 2, 26)
   # The first day of 'Ala:
   bahai.to_gregorian(175, 20, 1)
   # (2019, 3, 2)

"""
from calendar import isleap
from math import ceil, trunc

from pymeeus.Angle import Angle
from pymeeus.Epoch import Epoch
from pymeeus.Sun import Sun

from . import gregorian
from .utils import jwday, monthcalendarhelper

EPOCH = 2394646.5
EPOCH_GREGORIAN_YEAR = 1844

TEHRAN = 51.4215, 35.6944

WEEKDAYS = ("Jamál", "Kamál", "Fidál", "Idál", "Istijlál", "Istiqlál", "Jalál")

MONTHS = (
    "Bahá",
    "Jalál",
    "Jamál",
    "‘Aẓamat",
    "Núr",
    "Raḥmat",
    "Kalimát",
    "Kamál",
    "Asmá’",
    "‘Izzat",
    "Mashíyyat",
    "‘Ilm",
    "Qudrat",
    "Qawl",
    "Masá’il",
    "Sharaf",
    "Sulṭán",
    "Mulk",
    "Ayyám-i-Há",
    "‘Alá",
)

ENGLISH_MONTHS = (
    "Splendor",
    "Glory",
    "Beauty",
    "Grandeur",
    "Light",
    "Mercy",
    "Words",
    "Perfection",
    "Names",
    "Might",
    "Will",
    "Knowledge",
    "Power",
    "Speech",
    "Questions",
    "Honour",
    "Sovereignty",
    "Dominion",
    "Days of Há",
    "Loftiness",
)

BAHA = 1
JALAL = 2
JAMAL = 3
AZAMAT = 4
NUR = 5
RAHMAT = 6
KALIMAT = 7
KAMAL = 8
ASMA = 9
IZZAT = 10
MASHIYYAT = 11
ILM = 12
QUDRAT = 13
QAWL = 14
MASAIL = 15
SHARAF = 16
SULTAN = 17
MULK = 18
AYYAMIHA = 19
ALA = 20


def gregorian_nawruz(year):
    """
    Return Nawruz in the Gregorian calendar.
    Returns a tuple (month, day), where month is always 3
    """
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

    raise ValueError("Couldn't find date of Nawruz.")


def to_jd(year, month, day):
    '''Determine Julian day from Bahai date'''
    if month <= 18:
        gy = year - 1 + EPOCH_GREGORIAN_YEAR
        n_month, n_day = gregorian_nawruz(gy)
        return gregorian.to_jd(gy, n_month, n_day - 1) + day + (month - 1) * 19
    if month == 19:
        # Count Ayyám-i-Há from the last day of Mulk
        return to_jd(year, month - 1, 19) + day
    # For the month of ‘Alá we will count _backwards_ from the next Naw Rúz
    gy = year + EPOCH_GREGORIAN_YEAR
    n_month, n_day = gregorian_nawruz(gy)
    return gregorian.to_jd(gy, n_month, n_day) - 20 + day


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


def format(year, month, day, lang=None):
    """Convert a Baha'i date into a string with the format DD MONTH YYYY."""
    # pylint: disable=redefined-builtin
    lang = lang or "en"
    if lang[0:2] == 'ar' or lang[0:2] == 'fa':
        month_name = MONTHS[month - 1]
    else:
        month_name = ENGLISH_MONTHS[month - 1]

    return "{0:d} {1:} {2:d}".format(day, month_name, year)
