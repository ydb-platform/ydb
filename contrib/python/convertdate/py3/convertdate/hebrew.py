# -*- coding: utf-8 -*-
# This file is part of convertdate.
# http://github.com/fitnr/convertdate
# Licensed under the MIT license:
# http://opensource.org/licenses/MIT
# Copyright (c) 2016, fitnr <fitnr@fakeisthenewreal>
import warnings
from math import floor

from . import gregorian
from .utils import jwday, monthcalendarhelper

EPOCH = 347995.5
HEBREW_YEAR_OFFSET = 3760

# Hebrew months
NISAN = 1
IYYAR = 2
SIVAN = 3
TAMMUZ = 4
AV = 5
ELUL = 6
TISHRI = 7
HESHVAN = 8
KISLEV = 9
TEVETH = 10
SHEVAT = 11
ADAR = 12
VEADAR = 13

MONTHS = [
    'Nisan',
    'Iyyar',
    'Sivan',
    'Tammuz',
    'Av',
    'Elul',
    'Tishri',
    'Heshvan',
    'Kislev',
    'Teveth',
    'Shevat',
    'Adar',
    'Adar Bet',
]

MONTHS_HEB = [
    'ניסן',
    'אייר',
    'סיוון',
    'תמוז',
    'אב',
    'אלול',
    'תשרי',
    'חשוון',
    'כסלו',
    'טבת',
    'שבט',
    'אדר',
    'אדר ב'
]


def leap(year):
    # Is a given Hebrew year a leap year ?
    return (((year * 7) + 1) % 19) < 7


def year_months(year):
    '''How many months are there in a Hebrew year (12 = normal, 13 = leap)'''
    if leap(year):
        return VEADAR

    return ADAR


def delay_1(year):
    '''Test for delay of start of the ecclesiastical new year to
    avoid improper weekdays for holidays.'''
    # Sunday, Wednesday, and Friday as start of the new year.
    months = floor(((235 * year) - 234) / 19)
    parts = 12084 + (13753 * months)
    day = (months * 29) + floor(parts / 25920)

    if ((3 * (day + 1)) % 7) < 3:
        day += 1

    return day


def delay_2(year):
    '''Check for delay in start of the ecclesiastical new year due to length
    of adjacent years'''
    last = delay_1(year - 1)
    present = delay_1(year)
    next_ = delay_1(year + 1)

    if next_ - present == 356:
        return 2

    if present - last == 382:
        return 1

    return 0


def year_days(year):
    '''How many days are in a Hebrew year ?'''
    return to_jd(year + 1, 7, 1) - to_jd(year, 7, 1)



def month_length(year, month):
    '''How many days are in a given month of a given year'''
    if month > VEADAR:
        raise ValueError("Incorrect month index")

    # First of all, dispose of fixed-length 29 day months
    if month in (IYYAR, TAMMUZ, ELUL, TEVETH, VEADAR):
        return 29

    # If it's not a leap year, Adar has 29 days
    if month == ADAR and not leap(year):
        return 29

    # If it's Heshvan, days depend on length of year
    if month == HESHVAN and (year_days(year) % 10) != 5:
        return 29

    # Similarly, Kislev varies with the length of year
    if month == KISLEV and (year_days(year) % 10) == 3:
        return 29

    # Nope, it's a 30 day month
    return 30

def month_days(year, month):
    # retain for backwards compatibility, but warn that this is deprecated
    warnings.warn("month_days is deprecated, please use month_length",
        DeprecationWarning, stacklevel=2)
    return month_length(year, month)


def to_jd(year, month, day):
    months = year_months(year)
    jd = EPOCH + delay_1(year) + delay_2(year) + day + 1

    if month < TISHRI:
        for m in range(TISHRI, months + 1):
            jd += month_days(year, m)

        for m in range(NISAN, month):
            jd += month_days(year, m)
    else:
        for m in range(TISHRI, month):
            jd += month_days(year, m)

    return int(jd) + 0.5


def from_jd(jd):
    jd = floor(jd) + 0.5
    count = floor(((jd - EPOCH) * 98496.0) / 35975351.0)
    year = count - 1
    i = count
    while jd >= to_jd(i, TISHRI, 1):
        i += 1
        year += 1

    if jd < to_jd(year, NISAN, 1):
        first = 7
    else:
        first = 1

    month = i = first
    while jd > to_jd(year, i, month_days(year, i)):
        i += 1
        month += 1

    day = int(jd - to_jd(year, month, 1)) + 1
    return (year, month, day)


def to_civil(year, month, day):
    """Convert a date in the ecclestical calendar (year starts in Nisan) to
    the civil calendar (year starts in Tishrei)."""
    if month >= TISHRI:
        year = year + 1
    return year, month, day


def to_jd_gregorianyear(gregorianyear, hebrew_month, hebrew_day):
    '''Returns the Gregorian date when a given Hebrew month and year within a given Gregorian year.'''
    # gregorian year is either 3760 or 3761 years less than hebrew year
    # we'll first try 3760 if conversion to gregorian isn't the same
    # year that was passed to this method, then it must be 3761.
    for y in (gregorianyear + HEBREW_YEAR_OFFSET, gregorianyear + HEBREW_YEAR_OFFSET + 1):
        jd = to_jd(y, hebrew_month, hebrew_day)
        gd = gregorian.from_jd(jd)
        if gd[0] == gregorianyear:
            break

        jd = None

    if not jd:  # should never occur, but just incase...
        raise ValueError("Could not determine gregorian year")

    return gregorian.to_jd(gd[0], gd[1], gd[2])


def from_gregorian(year, month, day):
    return from_jd(gregorian.to_jd(year, month, day))


def to_gregorian(year, month, day):
    return gregorian.from_jd(to_jd(year, month, day))


def monthcalendar(year, month):
    start_weekday = jwday(to_jd(year, month, 1))
    monthlen = month_days(year, month)
    return monthcalendarhelper(start_weekday, monthlen)


def format(year, month, day, lang=None):
    """Convert a Hebrew date into a string with the format DD MONTH YYYY."""
    # pylint: disable=redefined-builtin
    lang = lang or "en"
    if lang[0:2] == "he":
        month_name = MONTHS_HEB[month - 1]
    else:
        month_name = MONTHS[month - 1]
    return "{0} {1} {2}".format(day, month_name, year)
