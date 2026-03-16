# -*- coding: utf-8 -*-

# This file is part of convertdate.
# http://github.com/fitnr/convertdate

# Licensed under the MIT license:
# http://opensource.org/licenses/MIT
# Copyright (c) 2016, fitnr <fitnr@fakeisthenewreal>
from calendar import isleap
from . import gregorian
from .utils import floor
from .data import positivist as data

'''
    Convert between Gregorian/Julian Day and Comte's Positivist calendar.
    The Positivist calendar has 13 months and one or two festival days.
    Festival days are given as the fourteenth month.
    The Gregorian date 1789-01-01 is Positivist 0001-01-01.
'''

# Positivist calendar has 13 28-day months and one festival day

EPOCH = 2374479.5

YEAR_EPOCH = 1789

DAYS_IN_YEAR = 365

MONTHS = (
    'Moses', 'Homer', 'Aristotle', 'Archimedes',
    'Caesar', 'Saint Paul', 'Charlemagne', 'Dante',
    'Gutenberg', 'Shakespeare', 'Descartes', 'Frederic',
    'Bichat', ''
)


def legal_date(year, month, day):
    '''Checks if a given date is a legal positivist date'''
    try:
        assert year >= 1
        assert 0 < month <= 14
        assert 0 < day <= 28
        if month == 14:
            if isleap(year + YEAR_EPOCH - 1):
                assert day <= 2
            else:
                assert day == 1

    except AssertionError:
        raise ValueError("Invalid Positivist date: ({}, {}, {})".format(year, month, day))

    return True


def to_jd(year, month, day):
    '''Convert a Positivist date to Julian day count.'''
    legal_date(year, month, day)
    gyear = year + YEAR_EPOCH - 1

    return (
        gregorian.EPOCH - 1 + (365 * (gyear - 1)) +
        floor((gyear - 1) / 4) + (-floor((gyear - 1) / 100)) +
        floor((gyear - 1) / 400) + (month - 1) * 28 + day
    )


def from_jd(jd):
    '''Convert a Julian day count to Positivist date.'''
    try:
        assert jd >= EPOCH
    except AssertionError:
        raise ValueError('Invalid Julian day')

    depoch = floor(jd - 0.5) + 0.5 - gregorian.EPOCH

    quadricent = floor(depoch / gregorian.INTERCALATION_CYCLE_DAYS)
    dqc = depoch % gregorian.INTERCALATION_CYCLE_DAYS

    cent = floor(dqc / gregorian.LEAP_SUPPRESSION_DAYS)
    dcent = dqc % gregorian.LEAP_SUPPRESSION_DAYS

    quad = floor(dcent / gregorian.LEAP_CYCLE_DAYS)
    dquad = dcent % gregorian.LEAP_CYCLE_DAYS

    yindex = floor(dquad / gregorian.YEAR_DAYS)
    year = (
        quadricent * gregorian.INTERCALATION_CYCLE_YEARS +
        cent * gregorian.LEAP_SUPPRESSION_YEARS +
        quad * gregorian.LEAP_CYCLE_YEARS + yindex
    )

    if yindex == 4:
        yearday = 365
        year = year - 1

    else:
        yearday = (
            depoch -
            quadricent * gregorian.INTERCALATION_CYCLE_DAYS -
            cent * gregorian.LEAP_SUPPRESSION_DAYS -
            quad * gregorian.LEAP_CYCLE_DAYS -
            yindex * gregorian.YEAR_DAYS
        )

    month = floor(yearday / 28)

    return (year - YEAR_EPOCH + 2, month + 1, int(yearday - (month * 28)) + 1)


def from_gregorian(year, month, day):
    return from_jd(gregorian.to_jd(year, month, day))


def to_gregorian(year, month, day):
    return gregorian.from_jd(to_jd(year, month, day))


def dayname(year, month, day):
    '''
    Give the name of the month and day for a given date.

    Returns:
        tuple month_name, day_name
    '''
    legal_date(year, month, day)

    yearday = (month - 1) * 28 + day

    if isleap(year + YEAR_EPOCH - 1):
        dname = data.day_names_leap[yearday - 1]
    else:
        dname = data.day_names[yearday - 1]

    return MONTHS[month - 1], dname


def weekday(day):
    '''
        Gives the weekday (0=Monday) of a positivist month and day.
        Note that the festival month does not have a day.
    '''
    return (day % 7) - 1


def festival(month, day):
    '''
    Gives the festival day for a month and day.
    Returns None if inapplicable.
    '''
    return data.festivals.get((month, day))
