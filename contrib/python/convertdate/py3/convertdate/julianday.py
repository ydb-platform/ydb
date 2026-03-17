# -*- coding: utf-8 -*-
# This file is part of convertdate.
# http://github.com/fitnr/convertdate
# Licensed under the MIT license:
# http://opensource.org/licenses/MIT
# Copyright (c) 2016, fitnr <fitnr@fakeisthenewreal>
"""
The `Julian day <https://en.wikipedia.org/wiki/Julian_day>`__
is a continuous count of days since the beginning of the Julian era on January 1, 4713 BC.
"""
from datetime import datetime, timezone

from . import gregorian, julian


def to_datetime(jdc):
    '''Return a datetime for the input floating point julian day count'''
    year, month, day = gregorian.from_jd(jdc)

    # in jdc: 0.0 = noon, 0.5 = midnight
    # the 0.5 changes it to 0.0 = midnight, 0.5 = noon
    frac = (jdc + 0.5) % 1

    hours = int(24 * frac)

    mfrac = frac * 24 - hours
    mins = int(60 * round(mfrac, 6))

    sfrac = mfrac * 60 - mins
    secs = int(60 * round(sfrac, 6))

    msfrac = sfrac * 60 - secs

    # down to ms, which are 1/1000 of a second
    ms = int(1000 * round(msfrac, 6))

    return datetime(
        year,
        month,
        day,
        int(hours),
        int(mins),
        int(secs),
        int(ms),
        tzinfo=timezone.utc
    )


def from_datetime(dt):
    '''Convert from ``datetime`` to julian day count.'''
    # take account of offset (if there isn't one, act like it's utc)
    try:
        dt = dt + dt.utcoffset()
    except TypeError:
        # Assuming UTC
        pass

    jdc = gregorian.to_jd(dt.year, dt.month, dt.day)

    hfrac = dt.hour / 24.0
    mfrac = round(dt.minute / (24.0 * 60), 5)
    sfrac = round(dt.second / (24.0 * 60 * 60), 5)
    msfrac = dt.microsecond / (24.0 * 60 * 60 * 1000)

    return jdc + hfrac + mfrac + sfrac + msfrac


def to_gregorian(jdc):
    '''Convert from julian day count to Gregorian date.'''
    return gregorian.from_jd(jdc)


def from_gregorian(year, month, day):
    '''Convert from Gregorian ``year``, ``month`` and ``day`` to julian day count.'''
    return gregorian.to_jd(year, month, day)


def to_julian(jdc):
    '''Convert from julian day count to Julian date.'''
    return julian.from_jd(jdc)


def from_julian(year, month, day):
    '''Convert from Julian ``year``, ``month`` and ``day`` to julian day count.'''
    return julian.to_jd(year, month, day)
