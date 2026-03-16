# -*- coding: utf-8 -*-

# This file is part of convertdate.
# http://github.com/fitnr/convertdate

# Licensed under the MIT license:
# http://opensource.org/licenses/MIT
# Copyright (c) 2016, fitnr <fitnr@fakeisthenewreal>
from datetime import datetime
from . import gregorian
from . import julian
from pytz import utc


def to_datetime(jdc):
    '''Return a datetime for the input floating point Julian Day Count'''
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

    return datetime(year, month, day, int(hours), int(mins), int(secs), int(ms), tzinfo=utc)


def from_datetime(dt):
    # take account of offset (if there isn't one, act like it's utc)
    try:
        dt = dt + dt.utcoffset()
    except TypeError:
        # Assuming UTC
        pass

    jdc = gregorian.to_jd(dt.year, dt.month, dt.day)

    hfrac = dt.hour / 24.
    mfrac = round(dt.minute / (24. * 60), 5)
    sfrac = round(dt.second / (24. * 60 * 60), 5)
    msfrac = dt.microsecond / (24. * 60 * 60 * 1000)

    return jdc + hfrac + mfrac + sfrac + msfrac


def to_gregorian(jdc):
    return gregorian.from_jd(jdc)


def from_gregorian(year, month, day):
    return gregorian.to_jd(year, month, day)


def to_julian(jdc):
    return julian.from_jd(jdc)


def from_julian(year, month, day):
    return julian.to_jd(year, month, day)
