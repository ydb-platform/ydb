# -*- coding: utf-8 -*-
# This file is part of convertdate.
# http://github.com/fitnr/convertdate
# Licensed under the MIT license:
# http://opensource.org/licenses/MIT
# Copyright (c) 2016, 2020, 2021, 2022 fitnr <fitnr@fakeisthenewreal>
"""
The Convertdate library contains methods and functions for converting dates between
different calendar systems.

It was originally developed as as `Python Date Util <(http://sourceforge.net/projects/pythondateutil/>`__
by Phil Schwartz. It has been significantly updated and expanded.

Most of the original code is ported from
`Fourmilab's calendar converter <http://www.fourmilab.ch/documents/calendar/>`__,
which was developed by John Walker.

The algorithms are believed to be derived from: Meeus, Jean. `Astronomical Algorithms`,
Richmond: Willmann-Bell, 1991 (ISBN 0-943396-35-2)
"""
from . import (
    armenian,
    bahai,
    coptic,
    daycount,
    dublin,
    french_republican,
    gregorian,
    hebrew,
    holidays,
    indian_civil,
    islamic,
    iso,
    julian,
    julianday,
    mayan,
    ordinal,
    persian,
    positivist,
    utils,
)

__version__ = '2.4.1'

__all__ = [
    'armenian',
    'bahai',
    'coptic',
    'daycount',
    'dublin',
    'french_republican',
    'gregorian',
    'hebrew',
    'holidays',
    'indian_civil',
    'islamic',
    'iso',
    'julian',
    'julianday',
    'mayan',
    'ordinal',
    'persian',
    'positivist',
    'utils',
]
