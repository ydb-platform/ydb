#!/usr/bin/env python

import unittest

# Determine whether the equinoxes and solstices which PyEphem predicts
# fall within one minute (by the clock) of those predicted by the
# United States Naval Observatory.

# Data is from http://aa.usno.navy.mil/data/docs/EarthSeasons.php

usno_data = """
                  d  h                      d  h  m           d  h  m
2000                        2000
Perihelion  Jan   3 05    Equinoxes  Mar   20 07 35    Sept  22 17 27
Aphelion    July  4 00    Solstices  June  21 01 48    Dec   21 13 37

2001                        2001
Perihelion  Jan   4 09    Equinoxes  Mar   20 13 31    Sept  22 23 04
Aphelion    July  4 14    Solstices  June  21 07 38    Dec   21 19 21

2002                        2002
Perihelion  Jan   2 14    Equinoxes  Mar   20 19 16    Sept  23 04 55
Aphelion    July  6 04    Solstices  June  21 13 24    Dec   22 01 14

2003                        2003
Perihelion  Jan   4 05    Equinoxes  Mar   21 01 00    Sept  23 10 47
Aphelion    July  4 06    Solstices  June  21 19 10    Dec   22 07 04

2004                        2004
Perihelion  Jan   4 18    Equinoxes  Mar   20 06 49    Sept  22 16 30
Aphelion    July  5 11    Solstices  June  21 00 57    Dec   21 12 42

2005                        2005
Perihelion  Jan   2 01    Equinoxes  Mar   20 12 33    Sept  22 22 23
Aphelion    July  5 05    Solstices  June  21 06 46    Dec   21 18 35

2006                        2006
Perihelion  Jan   4 15    Equinoxes  Mar   20 18 26    Sept  23 04 03
Aphelion    July  3 23    Solstices  June  21 12 26    Dec   22 00 22

2007                        2007
Perihelion  Jan   3 20    Equinoxes  Mar   21 00 07    Sept  23 09 51
Aphelion    July  7 00    Solstices  June  21 18 06    Dec   22 06 08

2008                        2008
Perihelion  Jan   3 00    Equinoxes  Mar   20 05 48    Sept  22 15 44
Aphelion    July  4 08    Solstices  June  20 23 59    Dec   21 12 04

2009                        2009
Perihelion  Jan   4 15    Equinoxes  Mar   20 11 44    Sept  22 21 18
Aphelion    July  4 02    Solstices  June  21 05 45    Dec   21 17 47

2010                        2010
Perihelion  Jan   3 00    Equinoxes  Mar   20 17 32    Sept  23 03 09
Aphelion    July  6 11    Solstices  June  21 11 28    Dec   21 23 38

2011                        2011
Perihelion  Jan   3 19    Equinoxes  Mar   20 23 21    Sept  23 09 04
Aphelion    July  4 15    Solstices  June  21 17 16    Dec   22 05 30

2012                        2012
Perihelion  Jan   5 00    Equinoxes  Mar   20 05 14    Sept  22 14 49
Aphelion    July  5 03    Solstices  June  20 23 09    Dec   21 11 11

2013                        2013
Perihelion  Jan   2 05    Equinoxes  Mar   20 11 02    Sept  22 20 44
Aphelion    July  5 15    Solstices  June  21 05 04    Dec   21 17 11

2014                        2014
Perihelion  Jan   4 12    Equinoxes  Mar   20 16 57    Sept  23 02 29
Aphelion    July  4 00    Solstices  June  21 10 51    Dec   21 23 03

2015                        2015
Perihelion  Jan   4 07    Equinoxes  Mar   20 22 45    Sept  23 08 20
Aphelion    July  6 19    Solstices  June  21 16 38    Dec   22 04 48

2016                        2016
Perihelion  Jan   2 23    Equinoxes  Mar   20 04 30    Sept  22 14 21
Aphelion    July  4 16    Solstices  June  20 22 34    Dec   21 10 44

2017                        2017
Perihelion  Jan   4 14    Equinoxes  Mar   20 10 28    Sept  22 20 02
Aphelion    July  3 20    Solstices  June  21 04 24    Dec   21 16 28

2018                        2018
Perihelion  Jan   3 06    Equinoxes  Mar   20 16 15    Sept  23 01 54
Aphelion    July  6 17    Solstices  June  21 10 07    Dec   21 22 22

2019                        2019
Perihelion  Jan   3 05    Equinoxes  Mar   20 21 58    Sept  23 07 50
Aphelion    July  4 22    Solstices  June  21 15 54    Dec   22 04 19

2020                        2020
Perihelion  Jan   5 08    Equinoxes  Mar   20 03 49    Sept  22 13 30
Aphelion    July  4 12    Solstices  June  20 21 43    Dec   21 10 02
"""

#

import datetime, time
import ephem

# Since users might be in another locale, we have to translate the
# month into an integer on our own.
month_ints = {
    'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
    'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12,
    }

sun = ephem.Sun()

def to_date(year, monthname, day, hour, minute):
    mi = month_ints[monthname[:3]]
    s = '%s/%s/%s %s:%s' % (year, mi, day, hour, minute)
    dt = datetime.datetime(*time.strptime(s, '%Y/%m/%d %H:%M')[0:5])
    return ephem.date(dt)

def sequence_of_events(data):
    seq = []
    for line in data.split('\n'):
        fields = line.split()
        if line.startswith('2'):
            fields = line.split()
            year = int(fields[0]) # '2001' or whatever
        elif line.startswith('Perihelion'):
            seq.append((to_date(year, *fields[5:9]), 'vernal equinox'))
            seq.append((to_date(year, *fields[9:13]), 'autumnal equinox'))
        elif line.startswith('Aphelion'):
            seq.append((to_date(year, *fields[5:9]), 'summer solstice'))
            seq.append((to_date(year, *fields[9:13]), 'winter solstice'))
    seq.sort()
    return seq

def run_test(seq, step, functions):
    if step > 0:
        date = seq[0][0] - 1  # one day earlier than first event
        r = range(0, len(seq))
    else:
        date = seq[-1][0] + 1 # one day after last event
        r = range(len(seq) - 1, -1, -1)
    for i in r:
        usno_date = seq[i][0]
        function = functions[i % len(functions)]
        date = function(date)
        if abs(usno_date - date) > ephem.minute:
            raise AssertionError(
                'The USNO predicts a %s at %s but PyEphem at %s'
                % (seq[i][1], usno_date, date))

def process_usno(data):
    seq = sequence_of_events(data)

    run_test(seq, +1, (ephem.next_equinox,
                       ephem.next_solstice,
                       ephem.next_equinox,
                       ephem.next_solstice,
                       ))

    run_test(seq, +1, (ephem.next_vernal_equinox,
                       ephem.next_summer_solstice,
                       ephem.next_autumnal_equinox,
                       ephem.next_winter_solstice,
                       ))

    run_test(seq, -1, (ephem.previous_equinox,
                       ephem.previous_solstice,
                       ephem.previous_equinox,
                       ephem.previous_solstice,
                       ))

    run_test(seq, -1, (ephem.previous_vernal_equinox,
                       ephem.previous_summer_solstice,
                       ephem.previous_autumnal_equinox,
                       ephem.previous_winter_solstice,
                       ))

class UsnoEquinoxesTests(unittest.TestCase):
    def test_equinoxes(self):
        process_usno(usno_data)
