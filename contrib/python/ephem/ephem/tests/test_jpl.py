#!/usr/bin/env python
# -*- coding: utf-8 -*-

import glob, os.path, re, traceback, unittest
from datetime import datetime
from time import strptime
import ephem

# Since users might be in another locale, we have to translate the
# month into an integer on our own.
month_ints = {
    'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
    'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12,
    }

# Read an ephemeris from the JPL, and confirm that PyEphem returns the
# same measurements to within one arcsecond of accuracy.

angle_error = ephem.degrees('0:00:10.8') # Hyperion is seriously misbehaving
size_error = 0.1

def cleanup(s):
    return s.strip().replace(' ', ':')

class JPLDatum(object):
    pass

class JPLTest(object):

    def test_jpl(self):
        in_data = False

        f = open(self.path)
        try:
            lines = f.readlines()
        finally:
            f.close()

        for line in lines:

            if line.startswith('Target body name:'):
                name = line.split()[3]
                if not hasattr(ephem, name):
                    raise ValueError('ephem lacks a body named %r' % name)
                body_class = getattr(ephem, name)
                body = body_class()

            elif line.startswith('$$SOE'):
                in_data = True

            elif line.startswith('$$EOE'):
                in_data = False

            elif in_data:
                datestr = line[1:6] + str(month_ints[line[6:9]]) + line[9:18]
                date = datetime.strptime(datestr, '%Y-%m-%d %H:%M')
                body.compute(date)

                jpl = JPLDatum()
                jpl.a_ra = ephem.hours(cleanup(line[23:34]))
                jpl.a_dec = ephem.degrees(cleanup(line[35:46]))
                jpl.size = float(line[71:])

                for attr, error in (('a_ra', angle_error),
                                    ('a_dec', angle_error),
                                    ('size', size_error)):
                    try:
                        body_value = getattr(body, attr)
                    except AttributeError: # moons lack "size"
                        continue
                    jpl_value = getattr(jpl, attr)
                    difference = abs(body_value - jpl_value)
                    if difference > error:
                        raise ValueError('at %s, %s returns %s=%s'
                                         ' but JPL insists that %s=%s'
                                         ' which is %s degrees away' %
                                         (date, body.name, attr, body_value,
                                          attr, jpl_value,
                                          ephem.degrees(difference)))

# re, traceback, datetime, strptime, ephem
#
# def load_tests(loader, tests, pattern):
#     suite = unittest.TestSuite()
#     for path in glob.glob(os.path.dirname(__file__) + '/jpl/*.txt'):
#         case = JPLTest()
#         case.path = path
#         suite.addTest(case)
#     return suite

from yatest.common import source_path
txts = source_path('contrib/python/ephem/ephem/tests')
class T1(unittest.TestCase, JPLTest): path = os.path.join(txts, 'jpl', 'europa.txt')
class T2(unittest.TestCase, JPLTest): path = os.path.join(txts, 'jpl', 'hyperion.txt')
class T3(unittest.TestCase, JPLTest): path = os.path.join(txts, 'jpl', 'jupiter.txt')
class T4(unittest.TestCase, JPLTest): path = os.path.join(txts, 'jpl', 'mars.txt')
class T5(unittest.TestCase, JPLTest): path = os.path.join(txts, 'jpl', 'neptune.txt')
class T6(unittest.TestCase, JPLTest): path = os.path.join(txts, 'jpl', 'oberon.txt')
class T7(unittest.TestCase, JPLTest): path = os.path.join(txts, 'jpl', 'phobos.txt')
class T8(unittest.TestCase, JPLTest): path = os.path.join(txts, 'jpl', 'saturn.txt')
class T9(unittest.TestCase, JPLTest): path = os.path.join(txts, 'jpl', 'uranus.txt')
