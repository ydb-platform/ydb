#!/usr/bin/env python

import unittest
from ephem import Angle, degrees, hours

# Determine whether angles work reasonably.

arcsecond_places = 5

class AngleTests(unittest.TestCase):
    def setUp(self):
        self.d = degrees(1.5)
        self.h = hours(1.6)

    def test_Angle_constructor(self):
        self.assertRaises(TypeError, Angle, 1.1)

    def test_degrees_constructor(self):
        self.assertAlmostEqual(self.d, degrees('85:56:37'),
                               places=arcsecond_places)
        self.assertAlmostEqual(self.d, degrees('85::3397'),
                               places=arcsecond_places)
        self.assertAlmostEqual(self.d, degrees('::309397'),
                               places=arcsecond_places)

        self.assertAlmostEqual(self.d, degrees('85 56 37'),
                               places=arcsecond_places)
        self.assertAlmostEqual(self.d, degrees('85 56:37'),
                               places=arcsecond_places)
        self.assertAlmostEqual(self.d, degrees('85:56 37'),
                               places=arcsecond_places)
        self.assertAlmostEqual(self.d, degrees('85 : 56 : 37'),
                               places=arcsecond_places)
        self.assertAlmostEqual(self.d, degrees(' 85 : 56 : 37 '),
                               places=arcsecond_places)

        self.assertAlmostEqual(self.d, degrees(' :  :   309397    '),
                               places=arcsecond_places)

    def test_degrees_constructor_refuses_alphabetics(self):
        self.assertRaises(ValueError, degrees, 'foo:bar')
        self.assertRaises(ValueError, degrees, '1:bar')
        self.assertRaises(ValueError, degrees, '1:2:bar')
        self.assertRaises(ValueError, degrees, '1:2:3bar')

    def test_degrees_float_value(self):
        self.assertAlmostEqual(self.d, 1.5)
    def test_degrees_string_value(self):
        self.assertEqual(str(self.d), '85:56:37.2')

    def test_hours_constructor(self):
        self.assertAlmostEqual(self.h, hours('6:06:41.6'),
                               places=arcsecond_places)
    def test_hours_float_value(self):
        self.assertAlmostEqual(self.h, 1.6)
    def test_hours_string_value(self):
        self.assertEqual(str(self.h), '6:06:41.58')

    def test_angle_addition(self):
        self.assertAlmostEqual(degrees('30') + degrees('90'), degrees('120'))
    def test_angle_subtraction(self):
        self.assertAlmostEqual(degrees('180') - hours('9'), degrees('45'))
