#!/usr/bin/env python

import unittest
import ephem

# Determine whether angles work reasonably.

class ConstantTests(unittest.TestCase):
    def test_constants(self):
        self.assertEqual(ephem.c, 299792458)
        self.assertEqual(ephem.meters_per_au, 1.4959787e11)
        self.assertEqual(ephem.earth_radius, 6378160)
        self.assertEqual(ephem.moon_radius, 1740000)
        self.assertEqual(ephem.sun_radius, 695000000)
