#!/usr/bin/env python

import unittest
from ephem import city

class CityTests(unittest.TestCase):
    def test_boston(self):
        b = city('Boston')
        self.assertEqual(b.name, 'Boston')

    def test_unknown_city(self):
        self.assertRaises(KeyError, city, 'Marietta')
